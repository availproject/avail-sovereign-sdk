use sov_blob_storage::SequenceNumber;
use sov_modules_api::{Runtime, Spec, StateCheckpoint};
use sov_rollup_interface::node::da::DaService;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{error, trace, warn};

use super::executor_events::ExecutorEvent;
use crate::preferred::{
    exit_rollup, track_in_progress_batch_size, AcceptedTx, PreferredBatchToReplay,
    PreferredBlobSender, PreferredSequencerDb, PreferredSequencerReadBatch,
    PreferredSequencerReadBlob, RecoveryStrategy, RECOVERY_ERROR_MESSAGE_ON_NONE_STRATEGY,
};

/// A task that runs in the background and handles side effects of accepted transactions.
pub(super) struct SideEffectsTask<S, Rt, Da>
where
    S: Spec,
    Rt: Runtime<S>,
    Da: DaService<Spec = S::Da>,
{
    pub checkpoint_sender: watch::Sender<StateCheckpoint<S>>,
    pub blob_sender: PreferredBlobSender<Da>,
    pub db: PreferredSequencerDb<S, Rt>,
    pub executor_events_receiver: mpsc::Receiver<ExecutorEvent<S, Rt>>,
    pub shutdown_sender: watch::Sender<()>,
}

impl<S, Rt, Da> SideEffectsTask<S, Rt, Da>
where
    S: Spec,
    Rt: Runtime<S>,
    Da: DaService<Spec = S::Da>,
{
    /// Syncs [`ApiState`]s with the latest [`StateCheckpoint`].
    #[tracing::instrument(skip_all, level = "trace")]
    fn update_api_state(&self, checkpoint: StateCheckpoint<S>) {
        if self.checkpoint_sender.send(checkpoint).is_err() {
            tracing::debug!("Could not send checkpoint because the receiver has been dropped; this probably means the rollup is shutting down");
        }
    }

    #[tracing::instrument(skip_all, level = "trace")]
    async fn close_and_publish_current_batch(
        &mut self,
        checkpoint: StateCheckpoint<S>,
    ) -> anyhow::Result<()> {
        let batch = self.db.terminate_batch().await?;
        self.update_api_state(checkpoint);

        // Publish the batch.
        self.blob_sender
            .hooks()
            .add_txs(batch.blob_id, batch.tx_hashes.clone())
            .await;
        self.blob_sender.publish_batch(batch).await?;

        // Update the metrics.
        track_in_progress_batch_size(
            self.db
                .in_progress_batch_opt()
                .map(|b| b.txs.len() as u64)
                .unwrap_or(0),
        );
        Ok(())
    }

    async fn trigger_recovery(
        &mut self,
        next_sequence_number_according_to_node: SequenceNumber,
        recovery_strategy: RecoveryStrategy,
    ) -> anyhow::Result<()> {
        let batches_to_replay =
            self.fetch_completed_blobs_by_sequence(next_sequence_number_according_to_node, true);
        if !batches_to_replay.is_empty() {
            match recovery_strategy {
                RecoveryStrategy::TryToSave => {
                    // Flush our batches to try to save them if we can
                    warn!(num_batches_to_replay = batches_to_replay.len(), "TryToSave recovery strategy has been configured. The currently pending soft confirmations will be flushed to the node. This may save some of the transactions, but if any are no longer valid, the sequencer will be penalised.");
                    self.flush_pending_batches_for_recovery(next_sequence_number_according_to_node)
                        .await?;
                }
                RecoveryStrategy::None => {
                    // Shut down
                    error!(RECOVERY_ERROR_MESSAGE_ON_NONE_STRATEGY);
                    exit_rollup(&self.shutdown_sender).await;
                }
            }
        } else {
            warn!("Recovery: sequencer will now fast-forward the visible slot number, and resume normal operations when ready. There were no pending soft confirmations, so users will not be affected except for the downtime.");
        }
        Ok(())
    }

    async fn flush_pending_batches_for_recovery(
        &mut self,
        next_sequence_number_according_to_node: SequenceNumber,
    ) -> anyhow::Result<()> {
        tracing::trace!("Recovery: flushing all preferred sequencer batches");

        // 1. close the in-progress batch, if any
        if self.db.in_progress_batch_opt().is_some() {
            tracing::debug!("Recovery: In-progress batch found, terminating it.");
            self.db.terminate_batch().await?;
            // No need to update API state, we're going to overwrite it with the node's state soon
        } else {
            tracing::debug!("Recovery: No in-progress batch to terminate.");
        }

        // 2. Flush all batches to the BlobSender
        let blobs_to_flush = self
            .db
            .all_completed_blobs_greater_than_or_equal_to(next_sequence_number_according_to_node);
        self.blob_sender.publish_blobs(blobs_to_flush).await?;

        Ok(())
    }

    fn fetch_completed_blobs_by_sequence(
        &mut self,
        after_and_including: SequenceNumber,
        include_in_progress_batch: bool,
    ) -> Vec<PreferredBatchToReplay> {
        let blobs_to_apply = self
            .db
            .all_completed_blobs_greater_than_or_equal_to(after_and_including);
        let first_sequence_number = blobs_to_apply.first().map(|b| b.sequence_number());

        trace!(
            blobs_count = blobs_to_apply.len(),
            first_sequence_number,
            last_sequence_number = blobs_to_apply.last().map(|b| b.sequence_number()),
            "Extracted blobs to apply from database"
        );

        let maybe_in_progress_batch = if include_in_progress_batch {
            self.db.in_progress_batch_opt().cloned().map(|batch| {
                let batch: PreferredSequencerReadBatch = batch.into();
                PreferredBatchToReplay {
                    is_in_progress: true,
                    visible_slot_number_after_increase: batch.visible_slot_number_after_increase,
                    batch: batch.into_with_cached_tx_hashes(),
                }
            })
        } else {
            None
        };

        blobs_to_apply
            .into_iter()
            .filter_map(|blob| match blob {
                PreferredSequencerReadBlob::Batch(batch) => Some(PreferredBatchToReplay {
                    is_in_progress: false,
                    visible_slot_number_after_increase: batch.visible_slot_number_after_increase,
                    batch: batch.into_with_cached_tx_hashes(),
                }),
                // TODO(https://github.com/Sovereign-Labs/sovereign-sdk-wip/issues/2063): Process proofs.
                // Note: once we start processing proofs in addition to batches,
                // we gotta make sure to order everything by sequence number as
                // proofs can have a sequence number that's greater than the
                // in-progress batch.
                _ => {
                    trace!(
                        sequence_number = %blob.sequence_number(),
                        "Ignoring proof blob"
                    );
                    None
                }
            })
            .chain(maybe_in_progress_batch)
            .collect::<Vec<_>>()
    }

    async fn handle_executor_event(
        &mut self,
        event: ExecutorEvent<S, Rt>,
    ) -> Result<(), anyhow::Error> {
        match event {
            ExecutorEvent::AcceptedTx(tx_hash, tx, confirmation, checkpoint, oneshot_sender) => {
                self.db.insert_tx(tx.clone(), tx_hash).await?;
                tracing::debug!(%tx_hash, "Transaction was accepted by the sequencer");
                track_in_progress_batch_size(
                    self.db
                        .in_progress_batch_opt()
                        .map(|b| b.txs.len() as u64)
                        .unwrap_or(0),
                );
                // If the receiver is no longer listening, just don't send the confirmation.
                let _ = oneshot_sender.send(AcceptedTx {
                    tx: tx.clone(),
                    tx_hash,
                    confirmation: confirmation.clone(),
                });
                self.update_api_state(checkpoint);
            }
            ExecutorEvent::CloseBatch(checkpoint) => {
                self.close_and_publish_current_batch(checkpoint).await?;
            }
            ExecutorEvent::Flush(id) => {
                self.db.flush(id).await;
            }
            ExecutorEvent::StartBatch {
                visible_slot_number_after_increase,
                visible_slots_to_advance,
                sequence_number,
            } => {
                self.db
                    .start_batch(
                        visible_slot_number_after_increase,
                        visible_slots_to_advance,
                        sequence_number,
                    )
                    .await?;
            }
            ExecutorEvent::EnterRecoveryMode {
                recovery_strategy,
                next_sequence_number_according_to_node,
            } => {
                self.trigger_recovery(next_sequence_number_according_to_node, recovery_strategy)
                    .await?;
            }
            ExecutorEvent::PublishProofBlob(blob_id, data, sequence_number) => {
                self.db
                    .insert_proof_blob(blob_id, data.clone(), sequence_number)
                    .await?;
                self.blob_sender
                    .publish_proof(data, sequence_number, blob_id)
                    .await?;
            }
            ExecutorEvent::ForceUpdateApiState(new_checkpoint) => {
                self.update_api_state(new_checkpoint);
            }
            ExecutorEvent::PruneDb(sequence_number) => {
                self.db.prune(sequence_number).await?;
            }
            ExecutorEvent::InsertTxWithoutConfirmation(tx, tx_hash) => {
                self.db.insert_tx(tx, tx_hash).await?;
            }

            ExecutorEvent::FetchCompletedBlobs {
                after_and_including,
                oneshot_sender,
                include_in_progress_batch,
            } => {
                let blobs = self.fetch_completed_blobs_by_sequence(
                    after_and_including,
                    include_in_progress_batch,
                );
                let _ = oneshot_sender.send(blobs);
            }
            ExecutorEvent::FetchInProgressBatch(oneshot_sender) => {
                let in_progress_batch = self.db.in_progress_batch_opt().cloned().map(|b| b.into());
                let _ = oneshot_sender.send(in_progress_batch);
            }
            ExecutorEvent::UpdateStateForRecovery(checkpoint) => {
                self.update_api_state(checkpoint);
            }
            ExecutorEvent::SubscribeToEvents(sender) => {
                self.db.subscribe_to_events(sender);
            }
        }
        Ok(())
    }

    pub(crate) fn spawn(mut self) -> JoinHandle<()> {
        tokio::spawn(async move {
            while let Some(event) = self.executor_events_receiver.recv().await {
                if let Err(e) = self.handle_executor_event(event).await {
                    tracing::error!("Error handling executor event: {:?}", e);
                    // If we've arleady started shutting down, this might fail - but then we're happy.
                    let _ = self.shutdown_sender.send(());
                    break;
                }
            }
        })
    }
}
