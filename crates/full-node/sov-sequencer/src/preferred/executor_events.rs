use std::num::NonZero;
use std::sync::Arc;

use sov_blob_sender::BlobInternalId;
use sov_blob_storage::SequenceNumber;
use sov_modules_api::{FullyBakedTx, Runtime, Spec, StateCheckpoint, TxHash, VisibleSlotNumber};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot, watch};
use uuid::Uuid;

use crate::common::AcceptedTx;
use crate::metrics::PreferredSequencerExecutorEventSendingMetrics;
use crate::preferred::db::PreferredSequencerReadBatch;
use crate::preferred::{
    exit_rollup, Confirmation, DbEvent, PreferredBatchToReplay, RecoveryStrategy,
};

const MAX_EXECUTOR_EVENT_QUEUE_DEPTH: usize = 20;

pub(super) struct ExecutorEventsSender<S: Spec, Rt: Runtime<S>> {
    events_sender: mpsc::Sender<ExecutorEvent<S, Rt>>,
    shutdown_sender: watch::Sender<()>,
}

impl<S: Spec, Rt: Runtime<S>> ExecutorEventsSender<S, Rt> {
    pub fn new(shutdown_sender: watch::Sender<()>) -> (Self, mpsc::Receiver<ExecutorEvent<S, Rt>>) {
        let (sender, receiver) = mpsc::channel(MAX_EXECUTOR_EVENT_QUEUE_DEPTH);
        (
            Self {
                events_sender: sender,
                shutdown_sender,
            },
            receiver,
        )
    }

    async fn shutdown_on_error(&self) {
        tracing::error!("Failed to send executor event because the receiver was dropped. This indicates that the database is no longer available. Shutting down.");
        exit_rollup(&self.shutdown_sender).await;
    }

    /// Send an event tracking metrics on the queue depth and blocking time and shutting down on error.
    pub async fn send(&self, event: ExecutorEvent<S, Rt>) {
        let mut metrics = PreferredSequencerExecutorEventSendingMetrics::default();
        match self.events_sender.try_send(event) {
            Ok(()) => (),
            Err(TrySendError::Full(event)) => {
                tracing::trace!(
                    "Executor event channel is full. Blocking until it becomes available again."
                );
                let started_blocking = std::time::Instant::now();
                if self.events_sender.send(event).await.is_err() {
                    self.shutdown_on_error().await;
                };
                metrics.blocked_for_us = started_blocking.elapsed().as_micros() as u64;
            }
            Err(TrySendError::Closed(_)) => self.shutdown_on_error().await,
        }

        let queue_depth = self.events_sender.max_capacity() - self.events_sender.capacity();
        metrics.queue_depth = queue_depth;
        sov_metrics::track_metrics(|t| {
            t.submit(metrics);
        });
    }

    /// Send a notification of an accepted tx. Return a receiver that will receive the confirmation.
    pub(crate) async fn send_accept_tx(
        &self,
        tx: FullyBakedTx,
        hash: TxHash,
        confirmation: Confirmation<S, Rt>,
        checkpoint: StateCheckpoint<S>,
    ) -> oneshot::Receiver<AcceptedTx<Confirmation<S, Rt>>> {
        let (sender, receiver) = oneshot::channel();
        self.send(ExecutorEvent::AcceptedTx(
            hash,
            tx,
            confirmation,
            checkpoint,
            sender,
        ))
        .await;
        receiver
    }

    /// Fetch the in-progress batch from the database.
    ///
    /// # Danger
    /// The result may be outdated by the time you receive it
    /// if you have not otherwise locked that database
    pub(crate) async fn fetch_in_progress_batch(
        &self,
    ) -> anyhow::Result<Option<PreferredSequencerReadBatch>> {
        let (sender, receiver) = oneshot::channel();
        self.send(ExecutorEvent::FetchInProgressBatch(sender)).await;
        receiver.await.map_err(|_| {
            anyhow::anyhow!(
                "Failed to fetch in-progress batch because the database is no longer available."
            )
        })
    }
}

pub(super) enum ExecutorEvent<S, Rt>
where
    S: Spec,
    Rt: Runtime<S>,
{
    /// Start a new batch.
    StartBatch {
        #[allow(missing_docs)]
        visible_slot_number_after_increase: VisibleSlotNumber,
        #[allow(missing_docs)]
        visible_slots_to_advance: NonZero<u8>,
        #[allow(missing_docs)]
        sequence_number: SequenceNumber,
    },
    /// Close the current batch.
    CloseBatch(StateCheckpoint<S>),
    /// Flush the current batch, sending an event from the db_events channel when finished
    Flush(Uuid),
    /// Publish a proof blob.
    PublishProofBlob(BlobInternalId, Arc<[u8]>, SequenceNumber),
    /// Insert an accepted transaction into the database and send out the confirmation
    AcceptedTx(
        TxHash,
        FullyBakedTx,
        Confirmation<S, Rt>,
        StateCheckpoint<S>,
        oneshot::Sender<AcceptedTx<Confirmation<S, Rt>>>,
    ),
    /// Update the API state to the given checkpoint without closing the current batch etc. Used during recovery
    ForceUpdateApiState(StateCheckpoint<S>),
    /// Prune the database up to the given sequence number.
    PruneDb(SequenceNumber),
    /// Enter recovery mode.
    EnterRecoveryMode {
        #[allow(missing_docs)]
        recovery_strategy: RecoveryStrategy,
        #[allow(missing_docs)]
        next_sequence_number_according_to_node: SequenceNumber,
    },
    /// During recovery mode, we periodically update the state to the node's state.
    UpdateStateForRecovery(StateCheckpoint<S>),
    /// Fetch completed blobs from the database.
    FetchCompletedBlobs {
        #[allow(missing_docs)]
        after_and_including: SequenceNumber,
        #[allow(missing_docs)]
        oneshot_sender: oneshot::Sender<Vec<PreferredBatchToReplay>>,
        #[allow(missing_docs)]
        include_in_progress_batch: bool,
    },
    /// Fetch completed blobs from the database.
    FetchInProgressBatch(oneshot::Sender<Option<PreferredSequencerReadBatch>>),
    /// Subscribe to events from the database.
    SubscribeToEvents(mpsc::Sender<DbEvent>),
    /// Insert a transaction into the database.
    InsertTxWithoutConfirmation(FullyBakedTx, TxHash),
}
