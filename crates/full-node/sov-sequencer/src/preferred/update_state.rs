use std::time::Instant;

use sov_modules_api::{Runtime, Spec};
use sov_rollup_interface::node::da::DaService;
use sov_state::{NativeStorage, Storage};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::metrics::{PreferredSequencerPruneMetrics, PreferredSequencerUpdateStateMetrics};
use crate::preferred::{
    get_next_sequence_number_according_to_node, DbEvent, ExecutorEvent, PreferredBatchToReplay,
    PreferredSequencer, RollupBlockExecutor, StateUpdateInfo,
};

impl<S, Rt, Da> PreferredSequencer<S, Rt, Da>
where
    S: Spec,
    Rt: Runtime<S>,
    Da: DaService<Spec = S::Da>,
{
    /// Replays all outstanding batches on top of the given executor,
    /// starting from the last one processed by that executor.
    ///
    /// This function works by...
    /// - Getting the list of all *completed* batches from the database that haven't yet been played on this sequencer
    /// - Replaying each completed batch
    /// - Repeating until we've reached the in-progress batch. Then...
    ///
    /// Subscribe to DB events:
    /// - for each event, play it on the executor;
    /// - if we fell more than `config.sequencer_kind_config.db_event_channel_size` events behind, we block the sequencer. This should never happen with proper configuration, but we log a warning in case it does.
    #[tracing::instrument(skip_all, level = "debug", name = "update_state")]
    pub(super) async fn replay_soft_confirmations_on_top_of_node_state(
        &self,
        info: StateUpdateInfo<S::Storage>,
        timer_start: Instant,
    ) -> anyhow::Result<()> {
        // On shutdown exit early. This prevents duplicate subscriptions to the DB events channel, which would cause spurious warnings.
        // Note that we only need to detect whether a previous `replay_soft_confirmations_on_top_of_node_state` was aborted due to shutdown
        // *while its subscription was active*, so a single check at the start is sufficient.
        if self.shutdown_receiver.has_changed().unwrap_or(true) {
            tracing::info!("The sequencer is shutting down. Exiting replay_soft_confirmations_on_top_of_node_state without completing replay.");
            return Ok(());
        }

        let mut batches_count = 0;
        let mut transactions_count = 0;
        let mut next_sequence_number =
            get_next_sequence_number_according_to_node(&info, &mut Rt::default());
        let mut total_lock_duration = std::time::Duration::ZERO;

        // Now that we're not locking on the sequencer state anymore, we can replay all the batches.
        let mut executor = RollupBlockExecutor::<_, Rt>::new(
            &info,
            None, // We don't re-send events when replaying batches in the background.
            None, // We don't re-send transactions when replaying batches in the background.
            self.config.clone(),
            self.block_executors_shutdown_notifier.clone(),
            self.state_root_compute_task.request_sender.clone(),
            self.shutdown_receiver.clone(),
            self.shutdown_sender.clone(),
            self.cached_events.clone(),
        );

        let node_state_root = tracing::trace_span!("root_hash")
            .in_scope(|| info.storage.get_root_hash(info.slot_number))?;

        // Repeatedly fetch all completed batches from the database that haven't yet been played on this sequencer and replay them
        let (in_progress_batch, mut db_event_subscription) = loop {
            let inner = self.lock_inner().await;
            let lock_start = std::time::Instant::now();
            // Because we just sent our own message while holding the lock, we know that it will be the last message in the db channel.
            // So, the response we receive is a completely up-to-date picture of the DB.
            let completed_batches = self
                .completed_batches_to_replay(&inner, next_sequence_number, false)
                .await?;

            // Once we've caught up to the in-progress batch, we're done.
            let (db_events_sender, subscription) =
                mpsc::channel(self.config.sequencer_kind_config.db_event_channel_size);
            if completed_batches.is_empty() {
                inner
                    .executor_events_sender
                    .send(ExecutorEvent::SubscribeToEvents(db_events_sender))
                    .await;

                let in_progress_batch = inner
                    .executor_events_sender
                    .fetch_in_progress_batch()
                    .await?;
                total_lock_duration += lock_start.elapsed();
                break (in_progress_batch, subscription);
            }

            drop(inner); // Drop quickly so we don't block the sequencer
            total_lock_duration += lock_start.elapsed();

            for batch in completed_batches {
                batches_count += 1;
                transactions_count += batch.batch.inner.data.len();
                next_sequence_number = batch.batch.inner.sequence_number.saturating_add(1);
                executor.replay_batch(&batch, &node_state_root).await?;
                if self.shutdown_receiver.has_changed().unwrap_or(true) {
                    tracing::info!("The sequencer is shutting down. Exiting replay_soft_confirmations_on_top_of_node_state.");
                    return Ok(());
                }
            }
        };

        // Now, we need to catch up by...
        // - Replaying the txs that are already present in the in-progress batch
        // - Replaying any db events that come in while we're catching up

        // Replay the in-progress batch if it exists.
        let mut batch_is_in_progress = false;
        if let Some(batch) = in_progress_batch {
            batches_count += 1;
            transactions_count += batch.txs.len();
            batch_is_in_progress = true;
            let in_progress_batch = PreferredBatchToReplay {
                is_in_progress: true,
                visible_slot_number_after_increase: batch.visible_slot_number_after_increase,
                batch: batch.into_with_cached_tx_hashes(),
            };

            executor
                .replay_batch(&in_progress_batch, &node_state_root)
                .await?;
        }

        // Replay any db events that have come in while we're doing that catchup.
        // We don't require the channel to become completely empty because of the jitter that might introduce
        // Just get close, then lock the sequencer. This will keep p99 reasonable while hopefully
        // minimizing the risk of extremely long catchup periods in `update_state`.
        while db_event_subscription.len() > 2 {
            if self.shutdown_receiver.has_changed().unwrap_or(true) {
                tracing::info!("The sequencer is shutting down. Exiting replay_batch");
                return Ok(());
            }
            let event = db_event_subscription.try_recv().unwrap();
            Self::do_next_event(
                &mut executor,
                event,
                &mut batches_count,
                &mut transactions_count,
                &node_state_root,
                &mut batch_is_in_progress,
            )
            .await?;
        }

        let mut inner = self.lock_inner().await;
        let inner_lock_start_time = std::time::Instant::now();
        // Some events might come in while we're waiting to grab the lock.
        // Replay them.

        // We need to wait until the events have been flushed from the DB so we can replay them.
        // To be extra extra safe, we assign a unique ID to the flush;.
        let flush_id = Uuid::now_v7();
        inner
            .executor_events_sender
            .send(ExecutorEvent::Flush(flush_id))
            .await;

        loop {
            let event = db_event_subscription.recv().await;
            if self.shutdown_receiver.has_changed().unwrap_or(true) {
                tracing::info!("The sequencer is shutting down. Exiting replay_batch");
                return Ok(());
            }
            let Some(event) = event else {
                tracing::error!("DB event subscription closed while update state held the lock. This is a bug, please report it. Shutting down.");
                self.shutdown_sender.send(()).unwrap();
                return Err(anyhow::anyhow!("DB event subscription closed while update state held the lock. This is a bug, please report it"));
            };
            if let DbEvent::Flushed(id) = event {
                if id == flush_id {
                    break;
                } else {
                    tracing::error!("Another process flushed the DB while update state held the lock. This is a bug, please report it. Shutting down.");
                    self.shutdown_sender.send(()).unwrap();
                    return Err(anyhow::anyhow!("Another process flushed the DB while update state held the lock. This is a bug, please report it"));
                }
            }
            Self::do_next_event(
                &mut executor,
                event,
                &mut batches_count,
                &mut transactions_count,
                &node_state_root,
                &mut batch_is_in_progress,
            )
            .await?;
        }

        // The executor is now caught up. Swap it in
        inner.executor.replace_state(executor).await;
        inner.is_ready = Ok(());
        inner.latest_info = info;
        let checkpoint = inner
            .executor
            .checkpoint
            .clone_with_empty_witness_dropping_temp_cache();
        inner
            .executor_events_sender
            .send(ExecutorEvent::ForceUpdateApiState(checkpoint))
            .await;
        drop(db_event_subscription);
        self.update_api_ledger(&inner.latest_info);
        drop(inner); // Release the lock and allow transactions to progress while we handle metrics

        total_lock_duration += inner_lock_start_time.elapsed();
        let metrics = PreferredSequencerUpdateStateMetrics {
            duration: timer_start.elapsed(),
            lock_duration: total_lock_duration,
            batches_count,
            transactions_count: transactions_count
                .try_into()
                .expect("transactions in a single batch cannot possibly exceed u64::MAX"),
            in_progress_batch: batch_is_in_progress,
        };

        sov_metrics::track_metrics(|t| {
            t.submit(metrics);
        });
        if !self.shutdown_receiver.has_changed().unwrap_or(true) {
            // Get back in line for the lock, and trigger batch production if it's convenient.
            // Since pruning might be expensive and we've already held the lock for a while, we
            // prefer to drop the lock above and re-acquire it here to help keep p99 stable.
            let start_prune = std::time::Instant::now();
            let mut inner = self.lock_inner().await;
            let time_to_lock = start_prune.elapsed();
            if !self.is_replica().await? {
                inner.trigger_batch_production_if_convenient().await;
            }
            inner.prune_sequencer_db().await;
            drop(inner);
            let prune_duration = start_prune.elapsed();
            let lock_duration = prune_duration - time_to_lock;
            let metrics = PreferredSequencerPruneMetrics {
                duration_ms: prune_duration.as_millis() as u64,
                lock_duration_ms: lock_duration.as_millis() as u64,
            };
            sov_metrics::track_metrics(|t| {
                t.submit(metrics);
            });
        }

        Ok(())
    }

    /// Replay an event on the executor.
    #[tracing::instrument(skip_all, level = "warn", name = "update_state::do_next_event")]
    async fn do_next_event(
        executor: &mut RollupBlockExecutor<S, Rt>,
        event: DbEvent,
        batches_count: &mut u64,
        transactions_count: &mut usize,
        node_state_root: &<S::Storage as Storage>::Root,
        batch_is_in_progress: &mut bool,
    ) -> anyhow::Result<()> {
        match event {
            DbEvent::Flushed(id) => {
                tracing::trace!(%id, "Flushed");
            }
            DbEvent::TxAccepted(tx, hash) => {
                executor.replay_tx(hash, &tx).await;
                *transactions_count += 1;
                *batch_is_in_progress = true;
            }
            DbEvent::BatchClosed(_) => {
                tracing::trace!("Done replaying txs");
                executor.end_rollup_block().await;
                *batch_is_in_progress = false;
            }
            DbEvent::BatchStarted {
                sequence_number: _,
                visible_slot_number_after_increase,
                visible_slots_to_advance,
            } => {
                *batches_count += 1;
                executor
                    .start_rollup_block_for_replay(
                        visible_slot_number_after_increase,
                        visible_slots_to_advance,
                        node_state_root,
                        0,
                    )
                    .await;

                *batch_is_in_progress = true;
            }
            DbEvent::ProofBlobAccepted(_) => {
                // We don't do anything with proofs yet.
                // Note that we also don't change the state of the batch_is_in_progress flag here.
                tracing::trace!("Proof blob accepted");
            }
        }
        Ok(())
    }
}
