mod db;
mod in_flight_blob;

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use db::{BlobSenderDb, BlobToSend};
use in_flight_blob::{track_num_of_in_flight_blobs, InFlightBlob, InFlightBlobInfo};
use sov_db::ledger_db::LedgerDb;
use sov_modules_api::{DaSpec, EventModuleName, RuntimeEventResponse};
use sov_rollup_interface::common::HexHash;
use sov_rollup_interface::node::da::{DaService, SubmitBlobReceipt};
use sov_rollup_interface::node::ledger_api::{LedgerStateProvider, QueryMode};
use sov_rollup_interface::node::{future_or_shutdown, FutureOrShutdownOutput};
use tokio::sync::{oneshot, watch, Mutex};
use tokio::task::JoinHandle;
use tokio::time::{interval, sleep};
use tracing::{debug, error, info, trace};

/// Uniquely identifies a blob managed by the [`BlobSender`].
///
/// Unfortunately, the blob hash can only be known *after*
/// submission to a [`DaService`]. In practice, this means that we need
/// some other way of identifying in-flight blobs short of using the entire blob
/// data as the blob ID (no thanks).
///
/// [`BlobInternalId`] values ought to be generated using UUIDv7s, which
/// makes them strictly monotonically increasing. It's also important to note
/// that [`BlobInternalId`]s ought to be instantiated by
/// callers, rather than [`BlobSender`]. This ensures no loss of data happens in
/// case of a crash at an inconvenient time.
pub type BlobInternalId = u128;

const LEDGER_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// If a blob is not published within this number of retries, the rollup will exit.
pub const MAX_NB_OF_BLOB_SUBMISSION_RETRIES: u8 = 3;

/// See [`BlobInternalId`].
pub fn new_blob_id() -> BlobInternalId {
    uuid::Uuid::now_v7().as_u128()
}

/// Hooks for [`BlobSender`] events.
///
/// We guarantee at-least-once delivery of all events.
#[async_trait]
pub trait BlobSenderHooks: Send + Sync + 'static {
    type Da: DaSpec;

    /// The blob was published and is now part of the DA's canonical chain.
    async fn on_published_blob(
        &self,
        _blob_id: BlobInternalId,
        _blob_hash: [u8; 32],
        _da_tx_id: <Self::Da as DaSpec>::TransactionId,
    ) {
    }

    /// The blob was processed by the rollup node, but it may not be finalized yet.
    async fn on_processed_blob(
        &self,
        _blob_id: BlobInternalId,
        _blob_hash: [u8; 32],
        _da_tx_id: <Self::Da as DaSpec>::TransactionId,
    ) {
    }

    /// The blob is considered to be finalized by the rollup node.
    async fn on_finalized_blob(
        &self,
        _blob_id: BlobInternalId,
        _blob_hash: [u8; 32],
        _da_tx_id: &<Self::Da as DaSpec>::TransactionId,
    ) {
    }
}

/// A reusable component that manages blob submission to the [`DaService`].
pub struct BlobSender<Da: DaService, H, FM: FinalizationManager> {
    db: Arc<BlobSenderDb>,
    hooks: Arc<H>,
    in_flight_blobs: Arc<Mutex<HashMap<BlobInternalId, InFlightBlob<Da::Spec>>>>,
    shutdown_receiver: watch::Receiver<()>,
    shutdown_sender: watch::Sender<()>,
    da: Da,
    finalization_manager: FM,
    nb_of_concurrent_blob_submissions: Arc<AtomicUsize>,
    blob_processing_timeout: Duration,
    ledger_pool_interval: Duration,
}

impl<Da, H, FM> BlobSender<Da, H, FM>
where
    Da: DaService,
    H: BlobSenderHooks<Da = Da::Spec>,
    FM: FinalizationManager,
{
    pub async fn new(
        da: Da,
        finalization_manager: FM,
        storage_path: &Path,
        hooks: H,
        shutdown_sender: watch::Sender<()>,
        blob_processing_timeout: Duration,
    ) -> anyhow::Result<(Self, JoinHandle<()>)> {
        Self::new_with_task_intervals(
            da,
            finalization_manager,
            storage_path,
            hooks,
            shutdown_sender,
            blob_processing_timeout,
            LEDGER_POLL_INTERVAL,
        )
        .await
    }

    pub async fn new_with_task_intervals(
        da: Da,
        finalization_manager: FM,
        storage_path: &Path,
        hooks: H,
        shutdown_sender: watch::Sender<()>,
        blob_processing_timeout: Duration,
        ledger_pool_interval: Duration,
    ) -> anyhow::Result<(Self, JoinHandle<()>)> {
        let shutdown_receiver = shutdown_sender.subscribe();
        let db = Arc::new(BlobSenderDb::new(storage_path).await?);

        let all_blobs = db.get_all::<Da::Spec>().await?;

        let hooks = Arc::new(hooks);
        let in_flight_blobs: Arc<Mutex<_>> = Default::default();

        let mut sender = Self {
            db,
            hooks,
            in_flight_blobs: in_flight_blobs.clone(),
            shutdown_receiver: shutdown_receiver.clone(),
            shutdown_sender,
            da,
            finalization_manager,
            nb_of_concurrent_blob_submissions: Arc::new(AtomicUsize::new(0)),
            blob_processing_timeout,
            ledger_pool_interval,
        };

        let handle = Self::main_task(in_flight_blobs, shutdown_receiver).await;

        for b in all_blobs {
            sender
                .publish_blob_inner(b.blob, b.blob_id, b.latest_known_processing_state)
                .await?;
        }

        Ok((sender, handle))
    }

    /// Number of concurrent blob submissions in flight.
    pub fn nb_of_concurrent_blob_submissions(&self) -> usize {
        self.nb_of_concurrent_blob_submissions
            .load(Ordering::Relaxed)
    }

    /// Returns a handle to the (atomic) number of blob submissions currently in flight.
    pub fn nb_of_in_flight_blobs_handle(&self) -> Arc<AtomicUsize> {
        self.nb_of_concurrent_blob_submissions.clone()
    }

    fn inc_nb_of_concurrent_blob_submissions(&self) {
        self.nb_of_concurrent_blob_submissions
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Returns a reference to the [`BlobSenderHooks`] instance.
    pub fn hooks(&self) -> &H {
        &self.hooks
    }

    /// Can be called again with the same [`BlobInternalId`] to resume publishing.
    pub async fn publish_batch_blob(
        &mut self,
        data: Arc<[u8]>,
        id: BlobInternalId,
    ) -> anyhow::Result<()> {
        self.publish_blob_inner(
            BlobToSend::Batch { data },
            id,
            BlobProcessingState::MustSubmit,
        )
        .await
    }

    /// Can be called again with the same [`BlobInternalId`] to resume publishing.
    pub async fn publish_proof_blob(
        &mut self,
        data: Arc<[u8]>,
        id: BlobInternalId,
    ) -> anyhow::Result<()> {
        self.publish_blob_inner(
            BlobToSend::Proof { data },
            id,
            BlobProcessingState::MustSubmit,
        )
        .await
    }

    async fn publish_blob_inner(
        &mut self,
        blob: BlobToSend,
        blob_id: BlobInternalId,
        latest_known_processing_state: BlobProcessingState<Da::Spec>,
    ) -> anyhow::Result<()> {
        if self.shutdown_receiver.has_changed()? {
            info!("BlobSender: shutdown signal received, skipping blob submission");
            return Ok(());
        }

        // It is ok to hold the lock here because:
        //  1. The logic below is not blocking.
        //  2. The lock is shared only between this method and the cleanup task which is invoked only once.
        let mut blobs = self.in_flight_blobs.lock().await;
        if blobs.contains_key(&blob_id) {
            info!(
                blob_id,
                "No need to publish blob as it's already in-flight or awaiting finalization. Skipping."
            );
            return Ok(());
        }

        self.db.push(blob.clone(), blob_id).await?;

        let is_batch = matches!(blob, BlobToSend::Batch { .. });

        let task_state = TaskState {
            da: self.da.clone(),
            finalization_manager: self.finalization_manager.clone(),
            db: self.db.clone(),
            hooks: self.hooks.clone(),
            in_flight_blobs: self.in_flight_blobs.clone(),
            nb_of_concurrent_blob_submissions: self.nb_of_concurrent_blob_submissions.clone(),
            blob_processing_timeout: self.blob_processing_timeout,
            ledger_pool_interval: self.ledger_pool_interval,
            shutdown_sender: self.shutdown_sender.clone(),
        };

        let shutdown_receiver = self.shutdown_receiver.clone();

        self.inc_nb_of_concurrent_blob_submissions();
        let handle = tokio::task::spawn({
            let state = task_state;
            let blob = blob.clone();
            let latest_known_processing_state = latest_known_processing_state.clone();

            async move {
                let fut = state.manage_blob_submission_inside_task(
                    blob,
                    blob_id,
                    latest_known_processing_state,
                );
                let res = future_or_shutdown(fut, &shutdown_receiver).await;

                match res {
                    FutureOrShutdownOutput::Output(()) | FutureOrShutdownOutput::Shutdown => {}
                }
                state.dec_nb_of_concurrent_blob_submissions();
            }
        });

        blobs.insert(
            blob_id,
            InFlightBlob {
                handle,
                info: InFlightBlobInfo {
                    blob_iid: blob_id,
                    start_time: std::time::Instant::now(),
                    is_batch,
                    size_in_bytes: blob.data().len() as u64,
                    was_resurrected: false,
                    last_known_state: latest_known_processing_state.clone(),
                },
            },
        );

        // TODO: handle errors from the spawned tasks.
        blobs.retain(|_, b| !b.handle.is_finished());

        Ok(())
    }

    async fn main_task(
        in_flight_blobs: Arc<Mutex<HashMap<BlobInternalId, InFlightBlob<Da::Spec>>>>,
        mut shutdown_receiver: watch::Receiver<()>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut metrics_interval = interval(Duration::from_secs(10));
            loop {
                let fut = future_or_shutdown(metrics_interval.tick(), &shutdown_receiver);

                match fut.await {
                    FutureOrShutdownOutput::Shutdown => {
                        if let Err(err) = shutdown_receiver.changed().await {
                            error!(%err, "BlobSender: The shutdown sender was dropped, shutting down anyway");
                        }
                    }
                    FutureOrShutdownOutput::Output(_) => {
                        let infos = {
                            let mut in_flight_blobs = in_flight_blobs.lock().await;
                            in_flight_blobs.retain(|_, b| !b.handle.is_finished());
                            in_flight_blobs
                                .values()
                                .map(|b| b.info.clone())
                                .collect::<Vec<_>>()
                        };

                        let len = infos.len();
                        sov_metrics::track_metrics(|tracker| {
                            tracker.submit_inline("sov_rollup_blobs_enter_scope", "foo=1");
                            for b in infos {
                                tracker.submit(b);
                            }
                            tracker.submit_inline("sov_rollup_blobs_exit_scope", "foo=1");
                        });

                        track_num_of_in_flight_blobs(len as u64);

                        continue;
                    }
                }

                let mut blobs = in_flight_blobs.lock().await;

                debug!(
                    num_handles_to_join = blobs.len(),
                    "Exiting the blob sender background task..."
                );

                let blobs = std::mem::take(&mut *blobs);

                for (_, b) in blobs.into_iter() {
                    if let Err(err) = b.handle.await {
                        error!(%err, blob_info = ?b.info, "Error in a blob sender background task.");
                    }
                }

                break;
            }
        })
    }
}

type BlobReceiptFut<Da> = oneshot::Receiver<
    Result<
        SubmitBlobReceipt<<<Da as DaService>::Spec as DaSpec>::TransactionId>,
        <Da as DaService>::Error,
    >,
>;

#[async_trait]
/// Decides if a given blob was finalized on the DA.
pub trait FinalizationManager: Clone + Send + Sync + 'static {
    async fn is_blob_finalized(
        &self,
        blob_hash: HexHash,
        blob_id: BlobInternalId,
    ) -> anyhow::Result<Option<bool>>;
}

#[async_trait]
impl FinalizationManager for LedgerDb {
    async fn is_blob_finalized(
        &self,
        blob_hash: HexHash,
        _blob_id: BlobInternalId,
    ) -> anyhow::Result<Option<bool>> {
        let Some(batch) = self
            .get_batch_by_hash::<(), (), RuntimeEventResponse<IgnoreEvent>>(
                &blob_hash.0,
                QueryMode::Compact,
            )
            .await?
        else {
            return Ok(None);
        };

        let slot_number = batch.slot_number;
        let latest_finalized_slot_number = self.get_latest_finalized_slot_number().await?;
        Ok(Some(slot_number <= latest_finalized_slot_number))
    }
}

struct TaskState<Da: DaService, FM: FinalizationManager> {
    da: Da,
    finalization_manager: FM,
    db: Arc<BlobSenderDb>,
    hooks: Arc<dyn BlobSenderHooks<Da = Da::Spec>>,
    in_flight_blobs: Arc<Mutex<HashMap<BlobInternalId, InFlightBlob<Da::Spec>>>>,
    nb_of_concurrent_blob_submissions: Arc<AtomicUsize>,
    blob_processing_timeout: Duration,
    ledger_pool_interval: Duration,
    shutdown_sender: watch::Sender<()>,
}

impl<Da: DaService, FM: FinalizationManager> TaskState<Da, FM> {
    fn dec_nb_of_concurrent_blob_submissions(&self) {
        self.nb_of_concurrent_blob_submissions
            .fetch_sub(1, Ordering::Relaxed);
    }

    async fn remove_blob_or_err(&self, blob_id: BlobInternalId) -> anyhow::Result<()> {
        let res = self.db.remove(blob_id).await;
        if let Err(err) = &res {
            tracing::error!(error = %err, ?blob_id, "BlobSender: unable to remove blob.");
        }
        res
    }

    async fn save_blob_state_or_err(
        &self,
        blob_id: BlobInternalId,
        state: &BlobProcessingState<Da::Spec>,
    ) -> anyhow::Result<()> {
        let res = self.db.set_state(blob_id, state).await;
        if let Err(err) = &res {
            tracing::error!(
                "BlobSender: unable to save blob state: {state:?}, error: {err}, blob_id: {blob_id}. Shutting down."
            );
        }
        res
    }

    async fn is_blob_finalized_or_err(
        &self,
        blob_hash: HexHash,
        blob_id: BlobInternalId,
        state: &BlobProcessingState<Da::Spec>,
    ) -> anyhow::Result<Option<bool>> {
        let is_finalized = self
            .finalization_manager
            .is_blob_finalized(blob_hash, blob_id)
            .await;

        if let Err(err) = &is_finalized {
            tracing::error!(
                "BlobSender: unable to check if blob is finalized: {state:?}, error: {err}, blob_id: {blob_id}. Shutting down."
            );
        }

        is_finalized
    }

    async fn check_timeout(
        &self,
        start_time: SystemTime,
        blob_hash: HexHash,
        da_tx_id: &<<Da as DaService>::Spec as DaSpec>::TransactionId,
    ) -> bool {
        let elapsed = match start_time.elapsed() {
            Ok(elapsed) => elapsed,
            Err(err) => {
                tracing::error!(
                    %blob_hash,
                    ?da_tx_id,
                    error = ?err,
                    timer = ?start_time,
                    "BlobSender: unable to get elapsed time for blob submission.",
                );
                return true;
            }
        };

        if elapsed > self.blob_processing_timeout {
            tracing::error!(
                %blob_hash,
                ?da_tx_id,
                timer = ?start_time,
                blob_processing_timeout = ?self.blob_processing_timeout,
                ?elapsed,
                "BlobSender: elapsed time for blob submission exceeded the resubmit interval.",
            );
            return true;
        }

        false
    }

    #[tracing::instrument(skip(self, blob), level = "debug")]
    async fn manage_blob_submission_inside_task(
        &self,
        blob: BlobToSend,
        blob_id: BlobInternalId,
        latest_known_processing_state: BlobProcessingState<Da::Spec>,
    ) {
        let mut blob_state = latest_known_processing_state;

        let mut nb_of_retries_attempted = 0;

        loop {
            trace!(?blob_state, ?blob_id, "Tracking blob submission state");
            let blob_state_clone = blob_state.clone();
            {
                if let Some(b) = self.in_flight_blobs.lock().await.get_mut(&blob_id) {
                    b.info.last_known_state = blob_state_clone.clone();
                }
            }

            match &blob_state {
                BlobProcessingState::MustSubmit => {
                    if self
                        .save_blob_state_or_err(blob_id, &blob_state)
                        .await
                        .is_err()
                    {
                        // If we can't save the state, we shut down.
                        let _ = self.shutdown_sender.send(());
                        return;
                    }

                    let receipt_fut = self.send_blob(blob.clone()).await;

                    match receipt_fut.await {
                        Ok(Ok(receipt)) => {
                            blob_state = BlobProcessingState::Published { receipt };
                        }
                        err => {
                            tracing::error!(
                                %blob_id,
                                error = ?err,
                                ?blob_state,
                                "BlobSender: unable to send blob. Shutting down."
                            );
                            let _ = self.shutdown_sender.send(());
                            return;
                        }
                    }

                    nb_of_retries_attempted += 1;
                }
                BlobProcessingState::Published { receipt } => {
                    if self
                        .save_blob_state_or_err(blob_id, &blob_state)
                        .await
                        .is_err()
                    {
                        // If we can't save the state, we shut down.
                        let _ = self.shutdown_sender.send(());
                        return;
                    }

                    self.hooks
                        .on_published_blob(
                            blob_id,
                            receipt.blob_hash.into(),
                            receipt.da_transaction_id.clone(),
                        )
                        .await;

                    let timer = SystemTime::now();
                    loop {
                        let blob_hash = receipt.blob_hash;
                        let da_tx_id = &receipt.da_transaction_id;
                        if self.check_timeout(timer, blob_hash, da_tx_id).await {
                            if nb_of_retries_attempted >= MAX_NB_OF_BLOB_SUBMISSION_RETRIES {
                                tracing::error!(
                                    nb_of_retries_attempted,
                                    MAX_NB_OF_BLOB_SUBMISSION_RETRIES,
                                    ?da_tx_id,
                                    %blob_hash,
                                    "Shutting down the rollup. Blob submission failed."
                                );
                                let _ = self.shutdown_sender.send(());
                                return;
                            }

                            blob_state = BlobProcessingState::MustSubmit;
                            break;
                        }

                        let finality_status = match self
                            .is_blob_finalized_or_err(blob_hash, blob_id, &blob_state)
                            .await
                        {
                            Ok(finality_status) => finality_status,
                            Err(_) => {
                                // If we can't check the finality status, we shut down.
                                let _ = self.shutdown_sender.send(());
                                return;
                            }
                        };

                        match finality_status {
                            Some(_) => {
                                // Never skip directly to `Finalized` state, or
                                // we won't send out the notification.
                                blob_state = BlobProcessingState::Processed {
                                    receipt: receipt.clone(),
                                };
                                break;
                            }
                            None => {
                                sleep(Duration::from_secs(1)).await;
                            }
                        }
                    }
                }
                BlobProcessingState::Processed { receipt } => {
                    if self
                        .save_blob_state_or_err(blob_id, &blob_state)
                        .await
                        .is_err()
                    {
                        // If we can't save the state, we shut down.
                        let _ = self.shutdown_sender.send(());
                        return;
                    }

                    self.hooks
                        .on_processed_blob(
                            blob_id,
                            receipt.blob_hash.into(),
                            receipt.da_transaction_id.clone(),
                        )
                        .await;

                    loop {
                        let finality_status = match self
                            .is_blob_finalized_or_err(receipt.blob_hash, blob_id, &blob_state)
                            .await
                        {
                            Ok(finality_status) => finality_status,
                            Err(_) => {
                                // If we can't check the finality status, we shut down.
                                let _ = self.shutdown_sender.send(());
                                return;
                            }
                        };

                        match finality_status {
                            Some(false) => {
                                sleep(self.ledger_pool_interval).await;
                                continue;
                            }
                            Some(true) => {
                                blob_state = BlobProcessingState::Finalized {
                                    receipt: receipt.clone(),
                                };
                                break;
                            }
                            None => {
                                debug!(
                                    blob_id,
                                    blob_hash = %receipt.blob_hash,
                                    "Re-org detected; resubmitting blob"
                                );
                                blob_state = BlobProcessingState::MustSubmit;
                                break;
                            }
                        }
                    }
                }
                BlobProcessingState::Finalized { receipt, .. } => {
                    // Upon crashing, we'd rather call the hook twice rather than not
                    // calling it at all. So, we call it *before* removing the blob from
                    // the database.
                    self.hooks
                        .on_finalized_blob(
                            blob_id,
                            receipt.blob_hash.into(),
                            &receipt.da_transaction_id,
                        )
                        .await;

                    // We won't shut down the rollup in case of this error, but we will log the error.
                    let _ = self.remove_blob_or_err(blob_id).await;
                    break;
                }
            }
        }
    }

    async fn send_blob(&self, blob: BlobToSend) -> BlobReceiptFut<Da> {
        trace!(
            blob_len = blob.data().len(),
            "Will attempt to publish blob to DA"
        );

        match blob {
            BlobToSend::Batch { data } => self.da.send_transaction(&data).await,
            BlobToSend::Proof { data } => self.da.send_proof(&data).await,
        }
    }
}

struct BlobSubmissionRequest<Da: DaSpec> {
    blob: BlobToSend,
    blob_id: BlobInternalId,
    latest_known_processing_state: BlobProcessingState<Da>,
}

#[derive(derive_more::Debug, Clone, serde::Serialize, serde::Deserialize)]
#[debug(bounds())]
enum BlobProcessingState<Da: DaSpec> {
    MustSubmit,
    Published {
        receipt: SubmitBlobReceipt<Da::TransactionId>,
    },
    Processed {
        receipt: SubmitBlobReceipt<Da::TransactionId>,
    },
    Finalized {
        receipt: SubmitBlobReceipt<Da::TransactionId>,
    },
}

/// We use it as a [`RuntimeEventResponse`] generic when we don't care about event data.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    borsh::BorshSerialize,
    borsh::BorshDeserialize,
)]
struct IgnoreEvent;

impl EventModuleName for IgnoreEvent {
    fn module_name(&self) -> &'static str {
        "ignore"
    }
}
