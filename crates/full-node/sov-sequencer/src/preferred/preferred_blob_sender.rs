use std::sync::Arc;

use sov_blob_sender::{BlobInternalId, BlobSender};
use sov_blob_storage::{PreferredBatchData, PreferredProofData};
use sov_db::ledger_db::LedgerDb;
use sov_rollup_interface::node::da::DaService;
use tracing::debug;

use super::db::{PreferredSequencerReadBatch, PreferredSequencerReadBlob};
use crate::common::TxStatusBlobSenderHooks;

/// Wrapper around [`BlobSender`] with preferred blob -specific logic.
#[derive(derive_more::Deref, derive_more::From)]
pub struct PreferredBlobSender<Da: DaService> {
    inner: BlobSender<Da, TxStatusBlobSenderHooks<Da::Spec>, LedgerDb>,
    #[deref(ignore)]
    is_replica: bool,
}

impl<Da: DaService> PreferredBlobSender<Da> {
    pub async fn publish_proof(
        &mut self,
        proof_data: Arc<[u8]>,
        sequence_number: u64,
        blob_id: BlobInternalId,
    ) -> anyhow::Result<()> {
        let blob = PreferredProofData {
            sequence_number,
            data: proof_data.to_vec(),
        };
        let blob_bytes = Arc::from(borsh::to_vec(&blob)?);

        debug!(
            sequence_number,
            blob_id, "Dispatching proof blob for publishing"
        );

        self.inner.publish_proof_blob(blob_bytes, blob_id).await?;

        Ok(())
    }

    pub async fn publish_batch(
        &mut self,
        batch: PreferredSequencerReadBatch,
    ) -> anyhow::Result<()> {
        let serialized_batch = borsh::to_vec::<PreferredBatchData>(&PreferredBatchData {
            sequence_number: batch.sequence_number,
            visible_slots_to_advance: batch.visible_slots_to_advance,
            data: batch.txs,
        })?
        .into();

        self.inner
            .publish_batch_blob(serialized_batch, batch.blob_id)
            .await?;

        Ok(())
    }

    pub async fn publish_blobs(
        &mut self,
        completed_blobs: Vec<PreferredSequencerReadBlob>,
    ) -> anyhow::Result<()> {
        if self.is_replica {
            return Ok(());
        }

        for blob in completed_blobs {
            match blob {
                PreferredSequencerReadBlob::Batch(batch) => {
                    self.publish_batch(batch).await?;
                }
                PreferredSequencerReadBlob::Proof {
                    data,
                    sequence_number,
                    blob_id,
                } => {
                    self.publish_proof(data, sequence_number, blob_id).await?;
                }
            }
        }

        Ok(())
    }
}
