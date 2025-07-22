use sov_modules_api::{FullyBakedTx, TxHash, VisibleSlotNumber};
use sov_test_utils::{generate_optimistic_runtime, TestSpec as S};

pub use super::*;

generate_optimistic_runtime!(TestRuntime <= );

type RT = TestRuntime<S>;

struct MockDbBackend {}

#[async_trait]
impl PreferredSequencerDbBackend for MockDbBackend {
    async fn read_in_progress_batch(&self) -> anyhow::Result<Option<InProgressBatch>> {
        Ok(None)
    }

    async fn begin_rollup_block(
        &mut self,
        _sequence_number: SequenceNumber,
        _blob_id: BlobInternalId,
        _visible_slot_number_after_increase: VisibleSlotNumber,
        _visible_slots_to_advance: NonZero<u8>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn add_tx(
        &mut self,
        _sequence_number: SequenceNumber,
        _tx_idx_within_batch: u64,
        _tx: FullyBakedTx,
        _hash: TxHash,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn end_rollup_block(
        &mut self,
        _in_progress_batch: &InProgressBatch,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn add_proof_blob(
        &mut self,
        _sequence_number: SequenceNumber,
        _blob_id: BlobInternalId,
        _data: Arc<[u8]>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn prune(&mut self, _prune_up_to_including: SequenceNumber) -> anyhow::Result<()> {
        Ok(())
    }

    async fn current_data(&self) -> anyhow::Result<DbSnapshotData> {
        Ok(DbSnapshotData::default())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_event_stream() {
    let (shutdown_sender, _) = tokio::sync::watch::channel(());
    let (mut db, _, _) =
        PreferredSequencerDb::<S, RT>::new(Box::new(MockDbBackend {}), shutdown_sender, false)
            .await
            .unwrap();

    let (sender, mut event_stream) = mpsc::channel(100);
    db.subscribe_to_events(sender);

    db.start_batch(VisibleSlotNumber::ONE, NonZero::new(1).unwrap(), 0)
        .await
        .unwrap();
    db.insert_tx(FullyBakedTx::new(vec![1u8; 100]), TxHash::new([0u8; 32]))
        .await
        .unwrap();
    db.insert_tx(FullyBakedTx::new(vec![2u8; 100]), TxHash::new([1u8; 32]))
        .await
        .unwrap();
    db.insert_proof_blob(0, Arc::new([3u8; 100]), 1)
        .await
        .unwrap();
    db.terminate_batch().await.unwrap();

    assert_eq!(
        event_stream.recv().await.unwrap(),
        DbEvent::BatchStarted {
            sequence_number: 0,
            visible_slot_number_after_increase: VisibleSlotNumber::ONE,
            visible_slots_to_advance: NonZero::new(1).unwrap(),
        }
    );

    assert_eq!(
        event_stream.recv().await.unwrap(),
        DbEvent::TxAccepted(FullyBakedTx::new(vec![1u8; 100]), TxHash::new([0u8; 32]))
    );
    assert_eq!(
        event_stream.recv().await.unwrap(),
        DbEvent::TxAccepted(FullyBakedTx::new(vec![2u8; 100]), TxHash::new([1u8; 32]))
    );

    assert_eq!(
        event_stream.recv().await.unwrap(),
        DbEvent::ProofBlobAccepted(1)
    );
    assert_eq!(event_stream.recv().await.unwrap(), DbEvent::BatchClosed(0));
}
