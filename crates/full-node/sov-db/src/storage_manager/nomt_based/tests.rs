use std::path::Path;

use nomt::trie::KeyPath;
use sha2::Digest;
use sov_mock_da::{MockDaSpec, MockHash};
use sov_rollup_interface::common::SlotNumber;

use super::{NomtChangeSet, NomtStorageManager, StateFinishedSession};
use crate::accessory_db::AccessoryDb;
use crate::config::RollupDbConfig;
use crate::historical_state::HistoricalStateReader;
use crate::namespaces::{KernelNamespace, UserNamespace};
use crate::storage_manager::tests::arbitrary::ForkDescription;
use crate::storage_manager::tests::data_helpers::verify_accessory_db;
use crate::storage_manager::tests::generic_tests::{
    calls_on_empty, check_snapshots_ordering, create_state_after_not_saved_block,
    double_create_storage, double_save_changes, finalize_only_last_block,
    ledger_finalized_height_is_updated_on_start, linear_progression, minimal_fork_bfs,
    parallel_forks_reading_while_finalization_is_happening, removed_fork_data_view,
    several_jumping_forks, test_exploration, unknown_block_cannot_be_saved, ExplorationMode,
    TestableStorage, TestableStorageManager,
};
use crate::test_utils::{TestNomtStorage, H};

impl std::fmt::Debug for TestNomtStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestNomtStorage")
            .field("state_session", &"<StateSession>")
            .field("accessory_db", &self.accessory_db)
            .finish()
    }
}

impl TestableStorage for TestNomtStorage {
    type ChangeSet = NomtChangeSet;

    fn materialize_from_key_values(self, items: &[(Vec<u8>, Option<Vec<u8>>)]) -> Self::ChangeSet {
        let TestNomtStorage {
            state_session_builder,
            historical_state: _,
            accessory_db: _,
        } = self;

        let user_session = state_session_builder.begin_user_session().unwrap();
        let kernel_session = state_session_builder.begin_kernel_session().unwrap();

        let mut state_writes = Vec::with_capacity(items.len());
        let mut accessory_writes = Vec::with_capacity(items.len());

        for (key, value) in items {
            let key_path = KeyPath::from(sha2::Sha256::digest(key));
            kernel_session.warm_up(key_path);
            user_session.warm_up(key_path);
            state_writes.push((key_path, nomt::KeyReadWrite::Write(value.clone())));
            accessory_writes.push((key.clone(), value.clone()));
        }

        state_writes.sort_by_key(|(k, _)| *k);

        let user_finished_session = user_session.finish(state_writes.clone()).unwrap();
        let kernel_finished_session = kernel_session.finish(state_writes.clone()).unwrap();

        let accessory_change_set =
            AccessoryDb::materialize_values(accessory_writes.clone(), SlotNumber::GENESIS).unwrap();

        let historical_change_set = HistoricalStateReader::materialize_values(
            accessory_writes.clone(),
            accessory_writes.clone(),
            // Not used at the moment,
            items.len().to_be_bytes().to_vec(),
            SlotNumber::GENESIS,
        )
        .unwrap();

        NomtChangeSet {
            state: StateFinishedSession {
                user: user_finished_session,
                kernel: kernel_finished_session,
            },
            historical_state: historical_change_set,
            accessory: accessory_change_set,
        }
    }

    fn get_value(&self, key: &[u8]) -> Option<Vec<u8>> {
        let schema_key = key.to_vec();
        let key_path = KeyPath::from(sha2::Sha256::digest(key));
        let kernel_value = {
            let kernel_session = self.state_session_builder.begin_kernel_session().unwrap();
            kernel_session.read(key_path).unwrap()
        };
        let user_value = {
            let user_session = self.state_session_builder.begin_user_session().unwrap();
            user_session.read(key_path).unwrap()
        };
        assert_eq!(kernel_value, user_value);

        let accessory_value = self
            .accessory_db
            .get_value_option(&schema_key, SlotNumber::GENESIS)
            .unwrap();
        assert_eq!(accessory_value, kernel_value);

        let historical_value_user = self
            .historical_state
            .get_value_option_by_key::<UserNamespace>(SlotNumber::GENESIS, &schema_key)
            .unwrap();
        assert_eq!(historical_value_user, kernel_value);

        let historical_value_kernel = self
            .historical_state
            .get_value_option_by_key::<KernelNamespace>(SlotNumber::GENESIS, &schema_key)
            .unwrap();
        assert_eq!(historical_value_kernel, kernel_value);

        kernel_value
    }
}

type Sm = NomtStorageManager<MockDaSpec, H, TestNomtStorage>;

impl TestableStorageManager for Sm {
    fn new(path: impl AsRef<Path>) -> Self {
        let config = RollupDbConfig::default_in_path(path.as_ref().to_path_buf());
        Sm::new(config).unwrap()
    }

    fn verify_stf_storage(stf_storage: &Self::StfState, expected_values: &[(u64, MockHash)]) {
        for (expected_height, expected_hash) in expected_values {
            let key = expected_height.to_be_bytes().to_vec();
            let actual_value = stf_storage.get_value(&key);
            let expected_value = Some(expected_hash.0.to_vec());
            assert_eq!(actual_value, expected_value);
        }

        // TODO: Verify historical state too!
        verify_accessory_db(&stf_storage.accessory_db, expected_values);
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }

    fn snapshots_count(&self) -> usize {
        self.snapshots_count()
    }

    fn blocks_to_parent_count(&self) -> usize {
        self.blocks_to_parent_count()
    }
}

#[test_log::test]
fn test_manager_linear_progression() {
    // Instant finality
    linear_progression::<Sm>(5, 0);
    // Non-instant finality
    linear_progression::<Sm>(5, 1);
    linear_progression::<Sm>(5, 4);
    linear_progression::<Sm>(5, 5);
    linear_progression::<Sm>(5, 6);
    linear_progression::<Sm>(5, 10);
}

#[test_log::test]
fn nomt_minimal_fork_bfs() {
    minimal_fork_bfs::<Sm>();
}

#[test_strategy::proptest]
#[ignore = "Too slow on MacOS currently"]
fn proptest_nomt_forks_exploration(fork: ForkDescription) {
    test_exploration::<Sm>(fork.clone(), ExplorationMode::Bfs);
    test_exploration::<Sm>(fork, ExplorationMode::Dfs);
}

#[test]
fn test_calls_on_empty() {
    calls_on_empty::<Sm>();
}

#[test]
fn test_double_create_storage() {
    double_create_storage::<Sm>();
}

#[test]
fn test_unknown_block_cannot_be_saved() {
    unknown_block_cannot_be_saved::<Sm>();
}

#[test]
fn test_double_save_changes() {
    double_save_changes::<Sm>();
}

#[test]
fn test_create_state_after_not_saved_block() {
    create_state_after_not_saved_block::<Sm>();
}

#[test]
fn test_finalize_only_last_block() {
    finalize_only_last_block::<Sm>();
}

// TODO: Needs to be converted to benchmark
#[test]
fn flaky_test_parallel_forks_reading_while_finalization_is_happening() {
    parallel_forks_reading_while_finalization_is_happening::<Sm>();
}

#[test]
fn test_several_jumping_forks() {
    several_jumping_forks::<Sm>();
}

#[test]
fn test_removed_fork_view() {
    removed_fork_data_view::<Sm>();
}

#[test]
fn test_snapshots_ordering() {
    check_snapshots_ordering::<Sm>();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_ledger_finalized_height_is_updated_on_start() {
    ledger_finalized_height_is_updated_on_start::<Sm>().await;
}
