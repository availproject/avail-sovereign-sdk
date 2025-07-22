use nomt::Options;
use schemars::JsonSchema;

/// Configuration for Sovereign Rollup node database.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Eq, PartialEq, JsonSchema)]
pub struct RollupDbConfig {
    /// Path where all databases are stored
    pub path: std::path::PathBuf,
    // User state configuration
    /// Number of concurrent commit workers for user state.
    /// More details at [`Options::commit_concurrency`]
    pub user_commit_concurrency: Option<usize>,
    /// Value is determined by the expected size of the state. Recommended to start with 15_000_000.
    /// Cannot be changed for the existing database.
    /// More details at [`Options::hashtable_buckets`]
    pub user_hashtable_buckets: Option<u32>,
    /// Page cache size for user state.
    /// More details at [`Options::page_cache_size`]
    pub user_page_cache_size: Option<usize>,
    /// Leaf cache size for user state.
    /// More details at [`Options::leaf_cache_size`]
    pub user_leaf_cache_size: Option<usize>,

    // Kernel state configuration
    /// Number of concurrent commit workers for kernel state.
    /// More details at [`Options::commit_concurrency`]
    pub kernel_commit_concurrency: Option<usize>,
    /// Cannot be changed for the existing database.
    /// More details at [`Options::hashtable_buckets`]
    pub kernel_hashtable_buckets: Option<u32>,
    /// Page cache size for kernel state.
    /// More details at [`Options::page_cache_size`]
    pub kernel_page_cache_size: Option<usize>,
    /// Leaf cache size for kernel state.
    /// More details at [`Options::leaf_cache_size`]
    pub kernel_leaf_cache_size: Option<usize>,
}

impl RollupDbConfig {
    /// Helper for development
    #[cfg(feature = "test-utils")]
    pub fn default_in_path(path: std::path::PathBuf) -> Self {
        Self {
            path,
            user_commit_concurrency: Some(4),
            user_hashtable_buckets: Some(if cfg!(debug_assertions) {
                1_000_000
            } else {
                15_000_000
            }),
            user_page_cache_size: None,
            user_leaf_cache_size: None,
            kernel_commit_concurrency: Some(2),
            kernel_hashtable_buckets: None,
            kernel_page_cache_size: None,
            kernel_leaf_cache_size: None,
        }
    }

    /// Kernel state is smaller, so we can have less required options.
    /// But it has rollback enabled for 2 database commit support
    pub(crate) fn get_kernel_options(&self) -> Options {
        let mut opts = nomt_default_options();
        // Enable rollback, so we can handle errors with commits to 2 databases.
        opts.rollback(true);
        opts.max_rollback_log_len(1);
        opts.commit_concurrency(
            self.kernel_commit_concurrency
                .expect("`kernel_commit_concurrency` concurrency must be set"),
        );
        opts.hashtable_buckets(self.kernel_hashtable_buckets.unwrap_or(256_000));
        if let Some(page_cache_size) = self.kernel_page_cache_size {
            opts.page_cache_size(page_cache_size);
        }
        if let Some(leaf_cache_size) = self.kernel_leaf_cache_size {
            opts.leaf_cache_size(leaf_cache_size);
        }
        opts.path(self.path.join("kernel_nomt_db"));
        opts
    }

    pub(crate) fn get_user_options(&self) -> Options {
        let mut opts = nomt_default_options();
        opts.commit_concurrency(
            self.user_commit_concurrency
                .expect("`user_commit_concurrency` must be set"),
        );
        opts.hashtable_buckets(
            self.user_hashtable_buckets
                .expect("`user_hashtable_buckets` must be set"),
        );
        if let Some(page_cache_size) = self.user_page_cache_size {
            opts.page_cache_size(page_cache_size);
        }
        if let Some(leaf_cache_size) = self.user_leaf_cache_size {
            opts.leaf_cache_size(leaf_cache_size);
        }
        opts.path(self.path.join("user_nomt_db"));
        opts
    }
}

fn nomt_default_options() -> Options {
    let mut opts = Options::new();
    opts.metrics(true);
    opts
}
