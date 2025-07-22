use borsh::{BorshDeserialize, BorshSerialize};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sov_modules_api::Spec;

/// Genesis configuration for the revenue share module
#[derive(
    Debug, Clone, PartialEq, BorshDeserialize, BorshSerialize, Serialize, Deserialize, JsonSchema,
)]
pub struct GenesisConfig<S: Spec> {
    /// The initial sovereign admin address
    pub sovereign_admin: S::Address,
}
