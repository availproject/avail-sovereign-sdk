use anyhow::Result;
use schemars::JsonSchema;
use sov_modules_api::macros::UniversalWallet;
use sov_modules_api::{Context, Spec, TxState};
use strum::{EnumDiscriminants, EnumIs, EnumIter, VariantArray};

use crate::OperatorIncentives;

/// This enumeration represents the available call messages for interacting with the sov-operator-incentives module.
#[derive(
    borsh::BorshDeserialize,
    borsh::BorshSerialize,
    serde::Serialize,
    serde::Deserialize,
    Debug,
    PartialEq,
    Eq,
    Clone,
    JsonSchema,
    EnumDiscriminants,
    EnumIs,
    UniversalWallet,
)]
#[schemars(bound = "S::Address: ::schemars::JsonSchema", rename = "CallMessage")]
#[serde(rename_all = "snake_case")]
#[strum_discriminants(derive(VariantArray, EnumIs, EnumIter))]
pub enum CallMessage<S: Spec> {
    UpdateRewardAddress {
        /// The new address that will receive rewards for operating the rollup.
        /// Note: We do not verify possession of the corresponding private key,
        /// so it's possible to set an address for which the `sender` does not control the private key.
        new_reward_address: S::Address,
    },
}

impl<S: Spec> OperatorIncentives<S> {
    /// Update the reward address.
    pub fn update_address(
        &mut self,
        new_reward_address: S::Address,
        context: &Context<S>,
        state: &mut impl TxState<S>,
    ) -> Result<()> {
        let current_reward_address = self.reward_address.get(state)?.ok_or_else(|| {
            anyhow::anyhow!(
                "Reward address is not set in the OperatorIncentives module. This is a bug."
            )
        })?;

        let sender = context.sender();

        if sender != &current_reward_address {
            anyhow::bail!("{sender} is not authorized to update the reward address; only {current_reward_address} can do so.");
        }
        self.reward_address.set(&new_reward_address, state)?;
        Ok(())
    }
}
