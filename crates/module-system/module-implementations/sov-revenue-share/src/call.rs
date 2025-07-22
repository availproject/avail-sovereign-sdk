use borsh::{BorshDeserialize, BorshSerialize};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sov_bank::TokenId;
use sov_modules_api::macros::UniversalWallet;
use sov_modules_api::Spec;

/// Call messages for the revenue share module
#[derive(
    Debug,
    Clone,
    PartialEq,
    BorshDeserialize,
    BorshSerialize,
    Serialize,
    Deserialize,
    JsonSchema,
    UniversalWallet,
    Eq,
)]
pub enum CallMessage<S: Spec> {
    /// Activate revenue sharing (admin only)
    ActivateRevenueShare,

    /// Deactivate revenue sharing (admin only)
    DeactivateRevenueShare,

    /// Lower the revenue share percentage (admin only)
    LowerRevenuePercentage {
        /// New percentage in basis points (e.g., 1000 = 10%)
        percentage_in_basis_points: u16,
    },

    /// Update the sovereign admin address (current admin only)
    UpdateSovereignAdmin {
        /// The new admin address
        new_admin: S::Address,
    },

    /// Withdraw accumulated rewards to the admin address (admin only)
    WithdrawRewards {
        /// The token ID to withdraw
        token_id: TokenId,
    },
}
