# sov-revenue-share

A Sovereign SDK module that automates revenue sharing across rollup applications.

## Quick facts

* **Default share:** 10 % (1 000 basis points) stored on‑chain and adjustable *downward* by the sovereign admin.
* **Activation:** Disabled after genesis; an admin transaction is required to start collecting.
* **Supported assets:** Any token recognised by the `sov-bank` module.
* **Isolation:** If sharing is disabled or the percentage is zero, calls to `share_revenue` do nothing.

## Installation

This module should come installed to the runtime of your rollup starter.

You just need to add a reference to the revenue-share module inside the module of your app that handles revenue collection, then call `revenue_share.compute_and_pay_revenue_share()`.

If your revenue collection set up is complicated, you also have access to `get_revenue_share_percentage_bps()` and `pay_revenue_share()` methods. To calculate the appropriate fee
and pay.

```rust, ignore
/// Your module 
#[derive(Clone, ModuleInfo, ModuleRestApi)]
pub struct Exchange<S: Spec> {
    /// The unique module identifier
    #[id]
    pub id: ModuleId,

    ...
    // The revenue share module
    #[module]
    pub revenue_share: RevenueShare<R>;
}
```

## Sharing flow

1. Your application module charges a fee, e.g. `total_fee`.
2. Compute and pay revenue share:

   ```rust, ignore
   revenue_share.compute_and_pay_revenue_share(&payer, token_id, total_fee, state)?;
   ```

This method transfers the correct share of the total fee from `payer` to the revenue-share module account.

## Admin interface

| Message                           | Purpose                         | Notes                                         |
| --------------------------------- | ------------------------------- | --------------------------------------------- |
| `ActivateRevenueShare`            | Start collecting fees           | Admin‑only                                    |
| `DeactivateRevenueShare`          | Pause collection                | Existing balances stay                        |
| `LowerRevenuePercentage { bps }` | Lower share percentage          | Cannot increase; `bps` ≤ current and ≤ 10 000 |
| `UpdateSovereignAdmin { addr }`   | Transfer admin role             |                                               |
| `WithdrawRewards { token_id }`    | Send accumulated funds to admin | Fails if balance is zero                      |

## Genesis

```jsonc
{
  "sov_revenue_share": {
    "sovereign_admin": "<SOV_ADMIN_ADDRESS>"
  }
}
```

## License

This crate is distributed under the Sovereign Commercial License and is only available to rollups that have purchased premium features.