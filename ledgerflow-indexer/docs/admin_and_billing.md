# Admin And Billing

The admin backend is intentionally separate from the ClickHouse indexing path.

## Storage Boundary

ClickHouse stores indexed chain references:

- `address_tx_refs`
- `indexer_progress`

Postgres stores product state:

- `indexer_users`
- `indexer_email_otps`
- `indexer_api_keys`
- `indexer_plans`
- `indexer_subscriptions`
- `indexer_payments`
- `indexer_usage_daily`

This avoids mixing user data and billing state into the append-heavy index.

## Email Verification

Email verification uses Postmark when `LEDGERFLOW_POSTMARK_SERVER_TOKEN` is set.

Flow:

1. `POST /admin/auth/email/start`
2. Generate a short-lived code.
3. Store only the SHA-256 hash of the code.
4. Send the code via Postmark.
5. `POST /admin/auth/email/verify`
6. Mark the user email verified.

The local scaffold returns `email_delivery: disabled` when Postmark is not configured.

## Base ETH Subscription Payments

The intended billing flow is:

1. User selects a plan.
2. User transfers ETH on Base to a treasury or assigned deposit address.
3. User submits the payment transaction hash.
4. Backend verifies immediately through the configured Base RPC:
   - transaction exists on Base
   - receipt status is success
   - `to` matches the billing address
   - `from` matches the user's wallet, if bound
   - `value` satisfies the selected plan
   - block confirmations are sufficient
5. The payment is marked `verified`.
6. A 30-day active subscription is created.

For production, assigned deposit addresses are cleaner than one shared treasury because plain ETH transfers do not carry a reliable memo.

## API Key Enforcement

Public `/v1/...` endpoints require:

- `Authorization: Bearer lf_live_xxx`
- non-revoked API key
- verified user email
- active subscription whose current period includes now

Every successful public request increments `indexer_usage_daily` by `(date, user_id, api_key_id, endpoint)`.
