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
- `indexer_referral_rewards`

This avoids mixing user data and billing state into the append-heavy index.

## Email Verification

Email verification uses Postmark when `LEDGERFLOW_POSTMARK_SERVER_TOKEN` is set.

Flow:

1. `POST /admin/auth/register/start`
2. Generate a short-lived code.
3. Store only the SHA-256 hash of the code.
4. Send the code via Postmark.
5. Reject repeat sends for the same email and purpose within 60 seconds.
6. `POST /admin/auth/register/verify`
7. Mark the user email verified.
8. Store the password using Argon2 PHC format.
9. Issue an HttpOnly `ledgerflow_session` cookie.

The local scaffold returns `email_delivery: disabled` when Postmark is not configured.

Password reset uses the same email-code mechanism through `/admin/auth/password-reset/start` and `/admin/auth/password-reset/verify`.

Before public launch, add Cloudflare Turnstile to registration, password reset, and login. See [cloudflare_turnstile.md](cloudflare_turnstile.md).

## Launch Pricing

The public product currently exposes one plan only:

- Plan id: `launch`
- Launch price: `0.009 ETH / month`
- Regular reference price: `0.018 ETH / month`
- Included usage: `1,000,000` API requests per month

The backend seeds and lists only the `launch` plan. Payment submission rejects non-launch plans even if older plan rows still exist in a local Postgres database from previous builds.

The launch price is intentionally narrow and discounted: the product is not selling a broad multi-chain RPC plan, it is selling Base address-to-top-level-transaction lookup for bots, wallet ops, and data teams.

## Referral Rewards

Each verified referred account grants the referrer `25,000` bonus API requests for the current month, capped at `250,000` bonus requests per month.

The reward is deliberately modest because email-only referral programs are easy to farm. Production hardening should add:

- Turnstile on registration and login
- IP and ASN rate limits in Cloudflare
- payment-required rewards for larger bonuses
- abuse review for repeated disposable email domains

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

The newer dashboard counters use `indexer_usage_daily_by_source`, which separates:

- `usage_source`: `subscription` or `bonus`
- `outcome`: `success` or `error`

Quota priority is:

1. active subscription quota, when an active subscription exists
2. signup/referral bonus quota only when no active subscription exists

Each registered verified account gets `1,000` signup bonus requests. Authenticated public API failures are also written to `indexer_api_request_failures` so users can debug recent failed calls from the dashboard.
