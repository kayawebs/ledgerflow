# Cloudflare Turnstile Setup

Use Turnstile on the registration, password reset, and email-code send flows before public launch.

## Cloudflare Dashboard

1. Open Cloudflare Dashboard.
2. Go to **Turnstile**.
3. Create a widget for `indexer.ledgerflow.top`.
4. Use **Managed** mode first.
5. Copy the site key and secret key.

## Frontend Integration

Add the Turnstile script to `/login`:

```html
<script src="https://challenges.cloudflare.com/turnstile/v0/api.js" async defer></script>
```

Add a widget near the submit buttons:

```html
<div
  class="cf-turnstile"
  data-sitekey="YOUR_SITE_KEY"
  data-action="auth">
</div>
```

When the form submits, read the token:

```js
const token = document.querySelector("[name='cf-turnstile-response']").value;
```

Send it to the backend as `turnstile_token`.

## Backend Validation

The backend must validate every token with Cloudflare Siteverify. Client-side rendering alone is not protection.

Endpoint:

```text
POST https://challenges.cloudflare.com/turnstile/v0/siteverify
```

Required fields:

- `secret`: private Turnstile secret key from Cloudflare
- `response`: token from the browser

Recommended fields:

- `remoteip`: visitor IP
- `idempotency_key`: UUID for safe retries

Reject the request unless Cloudflare returns `success: true`.

## LedgerFlow Variables To Add

```text
LEDGERFLOW_TURNSTILE_SITE_KEY=
LEDGERFLOW_TURNSTILE_SECRET_KEY=
LEDGERFLOW_TURNSTILE_EXPECTED_HOSTNAME=indexer.ledgerflow.top
```

## Where To Enforce

Protect these endpoints first:

- `POST /admin/auth/register/start`
- `POST /admin/auth/password-reset/start`
- `POST /admin/auth/login`

Keep the existing 60-second email send cooldown. Turnstile reduces bot traffic, but it does not replace rate limits.

## Cloudflare-Level Rules

In Cloudflare WAF, add rate limiting for:

- `/admin/auth/register/start`
- `/admin/auth/password-reset/start`
- `/admin/auth/login`

Suggested starting point:

- 5 requests per minute per IP for code sending
- 20 requests per minute per IP for login
- Managed Challenge when exceeded

Also enable Bot Fight Mode or Super Bot Fight Mode if your Cloudflare plan supports it.
