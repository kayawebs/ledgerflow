pub fn home() -> &'static str {
    r##"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>LedgerFlow Indexer</title>
  <style>
    :root {
      --ink: #181510;
      --muted: #6e665c;
      --paper: #fbf7ef;
      --card: #fffaf1;
      --line: #ded0bb;
      --accent: #c56a2d;
      --accent-dark: #8f3f18;
      --sage: #697d57;
      --coal: #231f18;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      color: var(--ink);
      background:
        radial-gradient(circle at 15% 10%, rgba(197, 106, 45, .18), transparent 28rem),
        radial-gradient(circle at 85% 18%, rgba(105, 125, 87, .18), transparent 24rem),
        linear-gradient(135deg, #fbf7ef 0%, #f4ecdc 100%);
      font-family: Georgia, "Times New Roman", serif;
    }
    a { color: inherit; text-decoration: none; }
    .nav {
      display: flex; justify-content: space-between; align-items: center;
      padding: 24px clamp(20px, 5vw, 72px);
    }
    .brand { font-size: 22px; font-weight: 800; letter-spacing: .08em; text-transform: uppercase; }
    .navlinks { display: flex; gap: 18px; align-items: center; font-family: ui-sans-serif, system-ui; }
    .pill {
      border: 1px solid var(--line); border-radius: 999px; padding: 10px 16px;
      background: rgba(255, 250, 241, .72);
    }
    .hero {
      padding: 52px clamp(20px, 5vw, 72px) 36px;
      display: grid; grid-template-columns: minmax(0, 1.12fr) minmax(320px, .88fr); gap: 42px;
      align-items: center;
    }
    h1 { font-size: clamp(48px, 8vw, 104px); line-height: .9; letter-spacing: -.06em; margin: 0; }
    .lede { font-family: ui-sans-serif, system-ui; font-size: 20px; line-height: 1.65; color: var(--muted); max-width: 740px; }
    .actions { display: flex; flex-wrap: wrap; gap: 14px; margin-top: 30px; font-family: ui-sans-serif, system-ui; }
    .button {
      display: inline-flex; align-items: center; justify-content: center;
      border-radius: 999px; padding: 14px 20px; border: 1px solid var(--coal);
      background: var(--coal); color: var(--paper); font-weight: 750;
    }
    .button.secondary { background: transparent; color: var(--coal); border-color: var(--line); }
    .terminal {
      background: #17130f; color: #f7ead8; border-radius: 28px; padding: 24px;
      box-shadow: 0 30px 80px rgba(35, 31, 24, .22);
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
      transform: rotate(1deg);
    }
    .terminal pre { white-space: pre-wrap; margin: 0; line-height: 1.75; font-size: 13px; color: #f1d7bc; }
    .grid {
      padding: 36px clamp(20px, 5vw, 72px) 76px;
      display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 18px;
    }
    .card {
      border: 1px solid var(--line); background: rgba(255, 250, 241, .72);
      border-radius: 26px; padding: 24px; min-height: 210px;
    }
    .eyebrow { font-family: ui-sans-serif, system-ui; text-transform: uppercase; letter-spacing: .18em; font-size: 12px; color: var(--accent-dark); }
    .card h2 { margin: 18px 0 10px; font-size: 28px; letter-spacing: -.03em; }
    .card p { color: var(--muted); line-height: 1.6; font-family: ui-sans-serif, system-ui; }
    @media (max-width: 880px) {
      .hero, .grid { grid-template-columns: 1fr; }
      .nav { align-items: flex-start; gap: 18px; flex-direction: column; }
    }
  </style>
</head>
<body>
  <nav class="nav">
    <a class="brand" href="/">LedgerFlow</a>
    <div class="navlinks">
      <a href="/docs">Docs</a>
      <a href="/login">Login</a>
      <a class="pill" href="/dashboard">Dashboard</a>
    </div>
  </nav>
  <main class="hero">
    <section>
      <div class="eyebrow">Base address intelligence API</div>
      <h1>Fast transaction discovery for bots and onchain operators.</h1>
      <p class="lede">LedgerFlow Indexer turns a Base address set into replayable top-level transaction references. It is built for bot developers, wallet operations, project treasuries, and analytics teams that need reliable address-to-transaction lookup before deeper analysis.</p>
      <div class="actions">
        <a class="button" href="/login">Create API access</a>
        <a class="button secondary" href="/docs">Read API docs</a>
      </div>
    </section>
    <aside class="terminal">
      <pre>curl https://indexer.ledgerflow.top/v1/base/address/0x.../txs \
  -H "Authorization: Bearer lf_live_xxx"

{
  "chain": "base",
  "items": [
    { "tx_hash": "0x...", "direction": "from" }
  ]
}</pre>
    </aside>
  </main>
  <section class="grid">
    <article class="card"><div class="eyebrow">Indexed primitive</div><h2>Top-level from/to</h2><p>Simple, fast, and predictable address reference lookup. No explorer label guessing, no ABI dependency, no semantic overclaiming.</p></article>
    <article class="card"><div class="eyebrow">Operator workflow</div><h2>Cluster-ready</h2><p>Use batch address queries to feed bot/EOA clusters into downstream spend analysis, backfills, and accounting jobs.</p></article>
    <article class="card"><div class="eyebrow">Product surface</div><h2>API keys and billing</h2><p>Email verification, metered API keys, and Base ETH subscription payments are built into the dashboard flow.</p></article>
  </section>
</body>
</html>"##
}

pub fn login() -> &'static str {
    r##"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Login | LedgerFlow Indexer</title>
  <style>
    :root { --ink:#17130f; --paper:#fbf7ef; --line:#dacbb5; --muted:#6f675d; --accent:#b85d24; --green:#617751; }
    * { box-sizing: border-box; }
    body { margin:0; min-height:100vh; color:var(--ink); background:linear-gradient(135deg,#fbf7ef,#efe2cf); font-family: Georgia,"Times New Roman",serif; display:grid; place-items:center; padding:24px; }
    .shell { width:min(980px,100%); display:grid; grid-template-columns:.95fr 1.05fr; gap:18px; }
    .panel { border:1px solid var(--line); border-radius:30px; background:rgba(255,250,241,.82); padding:30px; }
    h1 { font-size: clamp(38px,6vw,72px); line-height:.92; letter-spacing:-.055em; margin:0 0 20px; }
    p, label, button, input, .status, a { font-family:ui-sans-serif,system-ui; }
    p { color:var(--muted); line-height:1.6; }
    label { display:block; font-size:12px; text-transform:uppercase; letter-spacing:.14em; color:var(--muted); margin:18px 0 8px; }
    input { width:100%; border:1px solid var(--line); border-radius:16px; padding:14px 15px; background:#fffaf1; color:var(--ink); font-size:15px; }
    button { width:100%; margin-top:16px; border:0; border-radius:999px; padding:14px 18px; background:var(--ink); color:var(--paper); font-weight:760; cursor:pointer; }
    button.secondary { background:transparent; color:var(--ink); border:1px solid var(--line); }
    .status { margin-top:16px; white-space:pre-wrap; color:var(--muted); line-height:1.5; }
    .nav { position:fixed; top:22px; left:24px; right:24px; display:flex; justify-content:space-between; font-family:ui-sans-serif,system-ui; }
    .nav a { color:var(--ink); text-decoration:none; }
    @media (max-width:820px) { .shell { grid-template-columns:1fr; padding-top:64px; } }
  </style>
</head>
<body>
  <nav class="nav"><a href="/">LedgerFlow</a><a href="/docs">Docs</a></nav>
  <main class="shell">
    <section class="panel">
      <h1>Sign in with email verification.</h1>
      <p>Use the same flow for login and registration. We send a short-lived code through Postmark. After verification, continue to the dashboard to create API keys and activate a subscription.</p>
    </section>
    <section class="panel">
      <label for="email">Email</label>
      <input id="email" type="email" placeholder="operator@example.com" autocomplete="email">
      <button id="send">Send verification code</button>
      <label for="code">Verification code</label>
      <input id="code" placeholder="123456" inputmode="numeric">
      <button id="verify">Verify and enter dashboard</button>
      <button class="secondary" onclick="location.href='/dashboard'">I already have access</button>
      <div id="status" class="status"></div>
    </section>
  </main>
  <script>
    const statusEl = document.getElementById("status");
    const setStatus = (value) => { statusEl.textContent = typeof value === "string" ? value : JSON.stringify(value, null, 2); };
    async function post(url, body) {
      const res = await fetch(url, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
      return data;
    }
    document.getElementById("send").onclick = async () => {
      try {
        const email = document.getElementById("email").value.trim();
        localStorage.setItem("ledgerflow_email", email);
        const data = await post("/admin/auth/email/start", { email });
        setStatus(data.dev_code_hint ? `Code sent. Dev code: ${data.dev_code_hint}` : "Code sent. Check your email.");
      } catch (err) { setStatus(err.message); }
    };
    document.getElementById("verify").onclick = async () => {
      try {
        const email = document.getElementById("email").value.trim();
        const code = document.getElementById("code").value.trim();
        const data = await post("/admin/auth/email/verify", { email, code });
        localStorage.setItem("ledgerflow_email", data.user.email);
        location.href = "/dashboard";
      } catch (err) { setStatus(err.message); }
    };
    document.getElementById("email").value = localStorage.getItem("ledgerflow_email") || "";
  </script>
</body>
</html>"##
}

pub fn dashboard() -> &'static str {
    r##"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Dashboard | LedgerFlow Indexer</title>
  <style>
    :root { --ink:#17130f; --paper:#fbf7ef; --card:#fffaf1; --line:#dacbb5; --muted:#6f675d; --accent:#b85d24; --green:#617751; --red:#9f3a2d; }
    * { box-sizing:border-box; }
    body { margin:0; color:var(--ink); background:radial-gradient(circle at 10% 0%,rgba(184,93,36,.16),transparent 28rem),linear-gradient(135deg,#fbf7ef,#efe2cf); font-family:Georgia,"Times New Roman",serif; }
    .nav { display:flex; justify-content:space-between; align-items:center; padding:22px clamp(18px,4vw,54px); }
    .brand { font-weight:900; letter-spacing:.08em; text-transform:uppercase; }
    .nav a, button, input, select, textarea, p, label, .mono, .status { font-family:ui-sans-serif,system-ui; }
    a { color:inherit; text-decoration:none; }
    .wrap { padding:12px clamp(18px,4vw,54px) 64px; }
    .hero { display:flex; justify-content:space-between; gap:24px; align-items:end; margin-bottom:22px; }
    h1 { font-size:clamp(42px,7vw,84px); line-height:.9; letter-spacing:-.06em; margin:0; }
    .muted { color:var(--muted); line-height:1.55; }
    .grid { display:grid; grid-template-columns:repeat(12,1fr); gap:16px; }
    .card { grid-column:span 6; border:1px solid var(--line); border-radius:26px; background:rgba(255,250,241,.86); padding:22px; }
    .card.wide { grid-column:span 12; }
    .card.third { grid-column:span 4; }
    h2 { margin:0 0 12px; font-size:26px; letter-spacing:-.03em; }
    label { display:block; font-size:11px; text-transform:uppercase; letter-spacing:.14em; color:var(--muted); margin:14px 0 7px; }
    input, select, textarea { width:100%; border:1px solid var(--line); border-radius:14px; background:#fffaf1; color:var(--ink); padding:12px 13px; font-size:14px; }
    textarea { min-height:96px; resize:vertical; }
    button { border:0; border-radius:999px; background:var(--ink); color:var(--paper); padding:12px 16px; font-weight:760; cursor:pointer; margin-top:14px; }
    button.secondary { background:transparent; border:1px solid var(--line); color:var(--ink); }
    .status, pre { white-space:pre-wrap; border-radius:18px; padding:14px; background:#18130f; color:#f7ead8; overflow:auto; }
    pre { min-height:120px; font-size:12px; line-height:1.6; }
    .metric { font-size:38px; font-weight:800; letter-spacing:-.05em; }
    .mono { font-family:ui-monospace,SFMono-Regular,Menlo,monospace; font-size:12px; word-break:break-all; }
    @media (max-width:900px) { .card, .card.third { grid-column:span 12; } .hero { display:block; } }
  </style>
</head>
<body>
  <nav class="nav"><a class="brand" href="/">LedgerFlow</a><div><a href="/docs">Docs</a> &nbsp; <a href="/login">Login</a></div></nav>
  <main class="wrap">
    <section class="hero">
      <div><h1>Indexer dashboard.</h1><p class="muted">Manage API keys, activate subscriptions, and test Base address queries from the same origin your customers will use.</p></div>
      <button class="secondary" id="clear">Clear local session</button>
    </section>
    <section class="grid">
      <article class="card third"><h2>Account</h2><label>Email</label><input id="email" type="email" placeholder="operator@example.com"><button id="save-email">Save email</button><p class="muted">Email verification happens on the login page. This dashboard uses your verified email for key and payment operations.</p></article>
      <article class="card third"><h2>Indexer</h2><div class="metric" id="progress">—</div><p class="muted">Last indexed Base block. Requires a valid API key with an active subscription.</p><button id="load-progress">Load progress</button></article>
      <article class="card third"><h2>Billing</h2><p class="muted">Treasury</p><div class="mono" id="treasury">loading...</div><p class="muted">Confirmations required: <span id="confirmations">—</span></p><button id="load-config">Reload billing config</button></article>

      <article class="card">
        <h2>Create API key</h2>
        <label>Key name</label><input id="key-name" placeholder="production">
        <button id="create-key">Create key</button>
        <p class="muted">The raw key is only shown once. Store it before refreshing.</p>
        <div class="status" id="key-status">No key created in this browser session.</div>
      </article>

      <article class="card">
        <h2>Activate subscription</h2>
        <label>Plan</label><select id="plan"></select>
        <label>Payment tx hash</label><input id="payment-tx" placeholder="0x...">
        <label>From address (optional but recommended)</label><input id="payment-from" placeholder="0x...">
        <button id="submit-payment">Verify payment</button>
        <div class="status" id="payment-status">Submit a Base ETH transfer tx hash after sending payment.</div>
      </article>

      <article class="card wide">
        <h2>Test address lookup</h2>
        <div class="grid">
          <div class="card third"><label>API key</label><input id="api-key" placeholder="lf_live_xxx"></div>
          <div class="card third"><label>Address</label><input id="query-address" placeholder="0x..."></div>
          <div class="card third"><label>Direction</label><select id="direction"><option value="">any</option><option value="from">from</option><option value="to">to</option></select></div>
        </div>
        <button id="query">Query latest txs</button>
        <pre id="query-result">Waiting for query.</pre>
      </article>
    </section>
  </main>
  <script>
    const $ = (id) => document.getElementById(id);
    const email = localStorage.getItem("ledgerflow_email") || "";
    const apiKey = localStorage.getItem("ledgerflow_api_key") || "";
    $("email").value = email;
    $("api-key").value = apiKey;

    async function getJson(url, headers = {}) {
      const res = await fetch(url, { headers });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
      return data;
    }
    async function postJson(url, body) {
      const res = await fetch(url, { method:"POST", headers:{"content-type":"application/json"}, body:JSON.stringify(body) });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
      return data;
    }
    const authHeaders = () => ({ Authorization: `Bearer ${$("api-key").value.trim()}` });

    $("save-email").onclick = () => { localStorage.setItem("ledgerflow_email", $("email").value.trim()); };
    $("clear").onclick = () => { localStorage.removeItem("ledgerflow_email"); localStorage.removeItem("ledgerflow_api_key"); location.href="/"; };

    async function loadConfig() {
      const config = await getJson("/admin/config");
      $("treasury").textContent = config.billing.treasury_address;
      $("confirmations").textContent = config.billing.required_confirmations;
      const plans = await getJson("/admin/billing/plans");
      $("plan").innerHTML = plans.plans.map((p) => `<option value="${p.plan_id}">${p.name} · ${Number(BigInt(p.monthly_price_wei)) / 1e18} ETH / ${p.monthly_request_quota.toLocaleString()} req</option>`).join("");
    }
    $("load-config").onclick = () => loadConfig().catch((err) => alert(err.message));

    $("create-key").onclick = async () => {
      try {
        const data = await postJson("/admin/api-keys", { email: $("email").value.trim(), name: $("key-name").value.trim() || "production" });
        const key = data.api_key.api_key;
        localStorage.setItem("ledgerflow_api_key", key);
        $("api-key").value = key;
        $("key-status").textContent = JSON.stringify(data, null, 2);
      } catch (err) { $("key-status").textContent = err.message; }
    };

    $("submit-payment").onclick = async () => {
      try {
        const payload = { email: $("email").value.trim(), plan_id: $("plan").value, tx_hash: $("payment-tx").value.trim(), from_address: $("payment-from").value.trim() || null };
        const data = await postJson("/admin/billing/base-eth/payments", payload);
        $("payment-status").textContent = JSON.stringify(data, null, 2);
      } catch (err) { $("payment-status").textContent = err.message; }
    };

    $("load-progress").onclick = async () => {
      try {
        const data = await getJson("/v1/chains/base/progress", authHeaders());
        $("progress").textContent = data.last_indexed_block ?? "null";
      } catch (err) { $("progress").textContent = "blocked"; alert(err.message); }
    };

    $("query").onclick = async () => {
      try {
        localStorage.setItem("ledgerflow_api_key", $("api-key").value.trim());
        const params = new URLSearchParams({ limit: "25" });
        if ($("direction").value) params.set("direction", $("direction").value);
        const data = await getJson(`/v1/base/address/${$("query-address").value.trim()}/txs?${params}`, authHeaders());
        $("query-result").textContent = JSON.stringify(data, null, 2);
      } catch (err) { $("query-result").textContent = err.message; }
    };

    loadConfig().catch((err) => { $("treasury").textContent = err.message; });
  </script>
</body>
</html>"##
}

pub fn docs() -> &'static str {
    r##"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Docs | LedgerFlow Indexer</title>
  <style>
    :root { --ink:#17130f; --paper:#fbf7ef; --card:#fffaf1; --line:#dacbb5; --muted:#6f675d; --accent:#b85d24; }
    * { box-sizing:border-box; }
    body { margin:0; color:var(--ink); background:#fbf7ef; font-family:Georgia,"Times New Roman",serif; }
    .nav { display:flex; justify-content:space-between; padding:22px clamp(18px,4vw,54px); border-bottom:1px solid var(--line); background:#fffaf1; position:sticky; top:0; }
    .nav a, p, li, code, pre, th, td { font-family:ui-sans-serif,system-ui; }
    a { color:inherit; text-decoration:none; }
    main { width:min(1080px,100%); margin:0 auto; padding:42px 22px 80px; }
    h1 { font-size:clamp(42px,7vw,82px); line-height:.9; letter-spacing:-.06em; margin:0 0 28px; }
    h2 { margin-top:38px; font-size:30px; }
    p, li { color:var(--muted); line-height:1.65; }
    pre { background:#18130f; color:#f7ead8; border-radius:22px; padding:18px; overflow:auto; line-height:1.65; }
    code { background:#efe2cf; padding:2px 5px; border-radius:5px; }
    table { width:100%; border-collapse:collapse; background:#fffaf1; border:1px solid var(--line); border-radius:18px; overflow:hidden; }
    th, td { text-align:left; padding:12px; border-bottom:1px solid var(--line); }
    th { color:var(--accent); text-transform:uppercase; letter-spacing:.12em; font-size:12px; }
  </style>
</head>
<body>
  <nav class="nav"><a href="/">LedgerFlow</a><div><a href="/login">Login</a> &nbsp; <a href="/dashboard">Dashboard</a></div></nav>
  <main>
    <h1>LedgerFlow Indexer API docs.</h1>
    <p>LedgerFlow Indexer is a Base top-level address reference API. It answers which transaction hashes touched an address as top-level <code>from</code> or <code>to</code>, and is designed as a fast discovery layer for bots, wallet operations, and transaction analysis systems.</p>

    <h2>Authentication</h2>
    <pre>Authorization: Bearer lf_live_xxx</pre>
    <p>Public <code>/v1</code> endpoints require an API key with an active subscription. Usage is metered daily per key and endpoint.</p>

    <h2>Endpoints</h2>
    <table>
      <tr><th>Method</th><th>Path</th><th>Purpose</th></tr>
      <tr><td>GET</td><td><code>/health</code></td><td>Service health</td></tr>
      <tr><td>GET</td><td><code>/v1/chains/base/progress</code></td><td>Last indexed Base block</td></tr>
      <tr><td>GET</td><td><code>/v1/base/address/{address}/txs</code></td><td>Full address transaction refs</td></tr>
      <tr><td>GET</td><td><code>/v1/base/address/{address}/tx-hashes</code></td><td>Deduplicated tx hashes</td></tr>
      <tr><td>POST</td><td><code>/v1/base/addresses/txs</code></td><td>Batch address lookup</td></tr>
    </table>

    <h2>Address transaction refs</h2>
    <pre>curl "https://indexer.ledgerflow.top/v1/base/address/0xabc.../txs?direction=from&limit=100" \
  -H "Authorization: Bearer lf_live_xxx"</pre>

    <h2>Batch lookup</h2>
    <pre>curl "https://indexer.ledgerflow.top/v1/base/addresses/txs" \
  -H "Authorization: Bearer lf_live_xxx" \
  -H "content-type: application/json" \
  -d '{
    "addresses": ["0x111...", "0x222..."],
    "from_block": 30000000,
    "direction": "any",
    "limit": 1000
  }'</pre>

    <h2>Query parameters</h2>
    <ul>
      <li><code>from_block</code>: inclusive lower block bound.</li>
      <li><code>to_block</code>: inclusive upper block bound.</li>
      <li><code>direction</code>: <code>from</code>, <code>to</code>, or omitted/<code>any</code>.</li>
      <li><code>limit</code>: bounded by server-side max limit.</li>
    </ul>

    <h2>What this is not</h2>
    <p>This is not a full explorer clone and does not infer protocol semantics. Use it to discover transaction candidates, then run deeper evidence-first analysis elsewhere.</p>
  </main>
</body>
</html>"##
}
