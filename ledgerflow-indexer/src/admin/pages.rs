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
    .brand { font-size: 22px; font-weight: 800; letter-spacing: .08em; text-transform: uppercase; display:inline-flex; align-items:center; gap:10px; }
    .brand svg { width:34px; height:34px; }
    .navlinks { display: flex; gap: 18px; align-items: center; font-family: ui-sans-serif, system-ui; }
    .navlinks button { border:0; background:transparent; color:inherit; cursor:pointer; font:inherit; padding:0; }
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
    <a class="brand" href="/"><svg viewBox="0 0 64 64" aria-hidden="true"><rect x="7" y="7" width="50" height="50" rx="16" fill="#17130f"/><path d="M19 39V18h8v16h18v5H19Z" fill="#fbf7ef"/><path d="M28 46c10-1 17-7 19-18h-7c-2 6-6 10-12 12v6Z" fill="#c56a2d"/></svg><span>LedgerFlow</span></a>
    <div class="navlinks">
      <a href="/docs">Docs</a>
      <a href="/pricing">Pricing</a>
      <a href="/login" data-auth-link>Login</a>
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
  <script>
    async function installAuthNav() {
      const link = document.querySelector("[data-auth-link]");
      if (!link) return;
      const res = await fetch("/admin/auth/me");
      if (!res.ok) return;
      const button = document.createElement("button");
      button.textContent = "Logout";
      button.onclick = async () => {
        await fetch("/admin/auth/logout", { method:"POST", headers:{"content-type":"application/json"}, body:"{}" }).catch(() => {});
        localStorage.removeItem("ledgerflow_email");
        localStorage.removeItem("ledgerflow_api_key");
        location.href = "/login";
      };
      link.replaceWith(button);
    }
    installAuthNav().catch(() => {});
  </script>
</body>
</html>"##
}

pub fn pricing() -> &'static str {
    r##"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Pricing | LedgerFlow Indexer</title>
  <style>
    :root { --ink:#17130f; --paper:#fbf7ef; --line:#dacbb5; --muted:#6f675d; --accent:#b85d24; --green:#617751; }
    * { box-sizing:border-box; }
    body { margin:0; color:var(--ink); background:radial-gradient(circle at 78% 12%,rgba(97,119,81,.18),transparent 26rem),linear-gradient(135deg,#fbf7ef,#efe2cf); font-family:Georgia,"Times New Roman",serif; }
    .nav { display:flex; justify-content:space-between; align-items:center; padding:24px clamp(20px,5vw,72px); font-family:ui-sans-serif,system-ui; }
    .brand { display:inline-flex; align-items:center; gap:10px; font-weight:850; letter-spacing:.08em; text-transform:uppercase; }
    .brand svg { width:34px; height:34px; }
    a { color:inherit; text-decoration:none; }
    main { width:min(1120px,100%); margin:0 auto; padding:42px 22px 86px; }
    h1 { font-size:clamp(46px,8vw,96px); line-height:.9; letter-spacing:-.06em; margin:0; }
    p, li, button, .tag, .metric { font-family:ui-sans-serif,system-ui; }
    .nav button { border:0; background:transparent; color:inherit; cursor:pointer; font:inherit; padding:0; }
    .lede { color:var(--muted); font-size:19px; line-height:1.65; max-width:760px; }
    .price-card { margin-top:34px; border:1px solid var(--line); border-radius:34px; background:rgba(255,250,241,.88); padding:30px; display:grid; grid-template-columns:1.05fr .95fr; gap:22px; box-shadow:0 28px 80px rgba(23,19,15,.08); }
    .tag { display:inline-flex; border:1px solid var(--line); border-radius:999px; padding:8px 12px; color:var(--accent); background:#fffaf1; font-weight:750; }
    .price { font-size:clamp(54px,9vw,110px); line-height:.9; letter-spacing:-.07em; margin:18px 0 8px; }
    .strike { color:var(--muted); text-decoration:line-through; font-family:ui-sans-serif,system-ui; font-size:22px; }
    ul { padding-left:20px; color:var(--muted); line-height:1.75; }
    .button { display:inline-flex; margin-top:18px; border-radius:999px; padding:14px 20px; background:var(--ink); color:var(--paper); font-family:ui-sans-serif,system-ui; font-weight:760; }
    .aside { border-radius:26px; background:#18130f; color:#f7ead8; padding:24px; }
    .aside p { color:#dbc8b4; line-height:1.65; }
    .referral { margin-top:18px; border:1px dashed #806b58; border-radius:22px; padding:18px; }
    @media (max-width:840px) { .price-card { grid-template-columns:1fr; } }
  </style>
</head>
<body>
  <nav class="nav"><a class="brand" href="/"><svg viewBox="0 0 64 64" aria-hidden="true"><rect x="7" y="7" width="50" height="50" rx="16" fill="#17130f"/><path d="M19 39V18h8v16h18v5H19Z" fill="#fbf7ef"/><path d="M28 46c10-1 17-7 19-18h-7c-2 6-6 10-12 12v6Z" fill="#c56a2d"/></svg><span>LedgerFlow</span></a><div><a href="/docs">Docs</a> &nbsp; <a href="/pricing">Pricing</a> &nbsp; <a href="/login" data-auth-link>Login</a></div></nav>
  <main>
    <span class="tag">Launch pricing · limited discount</span>
    <h1>One plan for Base builders.</h1>
    <p class="lede">General RPC providers price broad multi-chain infrastructure; LedgerFlow Indexer is narrower and cheaper for the specific job of Base address-to-transaction discovery.</p>
    <section class="price-card">
      <div>
        <div class="tag">Launch</div>
        <div class="price">0.009 ETH</div>
        <div class="strike">0.018 ETH / month regular price</div>
        <p class="lede">Includes 1,000,000 metered API requests per month for Base top-level address lookup.</p>
        <ul>
          <li>Address transaction refs and deduplicated tx hashes</li>
          <li>Batch address lookup for bot and EOA clusters</li>
          <li>API key dashboard and usage metering</li>
          <li>Base ETH subscription payment</li>
        </ul>
        <a class="button" href="/subscribe">Subscribe with launch discount</a>
      </div>
      <aside class="aside">
        <h2>Referral bonus</h2>
        <p>Invite verified users and receive bonus API usage. The reward is intentionally modest to reduce email farming incentives.</p>
        <div class="referral">
          <div class="metric">25,000 bonus requests</div>
          <p>Per verified referred account, capped at 250,000 bonus requests per month.</p>
        </div>
      </aside>
    </section>
	  </main>
  <script>
    async function installAuthNav() {
      const link = document.querySelector("[data-auth-link]");
      if (!link) return;
      const res = await fetch("/admin/auth/me");
      if (!res.ok) return;
      const button = document.createElement("button");
      button.textContent = "Logout";
      button.onclick = async () => {
        await fetch("/admin/auth/logout", { method:"POST", headers:{"content-type":"application/json"}, body:"{}" }).catch(() => {});
        localStorage.removeItem("ledgerflow_email");
        localStorage.removeItem("ledgerflow_api_key");
        location.href = "/login";
      };
      link.replaceWith(button);
    }
    installAuthNav().catch(() => {});
  </script>
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
    p, label, button, input, .status, a, select { font-family:ui-sans-serif,system-ui; }
    p { color:var(--muted); line-height:1.6; }
    label { display:block; font-size:12px; text-transform:uppercase; letter-spacing:.14em; color:var(--muted); margin:18px 0 8px; }
    input, select { width:100%; border:1px solid var(--line); border-radius:16px; padding:14px 15px; background:#fffaf1; color:var(--ink); font-size:15px; }
    button { width:100%; margin-top:16px; border:0; border-radius:999px; padding:14px 18px; background:var(--ink); color:var(--paper); font-weight:760; cursor:pointer; }
    button:disabled { opacity:.55; cursor:not-allowed; }
    button.secondary { background:transparent; color:var(--ink); border:1px solid var(--line); }
    .tabs { display:grid; grid-template-columns:1fr 1fr; gap:8px; margin:8px 0 10px; }
    .tabs button { margin:0; background:transparent; border:1px solid var(--line); color:var(--ink); }
    .tabs button.active { background:var(--ink); color:var(--paper); border-color:var(--ink); }
    .small-link { display:block; margin-top:12px; color:var(--muted); text-align:center; font-size:13px; }
    .status { margin-top:16px; white-space:pre-wrap; color:var(--muted); line-height:1.5; }
    .nav { position:fixed; top:22px; left:24px; right:24px; display:flex; justify-content:space-between; font-family:ui-sans-serif,system-ui; }
    .nav a { color:var(--ink); text-decoration:none; }
    .brand { display:inline-flex; align-items:center; gap:10px; font-weight:850; letter-spacing:.08em; text-transform:uppercase; }
    .brand svg { width:32px; height:32px; }
    @media (max-width:820px) { .shell { grid-template-columns:1fr; padding-top:64px; } }
  </style>
</head>
<body>
  <nav class="nav"><a class="brand" href="/"><svg viewBox="0 0 64 64" aria-hidden="true"><rect x="7" y="7" width="50" height="50" rx="16" fill="#17130f"/><path d="M19 39V18h8v16h18v5H19Z" fill="#fbf7ef"/><path d="M28 46c10-1 17-7 19-18h-7c-2 6-6 10-12 12v6Z" fill="#c56a2d"/></svg><span>LedgerFlow</span></a><a href="/pricing">Pricing</a></nav>
  <main class="shell">
    <section class="panel">
      <h1>Account access for the indexer API.</h1>
      <p>Create an account with email verification and password, then use the dashboard to create API keys and activate a subscription. Verification codes can only be sent once per minute.</p>
    </section>
    <section class="panel">
      <div class="tabs"><button id="login-tab" class="active">Login</button><button id="register-tab">Register</button></div>
      <label for="email">Email</label>
      <input id="email" type="email" placeholder="operator@example.com" autocomplete="email">
      <label for="password">Password</label>
      <input id="password" type="password" placeholder="at least 8 characters" autocomplete="current-password">
      <label id="referral-label" for="referral" style="display:none">Referral code (optional)</label>
      <input id="referral" style="display:none" placeholder="lf...">
      <div id="code-area" style="display:none">
        <button id="send">Send verification code</button>
        <label for="code">Verification code</label>
        <input id="code" placeholder="123456" inputmode="numeric">
      </div>
      <button id="submit">Continue</button>
      <a href="#" id="forgot" class="small-link">Forgot password?</a>
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
    let mode = "login";
    const sendBtn = document.getElementById("send");
    const codeArea = document.getElementById("code-area");
    let cooldownUntil = Number(localStorage.getItem("ledgerflow_code_cooldown_until") || 0);
    function updateMode() {
      const needsCode = mode !== "login";
      codeArea.style.display = needsCode ? "block" : "none";
      document.getElementById("referral").style.display = mode === "register" ? "block" : "none";
      document.getElementById("referral-label").style.display = mode === "register" ? "block" : "none";
      document.getElementById("password").autocomplete = mode === "login" ? "current-password" : "new-password";
      document.getElementById("submit").textContent = mode === "login" ? "Login" : mode === "register" ? "Verify and create account" : "Reset password and login";
      document.getElementById("login-tab").classList.toggle("active", mode === "login");
      document.getElementById("register-tab").classList.toggle("active", mode === "register");
      document.getElementById("forgot").textContent = mode === "reset" ? "Back to login" : "Forgot password?";
    }
    function updateCooldown() {
      const remaining = Math.max(0, Math.ceil((cooldownUntil - Date.now()) / 1000));
      if (remaining > 0) {
        sendBtn.disabled = true;
        sendBtn.textContent = `Code sent. Retry in ${remaining}s`;
      } else {
        sendBtn.disabled = false;
        sendBtn.textContent = "Send verification code";
      }
    }
    setInterval(updateCooldown, 500);
    document.getElementById("login-tab").onclick = () => { mode = "login"; updateMode(); };
    document.getElementById("register-tab").onclick = () => { mode = "register"; updateMode(); };
    document.getElementById("forgot").onclick = (event) => { event.preventDefault(); mode = mode === "reset" ? "login" : "reset"; updateMode(); };
    document.getElementById("send").onclick = async () => {
      try {
        const email = document.getElementById("email").value.trim();
        localStorage.setItem("ledgerflow_email", email);
        const endpoint = mode === "reset" ? "/admin/auth/password-reset/start" : "/admin/auth/register/start";
        const data = await post(endpoint, { email });
        cooldownUntil = Date.now() + Number(data.resend_after_seconds || 60) * 1000;
        localStorage.setItem("ledgerflow_code_cooldown_until", String(cooldownUntil));
        updateCooldown();
        setStatus(data.dev_code_hint ? `Code sent. Dev code: ${data.dev_code_hint}` : "Code sent. Check your email. You can request another code in 60 seconds.");
      } catch (err) { setStatus(err.message); }
    };
    document.getElementById("submit").onclick = async () => {
      try {
        const email = document.getElementById("email").value.trim();
        const password = document.getElementById("password").value;
        const code = document.getElementById("code").value.trim();
        const data = mode === "login"
          ? await post("/admin/auth/login", { email, password })
          : mode === "register"
            ? await post("/admin/auth/register/verify", { email, code, password, referral_code: document.getElementById("referral").value.trim() || null })
            : await post("/admin/auth/password-reset/verify", { email, code, new_password: password });
        localStorage.setItem("ledgerflow_email", data.user.email);
        location.href = "/dashboard";
      } catch (err) { setStatus(err.message); }
    };
    const params = new URLSearchParams(location.search);
    if (params.get("ref")) {
      mode = "register";
      document.getElementById("referral").value = params.get("ref");
    }
    document.getElementById("email").value = localStorage.getItem("ledgerflow_email") || "";
    updateMode();
    updateCooldown();
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
    .brand { font-weight:900; letter-spacing:.08em; text-transform:uppercase; display:inline-flex; align-items:center; gap:10px; }
    .brand svg { width:32px; height:32px; }
    .nav a, button, input, select, textarea, p, label, .mono, .status { font-family:ui-sans-serif,system-ui; }
    .nav button { margin-top:0; padding:0; border:0; background:transparent; color:inherit; font:inherit; cursor:pointer; }
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
    button, .button { border:0; border-radius:999px; background:var(--ink); color:var(--paper); padding:12px 16px; font-weight:760; cursor:pointer; margin-top:14px; display:inline-flex; justify-content:center; }
    button.secondary, .button.secondary { background:transparent; border:1px solid var(--line); color:var(--ink); }
    .status, pre { white-space:pre-wrap; border-radius:18px; padding:14px; background:#18130f; color:#f7ead8; overflow:auto; margin-top:14px; }
    pre { min-height:120px; font-size:12px; line-height:1.6; }
    .metric { font-size:38px; font-weight:800; letter-spacing:-.05em; }
    .metric.small { font-size:28px; }
    .mono { font-family:ui-monospace,SFMono-Regular,Menlo,monospace; font-size:12px; word-break:break-all; }
    .table { width:100%; border-collapse:collapse; font-family:ui-sans-serif,system-ui; }
    .table td, .table th { text-align:left; border-bottom:1px solid var(--line); padding:9px 4px; font-size:13px; }
    @media (max-width:900px) { .card, .card.third { grid-column:span 12; } .hero { display:block; } }
  </style>
</head>
<body>
  <nav class="nav"><a class="brand" href="/"><svg viewBox="0 0 64 64" aria-hidden="true"><rect x="7" y="7" width="50" height="50" rx="16" fill="#17130f"/><path d="M19 39V18h8v16h18v5H19Z" fill="#fbf7ef"/><path d="M28 46c10-1 17-7 19-18h-7c-2 6-6 10-12 12v6Z" fill="#c56a2d"/></svg><span>LedgerFlow</span></a><div><a href="/subscribe">Subscribe</a> &nbsp; <a href="/referrals">Referrals</a> &nbsp; <a href="/docs">Docs</a> &nbsp; <button id="logout">Logout</button></div></nav>
  <main class="wrap">
    <section class="hero">
      <div><h1>Indexer dashboard.</h1><p class="muted">Manage API keys, subscription status, quota, usage, and referral credits.</p></div>
    </section>
    <section class="grid">
      <article class="card third"><h2>Account</h2><label>Email</label><input id="email" type="email" placeholder="operator@example.com"><button id="save-email">Save email</button><p class="muted">Email verification happens on the login page. This dashboard uses your verified email for key and payment operations.</p></article>
      <article class="card third"><h2>Indexer</h2><div class="metric" id="progress">—</div><p class="muted">Last indexed Base block, loaded through your logged-in dashboard session.</p><button id="load-progress">Load progress</button></article>
      <article class="card third"><h2>Quota</h2><div class="metric small" id="quota-remaining">—</div><p class="muted" id="quota-detail">Loading account quota...</p><a class="button" href="/subscribe">Subscribe / renew</a></article>

      <article class="card">
        <h2>Create API key</h2>
        <label>Key name</label><input id="key-name" placeholder="production">
        <button id="create-key">Create key</button>
        <p class="muted">The raw key is only shown once. Store it before refreshing.</p>
        <div class="status" id="key-status">No key created in this browser session.</div>
      </article>

      <article class="card">
        <h2>Referral bonus</h2>
        <p class="muted">Invite verified users. Each successful referral adds 25,000 bonus requests this month, capped at 250,000.</p>
        <label>Your referral code</label><input id="referral-code" readonly placeholder="loading...">
        <button id="copy-referral">Copy signup link</button>
        <a class="button secondary" href="/referrals">Open referral page</a>
        <div class="status" id="referral-status">Referral stats will load after login.</div>
      </article>

      <article class="card wide">
        <h2>API usage this month</h2>
        <p class="muted">Successful and failed authenticated public API calls are tracked separately. Recent failures show the latest error details for debugging.</p>
        <div id="usage-summary">Loading usage...</div>
        <pre id="failure-summary">Loading failures...</pre>
      </article>
    </section>
  </main>
  <script>
    const $ = (id) => document.getElementById(id);
    const email = localStorage.getItem("ledgerflow_email") || "";
    $("email").value = email;

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
    async function loadMe() {
      const data = await getJson("/admin/auth/me");
      $("email").value = data.user.email;
      localStorage.setItem("ledgerflow_email", data.user.email);
    }
    $("save-email").onclick = () => { localStorage.setItem("ledgerflow_email", $("email").value.trim()); };
    $("logout").onclick = async () => {
      try { await postJson("/admin/auth/logout", {}); } catch (_) {}
      localStorage.removeItem("ledgerflow_email");
      localStorage.removeItem("ledgerflow_api_key");
      location.href="/login";
    };

    async function loadConfig() {
      const config = await getJson("/admin/config");
      return config;
    }
    async function loadReferral() {
      const data = await getJson("/admin/referral");
      const r = data.referral;
      $("referral-code").value = r.referral_code;
      $("referral-status").textContent = `${r.bonus_requests_this_month.toLocaleString()} bonus requests this month from ${r.rewarded_referrals_this_month} verified referrals. Monthly cap: ${r.monthly_reward_cap.toLocaleString()}.`;
    }
    $("copy-referral").onclick = async () => {
      const code = $("referral-code").value;
      const url = `${location.origin}/login?ref=${encodeURIComponent(code)}`;
      await navigator.clipboard.writeText(url);
      $("referral-status").textContent = `Copied: ${url}`;
    };

    $("create-key").onclick = async () => {
      try {
        const data = await postJson("/admin/api-keys", { name: $("key-name").value.trim() || "production" });
        const key = data.api_key.api_key;
        localStorage.setItem("ledgerflow_api_key", key);
        $("key-status").textContent = JSON.stringify(data, null, 2);
      } catch (err) { $("key-status").textContent = err.message; }
    };

    $("load-progress").onclick = async () => {
      try {
        const data = await getJson("/admin/progress");
        $("progress").textContent = data.last_indexed_block ?? "null";
      } catch (err) { $("progress").textContent = "blocked"; alert(err.message); }
    };

    async function loadSummary() {
      const data = await getJson("/admin/account/summary");
      const s = data.summary;
      const remaining = s.has_active_subscription ? s.subscription_remaining_this_month : s.bonus_remaining_this_month;
      $("quota-remaining").textContent = remaining.toLocaleString();
      $("quota-detail").textContent = s.has_active_subscription
        ? `Active ${s.plan_id} subscription until ${new Date(s.current_period_end).toLocaleString()}. ${s.subscription_used_this_month.toLocaleString()} / ${s.subscription_quota.toLocaleString()} subscription requests used this month. Bonus credits are preserved for periods without subscription.`
        : `No active subscription. Using bonus credits: ${s.bonus_used_this_month.toLocaleString()} used, ${s.bonus_remaining_this_month.toLocaleString()} remaining. Signup bonus: ${s.signup_bonus_requests.toLocaleString()}; referral bonus this month: ${s.referral_bonus_requests_this_month.toLocaleString()}.`;
    }

    async function loadUsage() {
      const data = await getJson("/admin/usage/summary");
      if (!data.stats.length) {
        $("usage-summary").innerHTML = "<p class='muted'>No authenticated API usage yet.</p>";
      } else {
        $("usage-summary").innerHTML = `<table class="table"><tr><th>Endpoint</th><th>Source</th><th>Outcome</th><th>Count</th></tr>${data.stats.map((s) => `<tr><td>${s.endpoint}</td><td>${s.usage_source}</td><td>${s.outcome}</td><td>${Number(s.request_count).toLocaleString()}</td></tr>`).join("")}</table>`;
      }
      $("failure-summary").textContent = data.recent_failures.length ? JSON.stringify(data.recent_failures, null, 2) : "No recent API failures.";
    }

    loadMe().catch(() => { location.href = "/login"; });
    $("load-progress").click();
    loadSummary().catch((err) => { $("quota-detail").textContent = err.message; });
    loadUsage().catch((err) => { $("failure-summary").textContent = err.message; });
    loadReferral().catch((err) => { $("referral-status").textContent = err.message; });
  </script>
</body>
</html>"##
}

pub fn subscribe() -> &'static str {
    r##"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Subscribe | LedgerFlow Indexer</title>
  <style>
    :root { --ink:#17130f; --paper:#fbf7ef; --line:#dacbb5; --muted:#6f675d; --accent:#b85d24; }
    * { box-sizing:border-box; }
    body { margin:0; color:var(--ink); background:linear-gradient(135deg,#fbf7ef,#efe2cf); font-family:Georgia,"Times New Roman",serif; }
    .nav { display:flex; justify-content:space-between; padding:22px clamp(18px,4vw,54px); font-family:ui-sans-serif,system-ui; }
    .brand { display:inline-flex; align-items:center; gap:10px; font-weight:850; letter-spacing:.08em; text-transform:uppercase; }
    .brand svg { width:32px; height:32px; }
    a { color:inherit; text-decoration:none; }
    main { width:min(1040px,100%); margin:0 auto; padding:38px 22px 80px; }
    h1 { font-size:clamp(44px,7vw,88px); line-height:.9; letter-spacing:-.06em; margin:0 0 24px; }
    h2 { margin:0 0 14px; font-size:28px; }
    p, label, input, select, button, .status, li, .mono { font-family:ui-sans-serif,system-ui; }
    .nav button { border:0; background:transparent; color:inherit; cursor:pointer; font:inherit; padding:0; }
    p, li { color:var(--muted); line-height:1.65; }
    .grid { display:grid; grid-template-columns:1fr 1fr; gap:18px; }
    .card { border:1px solid var(--line); border-radius:28px; background:rgba(255,250,241,.9); padding:24px; }
    label { display:block; font-size:11px; text-transform:uppercase; letter-spacing:.14em; color:var(--muted); margin:16px 0 7px; }
    input, select { width:100%; border:1px solid var(--line); border-radius:14px; background:#fffaf1; color:var(--ink); padding:12px 13px; font-size:14px; }
    button { border:0; border-radius:999px; background:var(--ink); color:var(--paper); padding:12px 16px; font-weight:760; cursor:pointer; margin-top:16px; }
    .status { white-space:pre-wrap; border-radius:18px; padding:14px; background:#18130f; color:#f7ead8; overflow:auto; margin-top:16px; }
    .price { font-size:64px; line-height:.9; letter-spacing:-.06em; margin:12px 0; }
    .mono { word-break:break-all; font-size:13px; }
    @media (max-width:850px) { .grid { grid-template-columns:1fr; } }
  </style>
</head>
<body>
  <nav class="nav"><a class="brand" href="/"><svg viewBox="0 0 64 64" aria-hidden="true"><rect x="7" y="7" width="50" height="50" rx="16" fill="#17130f"/><path d="M19 39V18h8v16h18v5H19Z" fill="#fbf7ef"/><path d="M28 46c10-1 17-7 19-18h-7c-2 6-6 10-12 12v6Z" fill="#c56a2d"/></svg><span>LedgerFlow</span></a><div><a href="/dashboard">Dashboard</a> &nbsp; <a href="/docs">Docs</a> &nbsp; <button id="logout">Logout</button></div></nav>
  <main>
    <h1>Subscribe with Base ETH.</h1>
    <section class="grid">
      <article class="card">
        <h2>Launch plan</h2>
        <div class="price">0.009 ETH</div>
        <p>30 days of access with 1,000,000 metered API requests. When a subscription is active, requests consume subscription quota first. Bonus credits are only used when no active subscription exists.</p>
        <ol>
          <li>Send exactly or at least the plan amount on Base to the treasury address.</li>
          <li>Wait for the required confirmations.</li>
          <li>Paste the Base transaction hash below and verify payment.</li>
        </ol>
      </article>
      <article class="card">
        <h2>Verify payment</h2>
        <label>Email</label><input id="email" type="email" placeholder="operator@example.com">
        <label>Plan</label><select id="plan"></select>
        <label>Treasury address</label><div class="mono" id="treasury">loading...</div>
        <p>Required confirmations: <span id="confirmations">—</span></p>
        <label>Payment tx hash</label><input id="payment-tx" placeholder="0x...">
        <label>From address (optional but recommended)</label><input id="payment-from" placeholder="0x...">
        <button id="submit-payment">Verify payment</button>
        <div class="status" id="payment-status">If payment verification fails unexpectedly, email peter@ledgerflow.top. We will handle it within 24 hours.</div>
      </article>
    </section>
  </main>
  <script>
    const $ = (id) => document.getElementById(id);
    async function getJson(url) {
      const res = await fetch(url);
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
    $("logout").onclick = async () => {
      try { await postJson("/admin/auth/logout", {}); } catch (_) {}
      localStorage.removeItem("ledgerflow_email");
      localStorage.removeItem("ledgerflow_api_key");
      location.href = "/login";
    };
    async function load() {
      const me = await getJson("/admin/auth/me");
      $("email").value = me.user.email;
      const config = await getJson("/admin/config");
      $("treasury").textContent = config.billing.treasury_address;
      $("confirmations").textContent = config.billing.required_confirmations;
      const plans = await getJson("/admin/billing/plans");
      $("plan").innerHTML = plans.plans.map((p) => `<option value="${p.plan_id}">${p.name} · ${Number(BigInt(p.monthly_price_wei)) / 1e18} ETH / ${p.monthly_request_quota.toLocaleString()} req</option>`).join("");
    }
    $("submit-payment").onclick = async () => {
      try {
        const payload = { plan_id: $("plan").value, tx_hash: $("payment-tx").value.trim(), from_address: $("payment-from").value.trim() || null };
        const data = await postJson("/admin/billing/base-eth/payments", payload);
        $("payment-status").textContent = JSON.stringify(data, null, 2);
      } catch (err) {
        $("payment-status").textContent = `${err.message}\n\nIf this looks wrong, email peter@ledgerflow.top. We will handle it within 24 hours.`;
      }
    };
    load().catch(() => { location.href = "/login"; });
  </script>
</body>
</html>"##
}

pub fn referrals() -> &'static str {
    r##"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Referrals | LedgerFlow Indexer</title>
  <style>
    :root { --ink:#17130f; --paper:#fbf7ef; --line:#dacbb5; --muted:#6f675d; }
    * { box-sizing:border-box; }
    body { margin:0; color:var(--ink); background:radial-gradient(circle at 80% 10%,rgba(97,119,81,.18),transparent 24rem),#fbf7ef; font-family:Georgia,"Times New Roman",serif; }
    .nav { display:flex; justify-content:space-between; padding:22px clamp(18px,4vw,54px); font-family:ui-sans-serif,system-ui; }
    .brand { display:inline-flex; align-items:center; gap:10px; font-weight:850; letter-spacing:.08em; text-transform:uppercase; }
    .brand svg { width:32px; height:32px; }
    a { color:inherit; text-decoration:none; }
    main { width:min(920px,100%); margin:0 auto; padding:48px 22px 80px; }
    h1 { font-size:clamp(44px,7vw,88px); line-height:.9; letter-spacing:-.06em; margin:0 0 24px; }
    .card { border:1px solid var(--line); border-radius:28px; background:#fffaf1; padding:24px; }
    p, label, input, button, .status { font-family:ui-sans-serif,system-ui; }
    .nav button { border:0; background:transparent; color:inherit; cursor:pointer; font:inherit; padding:0; }
    p { color:var(--muted); line-height:1.65; }
    label { display:block; font-size:11px; text-transform:uppercase; letter-spacing:.14em; color:var(--muted); margin:16px 0 7px; }
    input { width:100%; border:1px solid var(--line); border-radius:14px; background:#fbf7ef; color:var(--ink); padding:12px 13px; font-size:14px; }
    button { border:0; border-radius:999px; background:var(--ink); color:var(--paper); padding:12px 16px; font-weight:760; cursor:pointer; margin-top:16px; }
    .status { white-space:pre-wrap; border-radius:18px; padding:14px; background:#18130f; color:#f7ead8; overflow:auto; margin-top:16px; }
  </style>
</head>
<body>
  <nav class="nav"><a class="brand" href="/"><svg viewBox="0 0 64 64" aria-hidden="true"><rect x="7" y="7" width="50" height="50" rx="16" fill="#17130f"/><path d="M19 39V18h8v16h18v5H19Z" fill="#fbf7ef"/><path d="M28 46c10-1 17-7 19-18h-7c-2 6-6 10-12 12v6Z" fill="#c56a2d"/></svg><span>LedgerFlow</span></a><div><a href="/dashboard">Dashboard</a> &nbsp; <a href="/subscribe">Subscribe</a> &nbsp; <button id="logout">Logout</button></div></nav>
  <main>
    <h1>Referral credits.</h1>
    <section class="card">
      <p>Each verified referral grants 25,000 bonus requests this month, capped at 250,000. New accounts also receive 1,000 signup bonus requests. Bonus credits are consumed only when you do not have an active subscription.</p>
      <label>Your referral code</label><input id="referral-code" readonly>
      <label>Your signup link</label><input id="referral-link" readonly>
      <button id="copy-referral">Copy signup link</button>
      <div class="status" id="referral-status">Loading referral stats...</div>
    </section>
  </main>
  <script>
    const $ = (id) => document.getElementById(id);
    async function getJson(url) {
      const res = await fetch(url);
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
    $("logout").onclick = async () => {
      try { await postJson("/admin/auth/logout", {}); } catch (_) {}
      localStorage.removeItem("ledgerflow_email");
      localStorage.removeItem("ledgerflow_api_key");
      location.href = "/login";
    };
    async function loadReferral() {
      const data = await getJson("/admin/referral");
      const r = data.referral;
      const url = `${location.origin}/login?ref=${encodeURIComponent(r.referral_code)}`;
      $("referral-code").value = r.referral_code;
      $("referral-link").value = url;
      $("referral-status").textContent = `${r.bonus_requests_this_month.toLocaleString()} bonus requests this month from ${r.rewarded_referrals_this_month} verified referrals. Monthly cap: ${r.monthly_reward_cap.toLocaleString()}.`;
    }
    $("copy-referral").onclick = async () => {
      await navigator.clipboard.writeText($("referral-link").value);
      $("referral-status").textContent += "\nCopied signup link.";
    };
    loadReferral().catch(() => { location.href = "/login"; });
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
    .brand { display:inline-flex; align-items:center; gap:10px; font-weight:850; letter-spacing:.08em; text-transform:uppercase; }
    .brand svg { width:32px; height:32px; }
    .nav a, .nav button, p, li, code, pre, th, td { font-family:ui-sans-serif,system-ui; }
    .nav button { border:0; background:transparent; color:inherit; cursor:pointer; padding:0; }
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
  <nav class="nav"><a class="brand" href="/"><svg viewBox="0 0 64 64" aria-hidden="true"><rect x="7" y="7" width="50" height="50" rx="16" fill="#17130f"/><path d="M19 39V18h8v16h18v5H19Z" fill="#fbf7ef"/><path d="M28 46c10-1 17-7 19-18h-7c-2 6-6 10-12 12v6Z" fill="#c56a2d"/></svg><span>LedgerFlow</span></a><div><a href="/login" data-auth-link>Login</a> &nbsp; <a href="/dashboard">Dashboard</a></div></nav>
  <main>
    <h1>LedgerFlow Indexer API docs.</h1>
    <p>LedgerFlow Indexer is a Base top-level address reference API. It answers which transaction hashes touched an address as top-level <code>from</code> or <code>to</code>, and is designed as a fast discovery layer for bots, wallet operations, and transaction analysis systems.</p>

    <h2>Authentication</h2>
    <pre>Authorization: Bearer lf_live_xxx</pre>
    <p>Public <code>/v1</code> endpoints require an API key. Requests use active subscription quota first; if there is no active subscription, signup/referral bonus credits are used.</p>

    <h2>Common response and errors</h2>
    <p>Address refs are normalized top-level <code>from</code>/<code>to</code> rows. Errors use a compact JSON shape:</p>
    <pre>{
  "error": "missing Authorization header"
}</pre>
    <p>Common status codes: <code>400</code> malformed input, <code>401</code> invalid auth, <code>402</code> quota or subscription required, <code>409</code> product rule conflict, <code>500</code> storage or upstream RPC failure.</p>

    <h2>Endpoint summary</h2>
    <table>
      <tr><th>Method</th><th>Path</th><th>Params</th><th>Returns</th></tr>
      <tr><td>GET</td><td><code>/health</code></td><td>none</td><td><code>{"status":"ok","service":"ledgerflow-indexer"}</code></td></tr>
      <tr><td>GET</td><td><code>/v1/chains/base/progress</code></td><td>none</td><td><code>chain</code>, <code>chain_id</code>, <code>worker_id</code>, <code>last_indexed_block</code></td></tr>
      <tr><td>GET</td><td><code>/v1/base/address/{address}/txs</code></td><td><code>address</code>, <code>from_block</code>, <code>to_block</code>, <code>direction</code>, <code>limit</code>, <code>cursor</code></td><td><code>items</code> as address refs plus <code>next_cursor</code></td></tr>
      <tr><td>GET</td><td><code>/v1/base/address/{address}/tx-hashes</code></td><td><code>address</code>, <code>from_block</code>, <code>to_block</code>, <code>direction</code>, <code>limit</code></td><td>deduplicated <code>tx_hashes</code></td></tr>
      <tr><td>POST</td><td><code>/v1/base/addresses/txs</code></td><td>JSON <code>addresses</code>, <code>from_block</code>, <code>to_block</code>, <code>direction</code>, <code>limit</code></td><td>combined <code>items</code> and <code>per_address_counts</code></td></tr>
      <tr><td>GET</td><td><code>/v1/base/block/latest</code></td><td>none</td><td><code>block_number</code></td></tr>
      <tr><td>GET</td><td><code>/v1/base/block/{block_number}</code></td><td>decimal <code>block_number</code></td><td>raw RPC <code>block</code> with full transactions</td></tr>
      <tr><td>GET</td><td><code>/v1/base/tx/{tx_hash}</code></td><td><code>tx_hash</code></td><td>raw RPC <code>transaction</code></td></tr>
      <tr><td>GET</td><td><code>/v1/base/tx/{tx_hash}/receipt</code></td><td><code>tx_hash</code></td><td>raw RPC <code>receipt</code></td></tr>
    </table>

    <h2>Address transaction refs</h2>
    <pre>curl "https://indexer.ledgerflow.top/v1/base/address/0xabc.../txs?direction=from&limit=100&cursor=..." \
  -H "Authorization: Bearer lf_live_xxx"</pre>
    <p>Path parameter: <code>address</code> must be a 20-byte <code>0x</code> EVM address. Query parameters: <code>from_block</code> and <code>to_block</code> are inclusive bounds; <code>direction</code> is <code>from</code>, <code>to</code>, or omitted; <code>limit</code> is clamped by server config; <code>cursor</code> is the previous <code>next_cursor</code>.</p>
    <pre>{
  "chain": "base",
  "chain_id": 8453,
  "address": "0x1111111111111111111111111111111111111111",
  "limit": 100,
  "next_cursor": "31050099:14:1",
  "items": [
    {
      "chain_id": 8453,
      "address": "0x1111111111111111111111111111111111111111",
      "tx_hash": "0x...",
      "block_number": 31050099,
      "tx_index": 14,
      "direction": "from"
    }
  ]
}</pre>

    <h2>Address tx hashes</h2>
    <pre>curl "https://indexer.ledgerflow.top/v1/base/address/0xabc.../tx-hashes?limit=1000" \
  -H "Authorization: Bearer lf_live_xxx"</pre>
    <p>Parameters match <code>/txs</code> except this endpoint does not take <code>cursor</code>. It returns deduplicated hashes only.</p>
    <pre>{
  "chain": "base",
  "chain_id": 8453,
  "address": "0x1111111111111111111111111111111111111111",
  "limit": 1000,
  "tx_hashes": ["0x..."]
}</pre>

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
    <p>Body field <code>addresses</code> is required and accepts at most 50 addresses. <code>limit</code> is per address, not total response size. Batch does not expose cursor pagination.</p>
    <pre>{
  "chain": "base",
  "chain_id": 8453,
  "items": [
    {
      "chain_id": 8453,
      "address": "0x1111111111111111111111111111111111111111",
      "tx_hash": "0x...",
      "block_number": 31050099,
      "tx_index": 14,
      "direction": "from"
    }
  ],
  "per_address_counts": {
    "0x1111111111111111111111111111111111111111": 1
  }
}</pre>

    <h2>Base RPC convenience endpoints</h2>
    <pre>curl "https://indexer.ledgerflow.top/v1/base/block/latest" \
  -H "Authorization: Bearer lf_live_xxx"

curl "https://indexer.ledgerflow.top/v1/base/block/31050099" \
  -H "Authorization: Bearer lf_live_xxx"

curl "https://indexer.ledgerflow.top/v1/base/tx/0x..." \
  -H "Authorization: Bearer lf_live_xxx"

curl "https://indexer.ledgerflow.top/v1/base/tx/0x.../receipt" \
  -H "Authorization: Bearer lf_live_xxx"</pre>
    <p>These proxy narrow JSON-RPC reads from the configured Base reth node. The block endpoint returns raw <code>eth_getBlockByNumber(..., true)</code>; tx and receipt endpoints return raw <code>eth_getTransactionByHash</code> and <code>eth_getTransactionReceipt</code> results.</p>

    <h2>Detailed parameter rules</h2>
    <ul>
      <li><code>from_block</code>: inclusive lower block bound.</li>
      <li><code>to_block</code>: inclusive upper block bound.</li>
      <li><code>direction</code>: <code>from</code>, <code>to</code>, or omitted/<code>any</code>.</li>
      <li><code>limit</code>: defaults to 1000 and is bounded by server-side max limit, currently 10000 by default config.</li>
      <li><code>cursor</code>: pagination token returned as <code>next_cursor</code> by <code>/txs</code>; treat it as opaque.</li>
    </ul>

    <h2>Request limits</h2>
    <ul>
      <li>Batch lookup accepts at most 50 addresses per request.</li>
      <li>Single address lookup is the correct interface for very high-volume addresses because it supports cursor pagination.</li>
      <li>Successful and failed authenticated requests are both visible in the dashboard usage page.</li>
    </ul>

    <h2>What this is not</h2>
    <p>This is not a full explorer clone and does not infer protocol semantics. Use it to discover transaction candidates, then run deeper evidence-first analysis elsewhere.</p>
	  </main>
  <script>
    async function installAuthNav() {
      const link = document.querySelector("[data-auth-link]");
      if (!link) return;
      const res = await fetch("/admin/auth/me");
      if (!res.ok) return;
      const button = document.createElement("button");
      button.textContent = "Logout";
      button.onclick = async () => {
        await fetch("/admin/auth/logout", { method:"POST", headers:{"content-type":"application/json"}, body:"{}" }).catch(() => {});
        localStorage.removeItem("ledgerflow_email");
        localStorage.removeItem("ledgerflow_api_key");
        location.href = "/login";
      };
      link.replaceWith(button);
    }
    installAuthNav().catch(() => {});
  </script>
</body>
</html>"##
}
