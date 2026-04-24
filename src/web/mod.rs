use axum::{
    Json, Router,
    body::Body,
    extract::{Path, Query, State},
    http::{Response, StatusCode, header},
    response::{Html, IntoResponse},
    routing::{get, post},
};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Clone)]
pub struct WebState {
    pub api_base_url: String,
    pub bind_addr: String,
    pub default_chain_id: u64,
    pub client: Client,
}

#[derive(Debug, Clone)]
pub struct WebConfig {
    pub bind_addr: String,
    pub api_base_url: String,
    pub default_chain_id: u64,
}

impl WebConfig {
    pub fn from_env() -> Self {
        Self {
            bind_addr: std::env::var("LEDGERFLOW_WEB_BIND_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:3001".to_owned()),
            api_base_url: std::env::var("LEDGERFLOW_API_BASE_URL")
                .unwrap_or_else(|_| "http://127.0.0.1:3000".to_owned()),
            default_chain_id: std::env::var("LEDGERFLOW_DEFAULT_CHAIN_ID")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(8453),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct AnalyzeTxRequest {
    pub chain_id: u64,
    pub tx_hash: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ReplayTxRequest {
    pub chain_id: u64,
    pub tx_hash: String,
    pub ruleset_id: Option<String>,
    pub recognizer_ids: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ClusterAddressInput {
    pub address: String,
    pub label: Option<String>,
    pub role: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CreateClusterRequest {
    pub chain_id: u64,
    pub owner_label: String,
    pub name: String,
    pub description: Option<String>,
    pub addresses: Vec<ClusterAddressInput>,
}

#[derive(Debug, Deserialize)]
pub struct AnalysisQuery {
    pub chain_id: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct ClusterSummaryQuery {
    pub start: Option<String>,
    pub end: Option<String>,
}

pub fn router(config: WebConfig) -> Router {
    let state = WebState {
        api_base_url: config.api_base_url,
        bind_addr: config.bind_addr,
        default_chain_id: config.default_chain_id,
        client: Client::new(),
    };

    Router::new()
        .route("/", get(index))
        .route("/health", get(health))
        .route("/api/recognizers", get(proxy_recognizers))
        .route("/api/analyses/tx", post(proxy_analyze_tx))
        .route("/api/analyses/{tx_hash}", get(proxy_get_analysis))
        .route(
            "/api/evidence-packs/{pack_id}",
            get(proxy_get_evidence_pack),
        )
        .route("/api/replay/tx", post(proxy_replay_tx))
        .route("/api/clusters", post(proxy_create_cluster))
        .route(
            "/api/clusters/{cluster_id}/coverage",
            get(proxy_get_cluster_coverage),
        )
        .route(
            "/api/clusters/{cluster_id}/backfill-runs",
            get(proxy_list_cluster_backfill_runs),
        )
        .route(
            "/api/clusters/{cluster_id}/spend-summary",
            get(proxy_get_cluster_spend_summary),
        )
        .with_state(state)
}

async fn index(State(state): State<WebState>) -> Html<String> {
    Html(render_index_html(&state))
}

async fn health(State(state): State<WebState>) -> Json<serde_json::Value> {
    Json(json!({
        "status": "ok",
        "web_bind_addr": state.bind_addr,
        "api_base_url": state.api_base_url,
        "default_chain_id": state.default_chain_id,
    }))
}

async fn proxy_recognizers(State(state): State<WebState>) -> Result<Response<Body>, WebError> {
    proxy_request(
        state
            .client
            .get(format!("{}/recognizers", state.api_base_url)),
    )
    .await
}

async fn proxy_analyze_tx(
    State(state): State<WebState>,
    Json(request): Json<AnalyzeTxRequest>,
) -> Result<Response<Body>, WebError> {
    proxy_request(
        state
            .client
            .post(format!("{}/analyses/tx", state.api_base_url))
            .json(&request),
    )
    .await
}

async fn proxy_get_analysis(
    State(state): State<WebState>,
    Path(tx_hash): Path<String>,
    Query(query): Query<AnalysisQuery>,
) -> Result<Response<Body>, WebError> {
    let chain_id = query.chain_id.unwrap_or(state.default_chain_id);
    proxy_request(
        state
            .client
            .get(format!("{}/analyses/{}", state.api_base_url, tx_hash))
            .query(&[("chain_id", chain_id)]),
    )
    .await
}

async fn proxy_get_evidence_pack(
    State(state): State<WebState>,
    Path(pack_id): Path<String>,
) -> Result<Response<Body>, WebError> {
    proxy_request(
        state
            .client
            .get(format!("{}/evidence-packs/{}", state.api_base_url, pack_id)),
    )
    .await
}

async fn proxy_replay_tx(
    State(state): State<WebState>,
    Json(request): Json<ReplayTxRequest>,
) -> Result<Response<Body>, WebError> {
    proxy_request(
        state
            .client
            .post(format!("{}/replay/tx", state.api_base_url))
            .json(&request),
    )
    .await
}

async fn proxy_create_cluster(
    State(state): State<WebState>,
    Json(request): Json<CreateClusterRequest>,
) -> Result<Response<Body>, WebError> {
    proxy_request(
        state
            .client
            .post(format!("{}/clusters", state.api_base_url))
            .json(&request),
    )
    .await
}

async fn proxy_get_cluster_coverage(
    State(state): State<WebState>,
    Path(cluster_id): Path<String>,
) -> Result<Response<Body>, WebError> {
    proxy_request(state.client.get(format!(
        "{}/clusters/{}/coverage",
        state.api_base_url, cluster_id
    )))
    .await
}

async fn proxy_list_cluster_backfill_runs(
    State(state): State<WebState>,
    Path(cluster_id): Path<String>,
) -> Result<Response<Body>, WebError> {
    proxy_request(state.client.get(format!(
        "{}/clusters/{}/backfill-runs",
        state.api_base_url, cluster_id
    )))
    .await
}

async fn proxy_get_cluster_spend_summary(
    State(state): State<WebState>,
    Path(cluster_id): Path<String>,
    Query(query): Query<ClusterSummaryQuery>,
) -> Result<Response<Body>, WebError> {
    let mut request = state.client.get(format!(
        "{}/clusters/{}/spend-summary",
        state.api_base_url, cluster_id
    ));
    if let Some(start) = query.start.as_deref() {
        request = request.query(&[("start", start)]);
    }
    if let Some(end) = query.end.as_deref() {
        request = request.query(&[("end", end)]);
    }
    proxy_request(request).await
}

async fn proxy_request(builder: reqwest::RequestBuilder) -> Result<Response<Body>, WebError> {
    let upstream = builder.send().await.map_err(WebError::upstream)?;
    let status = upstream.status();
    let content_type = upstream
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/json")
        .to_owned();
    let body = upstream.text().await.map_err(WebError::upstream)?;

    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, content_type)
        .body(Body::from(body))
        .map_err(|err| WebError::internal(err.to_string()))
}

pub struct WebError {
    status: StatusCode,
    message: String,
}

impl WebError {
    fn upstream(error: reqwest::Error) -> Self {
        Self {
            status: StatusCode::BAD_GATEWAY,
            message: format!("web proxy upstream error: {error}"),
        }
    }

    fn internal(message: String) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message,
        }
    }
}

impl IntoResponse for WebError {
    fn into_response(self) -> axum::response::Response {
        (
            self.status,
            Json(json!({
                "error": self.message,
            })),
        )
            .into_response()
    }
}

fn render_index_html(state: &WebState) -> String {
    format!(
        r#"<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>LedgerFlow Cluster Console</title>
    <style>
      :root {{
        color-scheme: light;
        --bg: #f4f1e8;
        --panel: #fffdf8;
        --ink: #16130f;
        --muted: #665f57;
        --line: #d7cdbf;
        --accent: #0e6d63;
        --accent-2: #bf5b04;
        --shadow: 0 14px 40px rgba(26, 19, 11, 0.08);
      }}
      * {{ box-sizing: border-box; }}
      body {{
        margin: 0;
        font-family: "Iowan Old Style", "Palatino Linotype", "Book Antiqua", Georgia, serif;
        color: var(--ink);
        background:
          radial-gradient(circle at top left, rgba(14, 109, 99, 0.14), transparent 28rem),
          radial-gradient(circle at top right, rgba(191, 91, 4, 0.12), transparent 22rem),
          linear-gradient(180deg, #f7f3eb 0%, var(--bg) 100%);
      }}
      .shell {{ max-width: 1280px; margin: 0 auto; padding: 32px 20px 48px; }}
      .hero {{ display: grid; gap: 16px; margin-bottom: 24px; }}
      .eyebrow {{ letter-spacing: 0.12em; text-transform: uppercase; font-size: 12px; color: var(--accent); font-weight: 700; }}
      h1 {{ margin: 0; font-size: clamp(2.4rem, 6vw, 4.5rem); line-height: 0.96; }}
      .subtitle {{ max-width: 900px; color: var(--muted); font-size: 1.02rem; line-height: 1.55; }}
      .grid {{ display: grid; grid-template-columns: 420px minmax(0, 1fr); gap: 20px; align-items: start; }}
      .stack {{ display: grid; gap: 20px; }}
      .panel {{ background: color-mix(in srgb, var(--panel) 92%, white 8%); border: 1px solid var(--line); border-radius: 18px; box-shadow: var(--shadow); overflow: hidden; }}
      .panel-head {{ display: flex; justify-content: space-between; align-items: center; gap: 12px; padding: 18px 20px 12px; }}
      .panel-head h2 {{ margin: 0; font-size: 1.1rem; }}
      .panel-body {{ padding: 0 20px 20px; }}
      .panel-copy {{ color: var(--muted); font-size: 0.95rem; line-height: 1.5; margin-bottom: 14px; }}
      .meta {{ display: grid; gap: 8px; color: var(--muted); font-size: 0.92rem; margin-top: 14px; }}
      form {{ display: grid; gap: 12px; }}
      label {{ display: grid; gap: 6px; font-size: 0.95rem; }}
      input, textarea {{
        width: 100%;
        padding: 12px 14px;
        border-radius: 12px;
        border: 1px solid var(--line);
        background: rgba(255, 255, 255, 0.9);
        font: inherit;
        color: var(--ink);
      }}
      textarea {{ min-height: 96px; resize: vertical; }}
      .actions {{ display: flex; flex-wrap: wrap; gap: 10px; margin-top: 4px; }}
      button {{
        appearance: none;
        border: 0;
        border-radius: 999px;
        padding: 11px 16px;
        font: inherit;
        font-weight: 700;
        cursor: pointer;
        transition: transform 120ms ease;
      }}
      button:hover {{ transform: translateY(-1px); }}
      button.primary {{ background: var(--accent); color: white; }}
      button.secondary {{ background: #efe6d7; color: var(--ink); }}
      .pill {{ display: inline-flex; align-items: center; gap: 8px; padding: 6px 10px; border-radius: 999px; background: #efe6d7; color: var(--muted); font-size: 0.82rem; }}
      .status {{ min-height: 24px; margin-top: 10px; color: var(--accent-2); font-size: 0.95rem; }}
      .result-shell {{ display: grid; gap: 16px; }}
      .summary {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 12px; }}
      .stat {{ border: 1px solid var(--line); border-radius: 14px; padding: 14px; background: rgba(255, 255, 255, 0.72); }}
      .stat-label {{ display: block; font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.1em; color: var(--muted); }}
      .stat-value {{ display: block; margin-top: 6px; font-size: 1.08rem; font-weight: 700; word-break: break-word; }}
      .code {{
        margin: 0;
        padding: 18px;
        border-radius: 16px;
        background: #14110d;
        color: #f9f5ed;
        overflow: auto;
        font-family: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace;
        font-size: 13px;
        line-height: 1.5;
      }}
      .chips {{ display: flex; flex-wrap: wrap; gap: 8px; }}
      .chip {{ padding: 8px 10px; border-radius: 999px; background: #f2eadf; border: 1px solid var(--line); font-size: 0.88rem; }}
      @media (max-width: 960px) {{
        .grid {{ grid-template-columns: 1fr; }}
      }}
    </style>
  </head>
  <body>
    <main class="shell">
      <section class="hero">
        <div class="eyebrow">LedgerFlow Web</div>
        <h1>Cluster spend analysis first. Single transaction inspection second.</h1>
        <div class="subtitle">
          This console is now aimed at money-ops style address-cluster review: create a cluster for your bot and EOA addresses,
          then summarize analyzed spend over a time window. Transaction-level analysis is still here, but it is the debug layer under the cluster view.
        </div>
      </section>

      <section class="grid">
        <aside class="stack">
          <section class="panel">
            <div class="panel-head">
              <h2>Create Cluster</h2>
              <span class="pill">Chain default: {default_chain_id}</span>
            </div>
            <div class="panel-body">
              <div class="panel-copy">Create a cluster for one operator, bot set, or wallet family. Addresses should be newline-separated. Optional syntax: <code>0xabc,label,role</code>.</div>
              <form id="cluster-form">
                <label>
                  Chain ID
                  <input id="cluster-chain-id" value="{default_chain_id}" inputmode="numeric">
                </label>
                <label>
                  Owner Label
                  <input id="cluster-owner" placeholder="team_alpha">
                </label>
                <label>
                  Cluster Name
                  <input id="cluster-name" placeholder="searcher_primary_set">
                </label>
                <label>
                  Description
                  <input id="cluster-description" placeholder="main bot + treasury EOAs">
                </label>
                <label>
                  Addresses
                  <textarea id="cluster-addresses" placeholder="0x111...,bot-1,bot&#10;0x222...,treasury,eoa"></textarea>
                </label>
                <div class="actions">
                  <button class="primary" type="submit">Create Cluster</button>
                </div>
              </form>
            </div>
          </section>

          <section class="panel">
            <div class="panel-head">
              <h2>Cluster Summary</h2>
              <span class="pill">Spend window</span>
            </div>
            <div class="panel-body">
              <div class="panel-copy">Spend summary only aggregates already analyzed transactions. New clusters are queued for historical backfill immediately after creation, then a separate worker process analyzes them in the background.</div>
              <div class="panel-copy">If txs are still empty, load coverage first. That tells you whether discovery has started, how many txs were found, and whether the worker is still catching up.</div>
              <form id="cluster-summary-form">
                <label>
                  Cluster ID
                  <input id="cluster-summary-id" placeholder="uuid">
                </label>
                <label>
                  Start Time (RFC3339)
                  <input id="cluster-start" placeholder="2026-04-01T00:00:00Z">
                </label>
                <label>
                  End Time (RFC3339)
                  <input id="cluster-end" placeholder="2026-04-30T23:59:59Z">
                </label>
                <div class="actions">
                  <button class="secondary" type="button" id="load-cluster-coverage">Load Coverage</button>
                  <button class="primary" type="submit">Load Spend Summary</button>
                </div>
              </form>
            </div>
          </section>

          <section class="panel">
            <div class="panel-head">
              <h2>Transaction Debug</h2>
              <span class="pill">Still useful</span>
            </div>
            <div class="panel-body">
              <div class="panel-copy">Use this when you need to inspect or replay a specific transaction before it contributes to a cluster summary.</div>
              <form id="analysis-form">
                <label>
                  Chain ID
                  <input id="chain-id" value="{default_chain_id}" inputmode="numeric">
                </label>
                <label>
                  Transaction Hash
                  <input id="tx-hash" placeholder="0x..." autocomplete="off">
                </label>
                <label>
                  Replay Recognizers
                  <textarea id="recognizer-ids" placeholder="selector_presence,native_transfer"></textarea>
                </label>
                <div class="actions">
                  <button class="secondary" type="submit">Analyze</button>
                  <button class="secondary" type="button" id="load-existing">Load Saved</button>
                  <button class="secondary" type="button" id="replay-run">Replay</button>
                  <button class="secondary" type="button" id="list-recognizers">Recognizers</button>
                </div>
              </form>
              <div id="status" class="status"></div>
              <div class="meta">
                <div>Web binds at <strong>{bind_addr}</strong></div>
                <div>Proxies API at <strong>{api_base_url}</strong></div>
              </div>
            </div>
          </section>
        </aside>

        <section class="panel">
          <div class="panel-head">
            <h2>Result</h2>
            <span class="pill" id="result-mode">idle</span>
          </div>
          <div class="panel-body result-shell">
            <div class="summary" id="summary"></div>
            <div class="chips" id="labels"></div>
            <pre class="code" id="output">No result yet.</pre>
          </div>
        </section>
      </section>
    </main>

    <script>
      const statusEl = document.getElementById("status");
      const outputEl = document.getElementById("output");
      const summaryEl = document.getElementById("summary");
      const labelsEl = document.getElementById("labels");
      const resultModeEl = document.getElementById("result-mode");

      const setStatus = (message) => {{
        statusEl.textContent = message;
      }};

      const setMode = (mode) => {{
        resultModeEl.textContent = mode;
      }};

      const renderTransactionSummary = (payload) => {{
        const analysis = payload.analysis || payload;
        const recognizerResults = analysis.recognizer_results || [];
        const flows = analysis.derived_flows || [];
        const validations = analysis.validation_results || [];
        const unresolved = analysis.unresolved_items || [];
        const gas = analysis.accounting_outputs?.gas_cost_raw || analysis.normalized_execution_facts?.derived_facts?.gas_cost_raw || "0";
        const pack = payload.evidence_pack;
        const cards = [
          ["status", analysis.analysis_status || "unknown"],
          ["recognizers", String(recognizerResults.length)],
          ["flows", String(flows.length)],
          ["gas", String(gas)],
          ["validations", String(validations.length)],
          ["pack", pack ? pack.pack_id : "-"],
          ["unresolved", String(unresolved.length)],
        ];
        summaryEl.innerHTML = cards.map(([label, value]) => `
          <div class="stat">
            <span class="stat-label">${{label}}</span>
            <span class="stat-value">${{value}}</span>
          </div>
        `).join("");
        labelsEl.innerHTML = recognizerResults
          .map((item) => `<div class="chip">${{item.label}} · conf=${{Number(item.confidence).toFixed(2)}}</div>`)
          .join("");
      }};

      const renderClusterSummary = (payload) => {{
        const breakdowns = payload.breakdowns || [];
        const coverage = payload.metadata?.coverage || null;
        const cards = [
          ["cluster", payload.cluster_name],
          ["owner", payload.owner_label],
          ["addresses", String(payload.address_count || 0)],
          ["tx", String(payload.analyzed_tx_count || 0)],
          ["gas", String(payload.total_gas_cost_raw || "0")],
          ["ext native", String(payload.total_external_native_outflow_raw || "0")],
          ["internal", String(payload.total_internal_native_transfer_raw || "0")],
          ["incoming", String(payload.total_incoming_native_raw || "0")],
        ];
        if (coverage) {{
          cards.push(["discovered", String(coverage.discovered_tx_count || 0)]);
          cards.push(["failed", String(coverage.failed_tx_count || 0)]);
          cards.push(["pending", String(coverage.pending_tx_count || 0)]);
        }}
        summaryEl.innerHTML = cards.map(([label, value]) => `
          <div class="stat">
            <span class="stat-label">${{label}}</span>
            <span class="stat-value">${{value}}</span>
          </div>
        `).join("");
        labelsEl.innerHTML = breakdowns
          .slice(0, 12)
          .map((item) => `<div class="chip">${{item.category}} · ${{item.asset_type}} · ${{item.amount_raw}}</div>`)
          .join("");
      }};

      const renderClusterCoverage = (payload) => {{
        const cards = [
          ["cluster", payload.cluster_id || "-"],
          ["discovered", String(payload.discovered_tx_count || 0)],
          ["analyzed", String(payload.analyzed_tx_count || 0)],
          ["failed", String(payload.failed_tx_count || 0)],
          ["pending", String(payload.pending_tx_count || 0)],
          ["latest run", payload.latest_run_status || "-"],
        ];
        summaryEl.innerHTML = cards.map(([label, value]) => `
          <div class="stat">
            <span class="stat-label">${{label}}</span>
            <span class="stat-value">${{value}}</span>
          </div>
        `).join("");
        labelsEl.innerHTML = "";
      }};

      const renderJson = (payload, mode) => {{
        outputEl.textContent = JSON.stringify(payload, null, 2);
        if (mode === "cluster-coverage") {{
          renderClusterCoverage(payload);
        }} else if (mode.startsWith("cluster")) {{
          renderClusterSummary(payload);
        }} else {{
          renderTransactionSummary(payload);
        }}
      }};

      const request = async (url, options = {{}}, mode = "response") => {{
        setStatus("Loading...");
        setMode(mode);
        const response = await fetch(url, options);
        const text = await response.text();
        let data;
        try {{
          data = JSON.parse(text);
        }} catch (_) {{
          data = {{ raw: text }};
        }}
        renderJson(data, mode);
        if (!response.ok) {{
          setStatus(`Request failed with HTTP ${{response.status}}.`);
          return null;
        }}
        setStatus("Done.");
        return data;
      }};

      const parseClusterAddresses = () => {{
        return document.getElementById("cluster-addresses").value
          .split(/\n/)
          .map((line) => line.trim())
          .filter(Boolean)
          .map((line) => {{
            const [address, label, role] = line.split(",").map((item) => item.trim());
            return {{ address, label: label || null, role: role || null }};
          }});
      }};

      document.getElementById("cluster-form").addEventListener("submit", async (event) => {{
        event.preventDefault();
        const payload = {{
          chain_id: Number(document.getElementById("cluster-chain-id").value || "{default_chain_id}"),
          owner_label: document.getElementById("cluster-owner").value.trim(),
          name: document.getElementById("cluster-name").value.trim(),
          description: document.getElementById("cluster-description").value.trim() || null,
          addresses: parseClusterAddresses(),
        }};
        const result = await request("/api/clusters", {{
          method: "POST",
          headers: {{ "content-type": "application/json" }},
          body: JSON.stringify(payload),
        }}, "cluster-create");
        if (result?.cluster_id) {{
          document.getElementById("cluster-summary-id").value = result.cluster_id;
          setStatus("Cluster created. Historical backfill has been queued; keep the worker process running and use Load Coverage to watch progress.");
        }}
      }});

      document.getElementById("load-cluster-coverage").addEventListener("click", async () => {{
        const clusterId = document.getElementById("cluster-summary-id").value.trim();
        await request(`/api/clusters/${{clusterId}}/coverage`, {{}}, "cluster-coverage");
      }});

      document.getElementById("cluster-summary-form").addEventListener("submit", async (event) => {{
        event.preventDefault();
        const clusterId = document.getElementById("cluster-summary-id").value.trim();
        const start = document.getElementById("cluster-start").value.trim();
        const end = document.getElementById("cluster-end").value.trim();
        const params = new URLSearchParams();
        if (start) params.set("start", start);
        if (end) params.set("end", end);
        const suffix = params.toString() ? `?${{params.toString()}}` : "";
        await request(`/api/clusters/${{clusterId}}/spend-summary${{suffix}}`, {{}}, "cluster-summary");
      }});

      const getTxPayload = () => {{
        const chainId = Number(document.getElementById("chain-id").value || "{default_chain_id}");
        const txHash = document.getElementById("tx-hash").value.trim();
        const recognizerIdsRaw = document.getElementById("recognizer-ids").value;
        const recognizerIds = recognizerIdsRaw
          .split(/[,\n]/)
          .map((item) => item.trim())
          .filter(Boolean);
        return {{ chainId, txHash, recognizerIds }};
      }};

      document.getElementById("analysis-form").addEventListener("submit", async (event) => {{
        event.preventDefault();
        const payload = getTxPayload();
        await request("/api/analyses/tx", {{
          method: "POST",
          headers: {{ "content-type": "application/json" }},
          body: JSON.stringify({{ chain_id: payload.chainId, tx_hash: payload.txHash }}),
        }}, "tx-analyze");
      }});

      document.getElementById("load-existing").addEventListener("click", async () => {{
        const payload = getTxPayload();
        await request(`/api/analyses/${{payload.txHash}}?chain_id=${{payload.chainId}}`, {{}}, "tx-load");
      }});

      document.getElementById("replay-run").addEventListener("click", async () => {{
        const payload = getTxPayload();
        await request("/api/replay/tx", {{
          method: "POST",
          headers: {{ "content-type": "application/json" }},
          body: JSON.stringify({{
            chain_id: payload.chainId,
            tx_hash: payload.txHash,
            ruleset_id: "base.ruleset.v0",
            recognizer_ids: payload.recognizerIds.length ? payload.recognizerIds : null,
          }}),
        }}, "tx-replay");
      }});

      document.getElementById("list-recognizers").addEventListener("click", async () => {{
        await request("/api/recognizers", {{}}, "tx-recognizers");
      }});
    </script>
  </body>
</html>"#,
        default_chain_id = state.default_chain_id,
        bind_addr = state.bind_addr,
        api_base_url = state.api_base_url,
    )
}
