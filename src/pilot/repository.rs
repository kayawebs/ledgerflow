use chrono::{DateTime, Duration, Utc};
use serde_json::{Value, json};
use sqlx::{PgPool, Postgres, Row, Transaction, types::Json};
use uuid::Uuid;

use crate::domain::entities::TransactionAnalysis;

use super::{PilotConfig, PilotEvidencePack, classifier::config_digest};

pub type PilotResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub async fn sync_config(pool: &PgPool, config: &PilotConfig) -> PilotResult<()> {
    super::validate_config(config)?;
    let mut tx = pool.begin().await?;
    let digest = config_digest(config);
    sqlx::query("INSERT INTO pilot_projects(project_id,name,chain_id,config,enabled) VALUES($1,$2,$3,$4,$5) ON CONFLICT(project_id) DO UPDATE SET name=excluded.name,chain_id=excluded.chain_id,config=excluded.config,enabled=excluded.enabled,updated_at=now()")
        .bind(&config.project.project_id).bind(&config.project.name).bind(config.project.chain_id as i64)
        .bind(Json(serde_json::to_value(config)?)).bind(config.project.enabled).execute(&mut *tx).await?;
    // Entities are a desired-state projection of the config. Replacing the
    // project's rows keeps sync idempotent when a logical entity changes its
    // address or an address is renamed to a different entity_id.
    sqlx::query("DELETE FROM pilot_entities WHERE project_id=$1")
        .bind(&config.project.project_id)
        .execute(&mut *tx)
        .await?;
    for entity in &config.entities {
        let id = stable_uuid("entity", &config.project.project_id, &entity.entity_id);
        sqlx::query("INSERT INTO pilot_entities(entity_id,project_id,address,role,label,enabled) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(project_id,address) DO UPDATE SET role=excluded.role,label=excluded.label,enabled=excluded.enabled")
            .bind(id).bind(&config.project.project_id).bind(entity.address.to_ascii_lowercase()).bind(&entity.role).bind(&entity.label).bind(entity.include).execute(&mut *tx).await?;
    }
    sqlx::query("UPDATE pilot_rules SET enabled=false WHERE project_id=$1")
        .bind(&config.project.project_id)
        .execute(&mut *tx)
        .await?;
    for rule in &config.rules {
        let id = stable_uuid("rule", &config.project.project_id, &rule.rule_id);
        sqlx::query("INSERT INTO pilot_rules(rule_id,project_id,event_type,severity,threshold,enabled,ruleset_id) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(project_id,event_type) DO UPDATE SET severity=excluded.severity,threshold=excluded.threshold,enabled=excluded.enabled,ruleset_id=excluded.ruleset_id")
            .bind(id).bind(&config.project.project_id).bind(super::event_type_name(&rule.event_type))
            .bind(format!("{:?}",rule.severity).to_ascii_lowercase()).bind(Json(json!({"threshold":rule.threshold,"filters":rule.filters,"rule_id":rule.rule_id})))
            .bind(rule.enabled).bind(&digest).execute(&mut *tx).await?;
    }
    sqlx::query("UPDATE pilot_telegram_destinations SET enabled=false WHERE project_id=$1")
        .bind(&config.project.project_id)
        .execute(&mut *tx)
        .await?;
    for destination in &config.telegram {
        let id = stable_uuid(
            "telegram",
            &config.project.project_id,
            &destination.destination_id,
        );
        sqlx::query("INSERT INTO pilot_telegram_destinations(destination_id,project_id,chat_id,secret_ref,enabled) VALUES($1,$2,$3,$4,$5) ON CONFLICT(destination_id) DO UPDATE SET chat_id=excluded.chat_id,secret_ref=excluded.secret_ref,enabled=excluded.enabled")
            .bind(id).bind(&config.project.project_id).bind(&destination.chat_id).bind(&destination.secret_ref).bind(destination.enabled).execute(&mut *tx).await?;
    }
    sync_monitoring_cluster(&mut tx, config).await?;
    tx.commit().await?;
    Ok(())
}

async fn sync_monitoring_cluster(
    tx: &mut Transaction<'_, Postgres>,
    config: &PilotConfig,
) -> PilotResult<()> {
    let cluster_id = stable_uuid("cluster", &config.project.project_id, "pilot-monitoring");
    let addresses = config
        .entities
        .iter()
        .filter(|entity| entity.include)
        .map(|entity| entity.address.to_ascii_lowercase())
        .collect::<Vec<_>>();
    let metadata = json!({
        "source": "pilot_config",
        "pilot_project_id": config.project.project_id,
        "seed_addresses": addresses.clone(),
        "auto_discovery": {
            "status": "completed",
            "disabled_by": "pilot_config",
            "reason": "Pilot entity scope is customer-configured"
        }
    });
    sqlx::query("INSERT INTO address_clusters(cluster_id,chain_id,owner_label,name,description,metadata) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(cluster_id) DO UPDATE SET chain_id=excluded.chain_id,owner_label=excluded.owner_label,name=excluded.name,description=excluded.description,metadata=excluded.metadata,updated_at=now()")
        .bind(cluster_id)
        .bind(config.project.chain_id as i64)
        .bind(format!("pilot:{}", config.project.project_id))
        .bind(&config.project.name)
        .bind(Some("LedgerFlow Economic Monitoring Pilot v0.1"))
        .bind(Json(metadata.clone()))
        .execute(&mut **tx)
        .await?;
    sqlx::query("DELETE FROM cluster_addresses WHERE cluster_id=$1")
        .bind(cluster_id)
        .execute(&mut **tx)
        .await?;
    for entity in config.entities.iter().filter(|entity| entity.include) {
        sqlx::query("INSERT INTO cluster_addresses(cluster_id,address,label,role,metadata) VALUES($1,$2,$3,$4,$5)")
            .bind(cluster_id)
            .bind(entity.address.to_ascii_lowercase())
            .bind(&entity.label)
            .bind(&entity.role)
            .bind(Json(json!({"source":"pilot_config","entity_id":entity.entity_id})))
            .execute(&mut **tx)
            .await?;
    }
    let tracking_status = if config.project.enabled && config.project.continuous_monitoring {
        "active"
    } else {
        "paused"
    };
    let initial_block = config
        .project
        .monitoring_start_block
        .map(|block| block.saturating_sub(1) as i64);
    sqlx::query("INSERT INTO cluster_tracking_state(cluster_id,chain_id,tracking_status,last_processed_block,tracked_address_count,metadata) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(cluster_id) DO UPDATE SET chain_id=excluded.chain_id,tracking_status=excluded.tracking_status,tracked_address_count=excluded.tracked_address_count,metadata=excluded.metadata,updated_at=now()")
        .bind(cluster_id)
        .bind(config.project.chain_id as i64)
        .bind(tracking_status)
        .bind(initial_block)
        .bind(addresses.len() as i32)
        .bind(Json(metadata))
        .execute(&mut **tx)
        .await?;
    Ok(())
}

pub async fn load_enabled_configs(pool: &PgPool) -> PilotResult<Vec<PilotConfig>> {
    let rows = sqlx::query("SELECT config FROM pilot_projects WHERE enabled=true AND chain_id=8453 ORDER BY project_id").fetch_all(pool).await?;
    rows.into_iter()
        .map(|row| {
            let Json(value): Json<Value> = row.try_get("config")?;
            let config: PilotConfig = serde_json::from_value(value)?;
            super::validate_config(&config)?;
            Ok(config)
        })
        .collect()
}

pub async fn load_unprocessed_analyses(
    pool: &PgPool,
    config: &PilotConfig,
    limit: i64,
) -> PilotResult<Vec<TransactionAnalysis>> {
    let addresses = config
        .entities
        .iter()
        .filter(|v| v.include)
        .map(|v| v.address.to_ascii_lowercase())
        .collect::<Vec<_>>();
    let ruleset_id = config_digest(config);
    let rows = sqlx::query(r#"
        SELECT ta.payload
        FROM transaction_analysis ta
        WHERE ta.chain_id=$1
          AND (
            lower(ta.from_address)=ANY($2) OR lower(ta.to_address)=ANY($2)
            OR EXISTS (SELECT 1 FROM flows f WHERE f.chain_id=ta.chain_id AND f.tx_hash=ta.tx_hash AND (lower(f.from_address)=ANY($2) OR lower(f.to_address)=ANY($2)))
          )
          AND ($5::bigint IS NULL OR NULLIF(ta.payload->>'block_number','')::bigint >= $5)
          AND NOT EXISTS (
            SELECT 1 FROM pilot_analysis_runs run
            WHERE run.project_id=$3 AND run.chain_id=ta.chain_id AND run.tx_hash=ta.tx_hash AND run.source_input_digest=ta.input_digest AND run.ruleset_id=$4 AND run.status='completed'
          )
        ORDER BY ta.block_timestamp ASC NULLS LAST, ta.created_at ASC
        LIMIT $6
    "#).bind(config.project.chain_id as i64).bind(&addresses).bind(&config.project.project_id).bind(&ruleset_id).bind(config.project.monitoring_start_block.map(|value| value as i64)).bind(limit.clamp(1,1000)).fetch_all(pool).await?;
    rows.into_iter()
        .map(|row| {
            let Json(value): Json<Value> = row.try_get("payload")?;
            Ok(serde_json::from_value(value)?)
        })
        .collect()
}

pub async fn failure_count(
    pool: &PgPool,
    project_id: &str,
    analysis: &TransactionAnalysis,
    window_minutes: u32,
) -> PilotResult<u32> {
    let end = analysis.block_timestamp.unwrap_or_else(Utc::now);
    let start = end - Duration::minutes(window_minutes.max(1) as i64);
    let count: i64 = sqlx::query_scalar(
        r#"
        SELECT count(*) FROM transaction_analysis ta
        WHERE ta.chain_id=$1 AND lower(ta.from_address)=lower($2)
          AND trim(both '"' from ta.tx_status)='failure'
          AND ta.block_timestamp >= $3 AND ta.block_timestamp <= $4
          AND EXISTS(SELECT 1 FROM pilot_projects p WHERE p.project_id=$5 AND p.enabled=true)
    "#,
    )
    .bind(analysis.chain_id as i64)
    .bind(&analysis.from_address)
    .bind(start)
    .bind(end)
    .bind(project_id)
    .fetch_one(pool)
    .await?;
    Ok(count.max(0) as u32)
}

pub async fn save_classification(
    pool: &PgPool,
    config: &PilotConfig,
    analysis: &TransactionAnalysis,
    events: &[PilotEvidencePack],
) -> PilotResult<()> {
    let mut tx = pool.begin().await?;
    let current_ids = events
        .iter()
        .map(|event| event.event_id)
        .collect::<Vec<_>>();
    sqlx::query("DELETE FROM pilot_events WHERE project_id=$1 AND chain_id=$2 AND tx_hash=$3 AND event_id<>ALL($4)")
        .bind(&config.project.project_id).bind(analysis.chain_id as i64).bind(&analysis.tx_hash).bind(&current_ids).execute(&mut *tx).await?;
    for event in events {
        save_event(&mut tx, config, event).await?;
    }
    sqlx::query("INSERT INTO pilot_analysis_runs(project_id,chain_id,tx_hash,source_input_digest,ruleset_id,event_count,status,last_error,processed_at) VALUES($1,$2,$3,$4,$5,$6,'completed',null,now()) ON CONFLICT(project_id,chain_id,tx_hash) DO UPDATE SET source_input_digest=excluded.source_input_digest,ruleset_id=excluded.ruleset_id,event_count=excluded.event_count,status='completed',last_error=null,processed_at=now()")
        .bind(&config.project.project_id).bind(analysis.chain_id as i64).bind(&analysis.tx_hash).bind(&analysis.input_digest).bind(config_digest(config)).bind(events.len() as i32).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
}

async fn save_event(
    tx: &mut Transaction<'_, Postgres>,
    config: &PilotConfig,
    event: &PilotEvidencePack,
) -> PilotResult<()> {
    super::validate_evidence_pack(event)
        .map_err(|error| format!("Evidence Pack schema validation failed: {error}"))?;
    let dedup = super::alerts::dedup_key(&config.project.project_id, event);
    sqlx::query("INSERT INTO pilot_events(event_id,project_id,chain_id,block_number,tx_hash,event_type,severity,subject,conclusion,evidence_pack,inputs_digest,dedup_key,status,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'new',$13) ON CONFLICT(project_id,dedup_key) DO UPDATE SET block_number=excluded.block_number,severity=excluded.severity,subject=excluded.subject,conclusion=excluded.conclusion,evidence_pack=excluded.evidence_pack,inputs_digest=excluded.inputs_digest")
        .bind(event.event_id).bind(&config.project.project_id).bind(event.chain_id as i64).bind(event.block_number.map(|v|v as i64)).bind(&event.tx_hash)
        .bind(super::event_type_name(&event.event_type)).bind(format!("{:?}",event.severity).to_ascii_lowercase()).bind(Json(event.subject.clone())).bind(&event.conclusion)
        .bind(Json(serde_json::to_value(event)?)).bind(&event.inputs_digest).bind(&dedup).bind(event.created_at).execute(&mut **tx).await?;
    sqlx::query("INSERT INTO pilot_alert_deliveries(delivery_id,event_id,destination_id,status,next_attempt_at,group_key) SELECT gen_random_uuid(),$1,destination_id,'queued',now(),$2 FROM pilot_telegram_destinations WHERE project_id=$3 AND enabled=true ON CONFLICT(event_id,destination_id) DO NOTHING")
        .bind(event.event_id).bind(format!("{}:{}", config.project.project_id, event.created_at.format("%Y%m%d%H%M"))).bind(&config.project.project_id).execute(&mut **tx).await?;
    Ok(())
}

pub async fn mark_failed(
    pool: &PgPool,
    config: &PilotConfig,
    analysis: &TransactionAnalysis,
    error: &str,
) -> PilotResult<()> {
    sqlx::query("INSERT INTO pilot_analysis_runs(project_id,chain_id,tx_hash,source_input_digest,ruleset_id,event_count,status,last_error,processed_at) VALUES($1,$2,$3,$4,$5,0,'failed',$6,now()) ON CONFLICT(project_id,chain_id,tx_hash) DO UPDATE SET source_input_digest=excluded.source_input_digest,ruleset_id=excluded.ruleset_id,status='failed',last_error=excluded.last_error,processed_at=now()")
        .bind(&config.project.project_id).bind(analysis.chain_id as i64).bind(&analysis.tx_hash).bind(&analysis.input_digest).bind(config_digest(config)).bind(error).execute(pool).await?;
    Ok(())
}

pub async fn load_week_events(
    pool: &PgPool,
    project_id: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> PilotResult<Vec<PilotEvidencePack>> {
    let rows = sqlx::query("SELECT evidence_pack FROM pilot_events WHERE project_id=$1 AND created_at >= $2 AND created_at < $3 ORDER BY created_at,event_id")
        .bind(project_id).bind(start).bind(end).fetch_all(pool).await?;
    rows.into_iter()
        .map(|row| {
            let Json(v): Json<Value> = row.try_get("evidence_pack")?;
            Ok(serde_json::from_value(v)?)
        })
        .collect()
}

pub async fn load_project_events(
    pool: &PgPool,
    project_id: &str,
    limit: i64,
) -> PilotResult<Vec<PilotEvidencePack>> {
    let rows=sqlx::query("SELECT evidence_pack FROM pilot_events WHERE project_id=$1 ORDER BY created_at DESC,event_id LIMIT $2").bind(project_id).bind(limit.clamp(1,1000)).fetch_all(pool).await?;
    rows.into_iter()
        .map(|row| {
            let Json(v): Json<Value> = row.try_get("evidence_pack")?;
            Ok(serde_json::from_value(v)?)
        })
        .collect()
}

/// Load only Evidence Packs from projects that explicitly opted into public
/// publication. The JSON flag intentionally defaults closed for all existing
/// customer configs.
pub async fn load_public_events(pool: &PgPool, limit: i64) -> PilotResult<Vec<PilotEvidencePack>> {
    let rows = sqlx::query(
        r#"
        SELECT event.evidence_pack
        FROM pilot_events event
        JOIN pilot_projects project ON project.project_id = event.project_id
        WHERE project.enabled = true
          AND project.config #>> '{project,public_publish}' = 'true'
        ORDER BY event.created_at DESC, event.event_id
        LIMIT $1
        "#,
    )
    .bind(limit.clamp(1, 5_000))
    .fetch_all(pool)
    .await?;
    let mut events = Vec::with_capacity(rows.len());
    for row in rows {
        let Json(value): Json<Value> = row.try_get("evidence_pack")?;
        let event: PilotEvidencePack = serde_json::from_value(value)?;
        super::validate_evidence_pack(&event).map_err(|error| {
            format!(
                "public event {} failed Evidence Pack validation: {error}",
                event.event_id
            )
        })?;
        events.push(event);
    }
    Ok(events)
}

/// Import a fixed, public regression corpus through the same validation and
/// idempotent persistence path used by the live classifier. This is used only
/// to bootstrap a newly deployed public project; future events are written by
/// the normal event worker.
pub async fn import_events(
    pool: &PgPool,
    config: &PilotConfig,
    events: &[PilotEvidencePack],
) -> PilotResult<usize> {
    if !config.project.public_publish {
        return Err("refusing to import a public corpus into a private Pilot project".into());
    }
    let mut tx = pool.begin().await?;
    for event in events {
        if event.project_id != config.project.project_id {
            return Err(format!(
                "event {} belongs to project {}, expected {}",
                event.event_id, event.project_id, config.project.project_id
            )
            .into());
        }
        save_event(&mut tx, config, event).await?;
    }
    tx.commit().await?;
    Ok(events.len())
}

pub async fn save_weekly_report(
    pool: &PgPool,
    project_id: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    markdown: &str,
    html: &str,
    inputs_digest: &str,
) -> PilotResult<Uuid> {
    let report_id = stable_uuid("weekly", project_id, &start.date_naive().to_string());
    sqlx::query("INSERT INTO pilot_weekly_reports(report_id,project_id,week_start,week_end,markdown,html,inputs_digest) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(project_id,week_start) DO UPDATE SET week_end=excluded.week_end,markdown=excluded.markdown,html=excluded.html,inputs_digest=excluded.inputs_digest,created_at=now()")
        .bind(report_id).bind(project_id).bind(start.date_naive()).bind(end.date_naive()).bind(markdown).bind(html).bind(inputs_digest).execute(pool).await?;
    Ok(report_id)
}

fn stable_uuid(kind: &str, project_id: &str, key: &str) -> Uuid {
    Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        format!("ledgerflow:{kind}:{project_id}:{key}").as_bytes(),
    )
}
