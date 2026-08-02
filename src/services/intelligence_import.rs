use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row, types::Json};
use uuid::Uuid;

use crate::core::errors::{LedgerError, LedgerResult};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct IntelligenceDatasetManifest {
    pub dataset_key: String,
    pub dataset_version: String,
    pub chain_id: u64,
    pub source: DatasetSource,
    #[serde(default)]
    pub addresses: Vec<DatasetAddress>,
    #[serde(default = "json_object")]
    pub metadata: Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DatasetSource {
    pub source_key: String,
    pub display_name: String,
    pub source_kind: String,
    pub trust_score: f32,
    pub owner: Option<String>,
    pub homepage: Option<String>,
    #[serde(default = "json_object")]
    pub metadata: Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DatasetAddress {
    pub address: String,
    #[serde(default = "unknown_address_kind")]
    pub address_kind: String,
    #[serde(default)]
    pub labels: Vec<DatasetLabel>,
    pub token_metadata: Option<DatasetTokenMetadata>,
    #[serde(default)]
    pub protocol_deployments: Vec<DatasetProtocolDeployment>,
    #[serde(default = "json_object")]
    pub metadata: Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DatasetLabel {
    pub label_key: String,
    pub dimension: String,
    pub exclusive_group: Option<String>,
    #[serde(default = "default_confidence")]
    pub confidence: f32,
    pub description: Option<String>,
    #[serde(default = "json_object")]
    pub evidence: Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DatasetTokenMetadata {
    pub symbol: String,
    pub name: String,
    pub decimals: i32,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DatasetProtocolDeployment {
    pub protocol_key: String,
    pub display_name: String,
    pub protocol_family: String,
    pub contract_role: String,
    #[serde(default = "default_confidence")]
    pub confidence: f32,
    #[serde(default = "json_object")]
    pub metadata: Value,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct IntelligenceImportSummary {
    pub chain_id: u64,
    pub dataset_key: String,
    pub dataset_version: String,
    pub address_rows: u64,
    pub assertion_rows: u64,
    pub evidence_rows: u64,
    pub token_rows: u64,
    pub protocol_rows: u64,
}

pub fn parse_dataset(raw: &str) -> LedgerResult<IntelligenceDatasetManifest> {
    serde_json::from_str(raw)
        .map_err(|err| LedgerError::MalformedEvidence(format!("invalid dataset JSON: {err}")))
}

pub fn dataset_checksum(raw: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(raw.as_bytes());
    hex::encode(hasher.finalize())
}

pub async fn import_intelligence_dataset(
    pool: &PgPool,
    manifest: &IntelligenceDatasetManifest,
    checksum_sha256: &str,
) -> LedgerResult<IntelligenceImportSummary> {
    validate_manifest(manifest, checksum_sha256)?;
    let mut tx = pool
        .begin()
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;

    sqlx::query(
        r#"
        insert into intelligence_sources (
          source_key, display_name, source_kind, trust_score, owner, homepage, metadata
        )
        values ($1,$2,$3,$4,$5,$6,$7)
        on conflict (source_key) do update set
          display_name = excluded.display_name,
          source_kind = excluded.source_kind,
          trust_score = excluded.trust_score,
          owner = excluded.owner,
          homepage = excluded.homepage,
          status = 'active',
          metadata = intelligence_sources.metadata || excluded.metadata,
          updated_at = now()
        "#,
    )
    .bind(&manifest.source.source_key)
    .bind(&manifest.source.display_name)
    .bind(&manifest.source.source_kind)
    .bind(manifest.source.trust_score as f64)
    .bind(&manifest.source.owner)
    .bind(&manifest.source.homepage)
    .bind(Json(manifest.source.metadata.clone()))
    .execute(&mut *tx)
    .await
    .map_err(|err| LedgerError::Persistence(err.to_string()))?;

    sqlx::query(
        r#"
        insert into intelligence_datasets (
          dataset_key, dataset_version, source_key, chain_id, checksum_sha256,
          status, manifest, imported_at
        )
        values ($1,$2,$3,$4,$5,'active',$6,now())
        on conflict (dataset_key, dataset_version) do update set
          source_key = excluded.source_key,
          chain_id = excluded.chain_id,
          checksum_sha256 = excluded.checksum_sha256,
          status = 'active',
          manifest = excluded.manifest,
          imported_at = now()
        "#,
    )
    .bind(&manifest.dataset_key)
    .bind(&manifest.dataset_version)
    .bind(&manifest.source.source_key)
    .bind(manifest.chain_id as i64)
    .bind(checksum_sha256)
    .bind(Json(serde_json::to_value(manifest).map_err(|err| {
        LedgerError::MalformedEvidence(err.to_string())
    })?))
    .execute(&mut *tx)
    .await
    .map_err(|err| LedgerError::Persistence(err.to_string()))?;

    // A newer import replaces the active projection of earlier versions. The
    // earlier assertions remain queryable for audit and rollback.
    sqlx::query(
        r#"
        update address_label_assertions
        set status = 'superseded', updated_at = now()
        where dataset_key = $1 and dataset_version <> $2 and status = 'active'
        "#,
    )
    .bind(&manifest.dataset_key)
    .bind(&manifest.dataset_version)
    .execute(&mut *tx)
    .await
    .map_err(|err| LedgerError::Persistence(err.to_string()))?;
    sqlx::query(
        r#"
        update protocol_deployments
        set status = 'superseded', updated_at = now()
        where dataset_key = $1 and dataset_version <> $2 and status = 'active'
        "#,
    )
    .bind(&manifest.dataset_key)
    .bind(&manifest.dataset_version)
    .execute(&mut *tx)
    .await
    .map_err(|err| LedgerError::Persistence(err.to_string()))?;
    sqlx::query(
        r#"
        update intelligence_datasets
        set status = 'superseded'
        where dataset_key = $1 and dataset_version <> $2 and status = 'active'
        "#,
    )
    .bind(&manifest.dataset_key)
    .bind(&manifest.dataset_version)
    .execute(&mut *tx)
    .await
    .map_err(|err| LedgerError::Persistence(err.to_string()))?;

    let mut summary = IntelligenceImportSummary {
        chain_id: manifest.chain_id,
        dataset_key: manifest.dataset_key.clone(),
        dataset_version: manifest.dataset_version.clone(),
        ..IntelligenceImportSummary::default()
    };

    for entry in &manifest.addresses {
        let address = normalize_evm_address(&entry.address)?;
        let address_result = sqlx::query(
            r#"
            insert into chain_addresses (
              chain_id, address, address_kind, metadata, first_seen_at, last_seen_at
            )
            values ($1,$2,$3,$4,now(),now())
            on conflict (chain_id, address) do update set
              address_kind = case
                when excluded.address_kind = 'unknown' then chain_addresses.address_kind
                else excluded.address_kind
              end,
              metadata = chain_addresses.metadata || excluded.metadata,
              last_seen_at = now(),
              updated_at = now()
            "#,
        )
        .bind(manifest.chain_id as i64)
        .bind(&address)
        .bind(&entry.address_kind)
        .bind(Json(entry.metadata.clone()))
        .execute(&mut *tx)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        summary.address_rows += address_result.rows_affected();

        for label in &entry.labels {
            let result =
                import_label_assertion(&mut tx, manifest, &address, label, checksum_sha256).await?;
            summary.assertion_rows += result.0;
            summary.evidence_rows += result.1;
        }

        if let Some(token) = &entry.token_metadata {
            let result = sqlx::query(
                r#"
                insert into erc20_token_metadata (
                  chain_id, token_address, symbol, name, decimals, fetch_status,
                  fetched_at, created_at, updated_at
                )
                values ($1,$2,$3,$4,$5,'ok',now(),now(),now())
                on conflict (chain_id, token_address) do update set
                  symbol = excluded.symbol,
                  name = excluded.name,
                  decimals = excluded.decimals,
                  fetch_status = 'ok',
                  error_message = null,
                  fetched_at = now(),
                  updated_at = now()
                "#,
            )
            .bind(manifest.chain_id as i64)
            .bind(&address)
            .bind(&token.symbol)
            .bind(&token.name)
            .bind(token.decimals)
            .execute(&mut *tx)
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
            summary.token_rows += result.rows_affected();
        }

        for deployment in &entry.protocol_deployments {
            let result =
                import_protocol_deployment(&mut tx, manifest, &address, deployment).await?;
            summary.protocol_rows += result;
        }
    }

    tx.commit()
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
    Ok(summary)
}

async fn import_label_assertion(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    manifest: &IntelligenceDatasetManifest,
    address: &str,
    label: &DatasetLabel,
    checksum_sha256: &str,
) -> LedgerResult<(u64, u64)> {
    validate_confidence(label.confidence, &format!("label {}", label.label_key))?;
    if label.label_key.trim().is_empty() || label.dimension.trim().is_empty() {
        return Err(LedgerError::MalformedEvidence(
            "label_key and dimension must not be empty".to_owned(),
        ));
    }
    sqlx::query(
        r#"
        insert into label_definitions (
          label_key, display_name, dimension, exclusive_group, description, metadata
        )
        values ($1,$1,$2,$3,$4,'{}'::jsonb)
        on conflict (label_key) do update set
          dimension = excluded.dimension,
          exclusive_group = excluded.exclusive_group,
          description = coalesce(excluded.description, label_definitions.description),
          status = 'active',
          updated_at = now()
        "#,
    )
    .bind(&label.label_key)
    .bind(&label.dimension)
    .bind(&label.exclusive_group)
    .bind(&label.description)
    .execute(&mut **tx)
    .await
    .map_err(|err| LedgerError::Persistence(err.to_string()))?;

    let fingerprint = claim_fingerprint(manifest, address, &label.label_key);
    let origin_kind = match manifest.source.source_kind.as_str() {
        "official" => "official",
        "manual" => "manual",
        "onchain" => "onchain",
        "model" => "model",
        _ => "curated_seed",
    };
    let evidence = json!({
        "dataset_key": manifest.dataset_key,
        "dataset_version": manifest.dataset_version,
        "dataset_checksum": checksum_sha256,
        "evidence": label.evidence,
    });
    let row = sqlx::query(
        r#"
        insert into address_label_assertions (
          claim_fingerprint, chain_id, address, label_key, source_key, origin_kind,
          method_version, dataset_key, dataset_version, assertion_value, confidence,
          status, observed_at, evidence_summary, created_at, updated_at
        )
        values ($1,$2,$3,$4,$5,$6,$7,$8,$9,true,$10,'active',now(),$11,now(),now())
        on conflict (claim_fingerprint) do update set
          confidence = excluded.confidence,
          status = 'active',
          observed_at = now(),
          evidence_summary = excluded.evidence_summary,
          updated_at = now()
        returning assertion_id
        "#,
    )
    .bind(&fingerprint)
    .bind(manifest.chain_id as i64)
    .bind(address)
    .bind(&label.label_key)
    .bind(&manifest.source.source_key)
    .bind(origin_kind)
    .bind(format!("dataset:{}", manifest.dataset_version))
    .bind(&manifest.dataset_key)
    .bind(&manifest.dataset_version)
    .bind(label.confidence as f64)
    .bind(Json(evidence.clone()))
    .fetch_one(&mut **tx)
    .await
    .map_err(|err| LedgerError::Persistence(err.to_string()))?;
    let assertion_id: Uuid = row
        .try_get("assertion_id")
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;

    let evidence_hash = json_hash(&evidence);
    let evidence_result = sqlx::query(
        r#"
        insert into assertion_evidence (
          assertion_id, evidence_type, evidence_ref, evidence_hash, payload, observed_at
        )
        values ($1,'dataset_manifest',$2,$3,$4,now())
        on conflict (assertion_id, evidence_hash) do nothing
        "#,
    )
    .bind(assertion_id)
    .bind(format!(
        "{}@{}",
        manifest.dataset_key, manifest.dataset_version
    ))
    .bind(evidence_hash)
    .bind(Json(evidence.clone()))
    .execute(&mut **tx)
    .await
    .map_err(|err| LedgerError::Persistence(err.to_string()))?;

    sqlx::query(
        r#"
        insert into address_labels (
          chain_id, address, label, source, confidence, evidence_json, created_at, updated_at
        )
        values ($1,$2,$3,$4,$5,$6,now(),now())
        on conflict (chain_id, address, label, source) do update set
          confidence = excluded.confidence,
          evidence_json = excluded.evidence_json,
          updated_at = now()
        "#,
    )
    .bind(manifest.chain_id as i64)
    .bind(address)
    .bind(&label.label_key)
    .bind(&manifest.source.source_key)
    .bind(label.confidence as f64)
    .bind(Json(evidence))
    .execute(&mut **tx)
    .await
    .map_err(|err| LedgerError::Persistence(err.to_string()))?;

    Ok((1, evidence_result.rows_affected()))
}

async fn import_protocol_deployment(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    manifest: &IntelligenceDatasetManifest,
    address: &str,
    deployment: &DatasetProtocolDeployment,
) -> LedgerResult<u64> {
    validate_confidence(
        deployment.confidence,
        &format!("protocol {}", deployment.protocol_key),
    )?;
    sqlx::query(
        r#"
        insert into protocols (protocol_key, display_name, protocol_family, metadata)
        values ($1,$2,$3,$4)
        on conflict (protocol_key) do update set
          display_name = excluded.display_name,
          protocol_family = excluded.protocol_family,
          metadata = protocols.metadata || excluded.metadata,
          updated_at = now()
        "#,
    )
    .bind(&deployment.protocol_key)
    .bind(&deployment.display_name)
    .bind(&deployment.protocol_family)
    .bind(Json(deployment.metadata.clone()))
    .execute(&mut **tx)
    .await
    .map_err(|err| LedgerError::Persistence(err.to_string()))?;

    let result = sqlx::query(
        r#"
        insert into protocol_deployments (
          chain_id, address, protocol_key, contract_role, source_key, method_version,
          dataset_key, dataset_version, confidence, status, metadata,
          first_seen_at, last_seen_at, created_at, updated_at
        )
        values ($1,$2,$3,$4,$5,$6,$7,$8,$9,'active',$10,now(),now(),now(),now())
        on conflict (chain_id, address, protocol_key, contract_role, source_key, method_version)
        do update set
          dataset_key = excluded.dataset_key,
          dataset_version = excluded.dataset_version,
          confidence = excluded.confidence,
          status = 'active',
          metadata = excluded.metadata,
          last_seen_at = now(),
          updated_at = now()
        "#,
    )
    .bind(manifest.chain_id as i64)
    .bind(address)
    .bind(&deployment.protocol_key)
    .bind(&deployment.contract_role)
    .bind(&manifest.source.source_key)
    .bind(format!("dataset:{}", manifest.dataset_version))
    .bind(&manifest.dataset_key)
    .bind(&manifest.dataset_version)
    .bind(deployment.confidence as f64)
    .bind(Json(deployment.metadata.clone()))
    .execute(&mut **tx)
    .await
    .map_err(|err| LedgerError::Persistence(err.to_string()))?;
    Ok(result.rows_affected())
}

fn validate_manifest(
    manifest: &IntelligenceDatasetManifest,
    checksum_sha256: &str,
) -> LedgerResult<()> {
    if manifest.dataset_key.trim().is_empty()
        || manifest.dataset_version.trim().is_empty()
        || manifest.source.source_key.trim().is_empty()
    {
        return Err(LedgerError::MalformedEvidence(
            "dataset_key, dataset_version, and source_key are required".to_owned(),
        ));
    }
    if manifest.chain_id == 0 {
        return Err(LedgerError::MalformedEvidence(
            "chain_id must be greater than zero".to_owned(),
        ));
    }
    if manifest
        .metadata
        .get("template")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Err(LedgerError::MalformedEvidence(
            "dataset is marked as a template; verify the addresses and remove metadata.template before import"
                .to_owned(),
        ));
    }
    if checksum_sha256.len() != 64 || !checksum_sha256.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return Err(LedgerError::MalformedEvidence(
            "checksum_sha256 must be a 64-character hex digest".to_owned(),
        ));
    }
    if !matches!(
        manifest.source.source_kind.as_str(),
        "curated" | "official" | "manual" | "onchain" | "model" | "import" | "migration"
    ) {
        return Err(LedgerError::MalformedEvidence(format!(
            "unsupported source_kind {}",
            manifest.source.source_kind
        )));
    }
    validate_confidence(manifest.source.trust_score, "source trust_score")?;
    for entry in &manifest.addresses {
        normalize_evm_address(&entry.address)?;
        if !matches!(
            entry.address_kind.as_str(),
            "unknown" | "eoa" | "contract" | "system" | "precompile"
        ) {
            return Err(LedgerError::MalformedEvidence(format!(
                "unsupported address_kind {} for {}",
                entry.address_kind, entry.address
            )));
        }
        if let Some(token) = &entry.token_metadata
            && !(0..=255).contains(&token.decimals)
        {
            return Err(LedgerError::MalformedEvidence(format!(
                "invalid token decimals for {}",
                entry.address
            )));
        }
    }
    Ok(())
}

fn validate_confidence(value: f32, field: &str) -> LedgerResult<()> {
    if !value.is_finite() || !(0.0..=1.0).contains(&value) {
        return Err(LedgerError::MalformedEvidence(format!(
            "{field} confidence must be between 0 and 1"
        )));
    }
    Ok(())
}

fn normalize_evm_address(value: &str) -> LedgerResult<String> {
    let normalized = value.trim().to_ascii_lowercase();
    let body = normalized.strip_prefix("0x").ok_or_else(|| {
        LedgerError::MalformedEvidence(format!("address must start with 0x: {value}"))
    })?;
    if body.len() != 40 || !body.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return Err(LedgerError::MalformedEvidence(format!(
            "invalid EVM address: {value}"
        )));
    }
    Ok(normalized)
}

fn claim_fingerprint(
    manifest: &IntelligenceDatasetManifest,
    address: &str,
    label_key: &str,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(format!(
        "{}|{}|{}|{}|{}|{}",
        manifest.chain_id,
        address,
        label_key,
        manifest.source.source_key,
        manifest.dataset_key,
        manifest.dataset_version
    ));
    hex::encode(hasher.finalize())
}

fn json_hash(value: &Value) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.to_string());
    hex::encode(hasher.finalize())
}

fn json_object() -> Value {
    json!({})
}

fn unknown_address_kind() -> String {
    "unknown".to_owned()
}

fn default_confidence() -> f32 {
    1.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_non_evm_addresses() {
        assert!(normalize_evm_address("0x1234").is_err());
        assert!(normalize_evm_address("not-an-address").is_err());
    }

    #[test]
    fn normalizes_evm_addresses() {
        assert_eq!(
            normalize_evm_address("0x4200000000000000000000000000000000000006").unwrap(),
            "0x4200000000000000000000000000000000000006"
        );
    }

    #[test]
    fn dataset_fingerprints_are_versioned() {
        let source = DatasetSource {
            source_key: "curated".to_owned(),
            display_name: "Curated".to_owned(),
            source_kind: "curated".to_owned(),
            trust_score: 1.0,
            owner: None,
            homepage: None,
            metadata: json!({}),
        };
        let mut manifest = IntelligenceDatasetManifest {
            dataset_key: "core".to_owned(),
            dataset_version: "1".to_owned(),
            chain_id: 4663,
            source,
            addresses: vec![],
            metadata: json!({}),
        };
        let first = claim_fingerprint(
            &manifest,
            "0x0000000000000000000000000000000000000001",
            "router",
        );
        manifest.dataset_version = "2".to_owned();
        let second = claim_fingerprint(
            &manifest,
            "0x0000000000000000000000000000000000000001",
            "router",
        );
        assert_ne!(first, second);
    }

    #[test]
    fn production_import_rejects_templates() {
        let raw = include_str!("../../config/intelligence/robinhood-mainnet.template.json");
        let manifest = parse_dataset(raw).unwrap();
        let error = validate_manifest(&manifest, &dataset_checksum(raw)).unwrap_err();
        assert!(error.to_string().contains("marked as a template"));
    }
}
