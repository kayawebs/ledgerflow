use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::types::Json as SqlJson;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::core::errors::{LedgerError, LedgerResult};

#[derive(Clone, Debug)]
pub struct DashboardClusterRecord {
    pub cluster_id: Uuid,
    pub chain_id: u64,
    pub owner_label: String,
    pub name: String,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
    pub address_count: u32,
    pub metadata: Value,
}

#[derive(Clone)]
pub struct PortalService {
    pool: PgPool,
}

impl PortalService {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn attach_cluster_to_user(
        &self,
        user_id: Uuid,
        cluster_id: Uuid,
    ) -> LedgerResult<()> {
        sqlx::query(
            r#"
            insert into app_user_clusters (user_id, cluster_id)
            values ($1, $2)
            on conflict (user_id, cluster_id) do nothing
            "#,
        )
        .bind(user_id)
        .bind(cluster_id)
        .execute(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        Ok(())
    }

    pub async fn list_clusters_for_user(
        &self,
        user_id: Uuid,
    ) -> LedgerResult<Vec<DashboardClusterRecord>> {
        let rows = sqlx::query(
            r#"
            select
              clusters.cluster_id,
              clusters.chain_id,
              clusters.owner_label,
              clusters.name,
              clusters.description,
              clusters.created_at,
              clusters.metadata,
              count(addresses.cluster_address_id)::bigint as address_count
            from app_user_clusters links
            join address_clusters clusters on clusters.cluster_id = links.cluster_id
            left join cluster_addresses addresses on addresses.cluster_id = clusters.cluster_id
            where links.user_id = $1
            group by
              clusters.cluster_id,
              clusters.chain_id,
              clusters.owner_label,
              clusters.name,
              clusters.description,
              clusters.created_at,
              clusters.metadata
            order by clusters.created_at desc, clusters.name asc
            "#,
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        rows.into_iter()
            .map(|row| {
                Ok(DashboardClusterRecord {
                    cluster_id: row
                        .try_get("cluster_id")
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                    chain_id: row
                        .try_get::<i64, _>("chain_id")
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?
                        as u64,
                    owner_label: row
                        .try_get("owner_label")
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                    name: row
                        .try_get("name")
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                    description: row
                        .try_get("description")
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                    created_at: row
                        .try_get("created_at")
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                    address_count: row
                        .try_get::<i64, _>("address_count")
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?
                        as u32,
                    metadata: row
                        .try_get::<SqlJson<Value>, _>("metadata")
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?
                        .0,
                })
            })
            .collect()
    }
}
