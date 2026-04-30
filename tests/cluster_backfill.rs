mod common;

use std::{collections::HashMap, sync::{Arc, Mutex}};

use async_trait::async_trait;
use chrono::Utc;
use serde_json::json;

use common::MockRpcClient;
use ledgerflow::{
    core::errors::LedgerResult,
    domain::{
        entities::ClusterAddress,
        enums::{AddressHitDirection, BehaviorTag, ClusterBackfillStatus, ClusterTrackingStatus},
        interfaces::ClusterTxSource,
        value_objects::AddressTxRef,
    },
    ingestion::fetchers::TransactionBundleFetcher,
    plugins::loader::load_plugins,
    recognition::registry::RecognizerRegistry,
    repositories::memory::InMemoryAnalysisRepository,
    services::{
        analysis::AnalysisService, cluster::ClusterService,
        cluster_backfill::ClusterBackfillRequest, cluster_backfill::ClusterBackfillService,
        cluster_tracking::ClusterTrackingService,
    },
};

#[derive(Clone)]
struct MockClusterTxSource {
    last_indexed_block: u64,
    refs_by_address: HashMap<String, Vec<AddressTxRef>>,
}

#[derive(Clone)]
struct MutableClusterTxSource {
    inner: Arc<Mutex<MockClusterTxSource>>,
}

impl MutableClusterTxSource {
    fn new(last_indexed_block: u64, refs_by_address: HashMap<String, Vec<AddressTxRef>>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(MockClusterTxSource {
                last_indexed_block,
                refs_by_address,
            })),
        }
    }

    fn update_last_indexed_block(&self, value: u64) {
        self.inner.lock().unwrap().last_indexed_block = value;
    }

    fn insert_refs(&self, address: &str, refs: Vec<AddressTxRef>) {
        self.inner
            .lock()
            .unwrap()
            .refs_by_address
            .insert(address.to_owned(), refs);
    }
}

#[async_trait]
impl ClusterTxSource for MockClusterTxSource {
    async fn get_last_indexed_block(&self, _chain_id: u64) -> LedgerResult<u64> {
        Ok(self.last_indexed_block)
    }

    async fn query_address_refs(
        &self,
        _chain_id: u64,
        address: &str,
        limit: u32,
        from_block: Option<u64>,
        to_block: Option<u64>,
    ) -> LedgerResult<Vec<AddressTxRef>> {
        let from_block = from_block.unwrap_or(0);
        let to_block = to_block.unwrap_or(u64::MAX);
        Ok(self
            .refs_by_address
            .get(address)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|item| item.block_number >= from_block && item.block_number <= to_block)
            .take(limit as usize)
            .collect())
    }
}

#[async_trait]
impl ClusterTxSource for MutableClusterTxSource {
    async fn get_last_indexed_block(&self, _chain_id: u64) -> LedgerResult<u64> {
        Ok(self.inner.lock().unwrap().last_indexed_block)
    }

    async fn query_address_refs(
        &self,
        _chain_id: u64,
        address: &str,
        limit: u32,
        from_block: Option<u64>,
        to_block: Option<u64>,
    ) -> LedgerResult<Vec<AddressTxRef>> {
        let from_block = from_block.unwrap_or(0);
        let to_block = to_block.unwrap_or(u64::MAX);
        Ok(self
            .inner
            .lock()
            .unwrap()
            .refs_by_address
            .get(address)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|item| item.block_number >= from_block && item.block_number <= to_block)
            .take(limit as usize)
            .collect())
    }
}

#[tokio::test]
async fn cluster_backfill_discovers_dedupes_and_analyzes_transactions() {
    let repository = Arc::new(InMemoryAnalysisRepository::default());
    let fetcher = TransactionBundleFetcher::new(MockRpcClient::default().boxed(), true);
    let mut registry = RecognizerRegistry::new();
    let plugin_context = load_plugins(&["sample_builtin_pack".to_owned()], &mut registry);
    let analysis_service = Arc::new(AnalysisService::new(
        fetcher,
        registry,
        plugin_context,
        repository.clone(),
        vec![
            "selector_presence".to_owned(),
            "erc20_transfer_flow".to_owned(),
            "native_transfer".to_owned(),
        ],
    ));
    let backfill_service = ClusterBackfillService::new(
        Arc::clone(&analysis_service),
        Some(Arc::new(MockClusterTxSource {
            last_indexed_block: 100,
            refs_by_address: HashMap::from([
                (
                    "0x1111111111111111111111111111111111111111".to_owned(),
                    vec![
                        AddressTxRef {
                            address: "0x1111111111111111111111111111111111111111".to_owned(),
                            tx_hash: "0xcluster-a".to_owned(),
                            block_number: 12,
                            tx_index: 0,
                            direction: AddressHitDirection::From,
                            from_address: None,
                            to_address: None,
                        },
                        AddressTxRef {
                            address: "0x1111111111111111111111111111111111111111".to_owned(),
                            tx_hash: "0xcluster-b".to_owned(),
                            block_number: 30,
                            tx_index: 1,
                            direction: AddressHitDirection::From,
                            from_address: None,
                            to_address: None,
                        },
                    ],
                ),
                (
                    "0x5555555555555555555555555555555555555555".to_owned(),
                    vec![AddressTxRef {
                        address: "0x5555555555555555555555555555555555555555".to_owned(),
                        tx_hash: "0xcluster-b".to_owned(),
                        block_number: 30,
                        tx_index: 1,
                        direction: AddressHitDirection::To,
                        from_address: None,
                        to_address: None,
                    }],
                ),
            ]),
        })),
        1,
        8,
    );
    let cluster_service = ClusterService::new(repository.clone());

    let cluster = cluster_service
        .create_cluster(
            8453,
            "ops_team",
            "primary_searcher",
            Some("bot + treasury".to_owned()),
            vec![
                ClusterAddress {
                    address: "0x1111111111111111111111111111111111111111".to_owned(),
                    label: Some("bot".to_owned()),
                    role: Some("bot".to_owned()),
                    metadata: json!({}),
                },
                ClusterAddress {
                    address: "0x5555555555555555555555555555555555555555".to_owned(),
                    label: Some("treasury".to_owned()),
                    role: Some("eoa".to_owned()),
                    metadata: json!({}),
                },
            ],
        )
        .await
        .unwrap();

    let result = backfill_service
        .backfill_cluster(
            cluster.cluster_id,
            ClusterBackfillRequest {
                start_block: Some(0),
                end_block: Some(100),
                force_reanalyze: false,
                query_limit: Some(1),
            },
        )
        .await
        .unwrap();

    assert_eq!(result.run.discovered_tx_count, 2);
    assert_eq!(result.run.analyzed_tx_count, 2);
    assert_eq!(result.run.failed_tx_count, 0);
    assert_eq!(result.coverage.discovered_tx_count, 2);
    assert_eq!(result.coverage.analyzed_tx_count, 2);
    assert_eq!(result.coverage.pending_tx_count, 0);

    let summary = cluster_service
        .get_cluster_summary(cluster.cluster_id, None, None)
        .await
        .unwrap();
    assert_eq!(summary.analyzed_tx_count, 2);
    assert_eq!(
        summary.metadata["coverage"]["discovered_tx_count"],
        json!(2)
    );
    assert_eq!(summary.metadata["coverage"]["analyzed_tx_count"], json!(2));

    let runs = backfill_service
        .list_cluster_backfill_runs(cluster.cluster_id)
        .await
        .unwrap();
    assert_eq!(runs.len(), 1);
}

#[tokio::test]
async fn cluster_coverage_is_zero_before_any_backfill() {
    let repository = Arc::new(InMemoryAnalysisRepository::default());
    let fetcher = TransactionBundleFetcher::new(MockRpcClient::default().boxed(), true);
    let mut registry = RecognizerRegistry::new();
    let plugin_context = load_plugins(&["sample_builtin_pack".to_owned()], &mut registry);
    let analysis_service = Arc::new(AnalysisService::new(
        fetcher,
        registry,
        plugin_context,
        repository.clone(),
        vec!["selector_presence".to_owned()],
    ));
    let backfill_service = ClusterBackfillService::new(
        Arc::clone(&analysis_service),
        Some(Arc::new(MockClusterTxSource {
            last_indexed_block: 0,
            refs_by_address: HashMap::new(),
        })),
        100,
        8,
    );
    let cluster_service = ClusterService::new(repository.clone());

    let cluster = cluster_service
        .create_cluster(
            8453,
            "ops_team",
            "empty_cluster",
            None,
            vec![ClusterAddress {
                address: "0x1111111111111111111111111111111111111111".to_owned(),
                label: None,
                role: None,
                metadata: json!({}),
            }],
        )
        .await
        .unwrap();

    let coverage = backfill_service
        .get_cluster_coverage(cluster.cluster_id)
        .await
        .unwrap();
    assert_eq!(coverage.discovered_tx_count, 0);
    assert_eq!(coverage.analyzed_tx_count, 0);
    assert_eq!(coverage.failed_tx_count, 0);
    assert_eq!(coverage.pending_tx_count, 0);
    assert_eq!(coverage.latest_run_id, None);
    assert_eq!(coverage.latest_run_started_at, None);
    assert_eq!(coverage.latest_run_finished_at, None);
    assert!(coverage.metadata["has_backfill_runs"] == json!(false));

    let now = Utc::now();
    assert!(
        coverage
            .metadata
            .as_object()
            .map(|item| !item.is_empty())
            .unwrap_or(false)
    );
    assert!(now <= Utc::now());
}

#[tokio::test]
async fn queued_cluster_backfill_is_processed_by_worker_path() {
    let repository = Arc::new(InMemoryAnalysisRepository::default());
    let fetcher = TransactionBundleFetcher::new(MockRpcClient::default().boxed(), true);
    let mut registry = RecognizerRegistry::new();
    let plugin_context = load_plugins(&["sample_builtin_pack".to_owned()], &mut registry);
    let analysis_service = Arc::new(AnalysisService::new(
        fetcher,
        registry,
        plugin_context,
        repository.clone(),
        vec!["selector_presence".to_owned(), "native_transfer".to_owned()],
    ));
    let backfill_service = ClusterBackfillService::new(
        Arc::clone(&analysis_service),
        Some(Arc::new(MockClusterTxSource {
            last_indexed_block: 42,
            refs_by_address: HashMap::from([(
                "0x1111111111111111111111111111111111111111".to_owned(),
                vec![AddressTxRef {
                    address: "0x1111111111111111111111111111111111111111".to_owned(),
                    tx_hash: "0xqueued-a".to_owned(),
                    block_number: 9,
                    tx_index: 0,
                    direction: AddressHitDirection::From,
                    from_address: None,
                    to_address: None,
                }],
            )]),
        })),
        100,
        8,
    );
    let cluster_service = ClusterService::new(repository.clone());

    let cluster = cluster_service
        .create_cluster(
            8453,
            "ops_team",
            "queued_cluster",
            None,
            vec![ClusterAddress {
                address: "0x1111111111111111111111111111111111111111".to_owned(),
                label: Some("bot".to_owned()),
                role: Some("bot".to_owned()),
                metadata: json!({}),
            }],
        )
        .await
        .unwrap();

    let queued = backfill_service
        .enqueue_cluster(
            cluster.cluster_id,
            ClusterBackfillRequest {
                start_block: Some(0),
                end_block: None,
                force_reanalyze: false,
                query_limit: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(queued.status, ClusterBackfillStatus::Pending);

    let result = backfill_service
        .process_next_pending_run()
        .await
        .unwrap()
        .expect("pending run should be claimed");
    assert_eq!(result.run.run_id, queued.run_id);
    assert_eq!(result.run.status, ClusterBackfillStatus::Completed);
    assert_eq!(result.coverage.discovered_tx_count, 1);
    assert_eq!(result.coverage.analyzed_tx_count, 1);
}

#[tokio::test]
async fn incremental_watch_enqueues_follow_up_range_after_initial_backfill() {
    let repository = Arc::new(InMemoryAnalysisRepository::default());
    let fetcher = TransactionBundleFetcher::new(MockRpcClient::default().boxed(), true);
    let mut registry = RecognizerRegistry::new();
    let plugin_context = load_plugins(&["sample_builtin_pack".to_owned()], &mut registry);
    let analysis_service = Arc::new(AnalysisService::new(
        fetcher,
        registry,
        plugin_context,
        repository.clone(),
        vec!["selector_presence".to_owned(), "native_transfer".to_owned()],
    ));
    let tx_source = MutableClusterTxSource::new(
        50,
        HashMap::from([(
            "0x1111111111111111111111111111111111111111".to_owned(),
            vec![AddressTxRef {
                address: "0x1111111111111111111111111111111111111111".to_owned(),
                tx_hash: "0xwatch-a".to_owned(),
                block_number: 40,
                tx_index: 0,
                direction: AddressHitDirection::From,
                from_address: None,
                to_address: None,
            }],
        )]),
    );
    let backfill_service = ClusterBackfillService::new(
        Arc::clone(&analysis_service),
        Some(Arc::new(tx_source.clone())),
        100,
        4,
    );
    let cluster_service = ClusterService::new(repository.clone());
    let tracking_service = ClusterTrackingService::new(repository.clone());

    let cluster = cluster_service
        .create_cluster(
            8453,
            "ops_team",
            "watch_cluster",
            None,
            vec![ClusterAddress {
                address: "0x1111111111111111111111111111111111111111".to_owned(),
                label: Some("bot".to_owned()),
                role: Some("bot".to_owned()),
                metadata: json!({}),
            }],
        )
        .await
        .unwrap();

    backfill_service
        .backfill_cluster(
            cluster.cluster_id,
            ClusterBackfillRequest {
                start_block: Some(0),
                end_block: Some(50),
                force_reanalyze: false,
                query_limit: None,
            },
        )
        .await
        .unwrap();

    tracking_service
        .enable_tracking(cluster.cluster_id)
        .await
        .unwrap();

    tx_source.update_last_indexed_block(62);
    tx_source.insert_refs(
        "0x1111111111111111111111111111111111111111",
        vec![
            AddressTxRef {
                address: "0x1111111111111111111111111111111111111111".to_owned(),
                tx_hash: "0xwatch-a".to_owned(),
                block_number: 40,
                tx_index: 0,
                direction: AddressHitDirection::From,
                from_address: None,
                to_address: None,
            },
            AddressTxRef {
                address: "0x1111111111111111111111111111111111111111".to_owned(),
                tx_hash: "0xwatch-b".to_owned(),
                block_number: 61,
                tx_index: 0,
                direction: AddressHitDirection::From,
                from_address: None,
                to_address: None,
            },
        ],
    );

    let enqueued = backfill_service.enqueue_incremental_runs().await.unwrap();
    assert_eq!(enqueued, 1);

    let runs = backfill_service
        .list_cluster_backfill_runs(cluster.cluster_id)
        .await
        .unwrap();
    assert_eq!(runs.len(), 2);
    assert_eq!(runs[0].status, ClusterBackfillStatus::Pending);
    assert_eq!(runs[0].start_block, Some(47));
    assert_eq!(runs[0].end_block, Some(62));
}

#[tokio::test]
async fn tracking_state_and_classified_transactions_are_written() {
    let repository = Arc::new(InMemoryAnalysisRepository::default());
    let fetcher = TransactionBundleFetcher::new(MockRpcClient::default().boxed(), true);
    let mut registry = RecognizerRegistry::new();
    let plugin_context = load_plugins(&["sample_builtin_pack".to_owned()], &mut registry);
    let analysis_service = Arc::new(AnalysisService::new(
        fetcher,
        registry,
        plugin_context,
        repository.clone(),
        vec!["selector_presence".to_owned()],
    ));
    let backfill_service = ClusterBackfillService::new(
        Arc::clone(&analysis_service),
        Some(Arc::new(MockClusterTxSource {
            last_indexed_block: 55,
            refs_by_address: HashMap::from([(
                "0x1111111111111111111111111111111111111111".to_owned(),
                vec![AddressTxRef {
                    address: "0x1111111111111111111111111111111111111111".to_owned(),
                    tx_hash: "0xtracked-a".to_owned(),
                    block_number: 42,
                    tx_index: 0,
                    direction: AddressHitDirection::From,
                    from_address: None,
                    to_address: None,
                }],
            )]),
        })),
        100,
        8,
    );
    let cluster_service = ClusterService::new(repository.clone());
    let tracking_service = ClusterTrackingService::new(repository.clone());

    let cluster = cluster_service
        .create_cluster(
            8453,
            "ops_team",
            "tracked_cluster",
            None,
            vec![ClusterAddress {
                address: "0x1111111111111111111111111111111111111111".to_owned(),
                label: Some("bot".to_owned()),
                role: Some("bot".to_owned()),
                metadata: json!({}),
            }],
        )
        .await
        .unwrap();

    let tracking = tracking_service
        .enable_tracking(cluster.cluster_id)
        .await
        .unwrap();
    assert_eq!(tracking.status, ClusterTrackingStatus::Active);

    backfill_service
        .backfill_cluster(
            cluster.cluster_id,
            ClusterBackfillRequest {
                start_block: Some(0),
                end_block: Some(55),
                force_reanalyze: false,
                query_limit: Some(100),
            },
        )
        .await
        .unwrap();

    let tracking = tracking_service
        .get_tracking_state(cluster.cluster_id)
        .await
        .unwrap();
    assert_eq!(tracking.last_processed_block, Some(55));
    assert!(tracking.last_processed_at.is_some());

    let classified = tracking_service
        .list_recent_classified_transactions(cluster.cluster_id, 10)
        .await
        .unwrap();
    assert_eq!(classified.len(), 1);
    assert_eq!(classified[0].tx_hash, "0xtracked-a");
    assert_eq!(classified[0].behavior_tag, BehaviorTag::Unknown);
}
