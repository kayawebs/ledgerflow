# Backup and recovery

PostgreSQL is the source of truth for Pilot configuration, events, evidence packs, delivery state and weekly reports. Take a daily logical backup and retain WAL/PITR according to the deployment policy:

```bash
pg_dump --format=custom --file=ledgerflow-$(date -u +%Y%m%d).dump "$DATABASE_URL"
pg_restore --list ledgerflow-YYYYMMDD.dump
```

Restore into a new database, run all migrations, verify `pilot_events` and delivery states, then resume workers from their persisted checkpoints. Never restore over the live database without a tested rollback plan. ClickHouse is rebuildable from PostgreSQL/indexer checkpoints and should be snapshotted separately for reporting workloads.
