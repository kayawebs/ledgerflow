# 新机器部署手册

日期：2026-09-20 · 适用：LedgerFlow 市场情报栈（`ledgerflow-chainactors-v2`）

这份文档是为了让换机器这件事不用重新摸索一遍。所有数字都是 2026-09-20 在旧机器上实测的。

---

## 1. 为什么要换

旧机器（32 核 / 188 GB / 14 TB NVMe）同时跑着链节点 `robinhood-nitro`、`robinhood-arb` 栈和这套分析系统。分析栈的回填任务把负载从 **6.3 推到 20 以上**，挤到了链节点。

停掉分析栈后负载回到 6.3，对照如下：

| 服务 | CPU | 内存 |
|---|---|---|
| `robinhood-nitro`（链节点） | 263% | 22.0 GB |
| `robinhood-arb-postgres` | 307% | 23.5 GB |
| `robinhood-arb-redis` | 6% | 2.0 GB |
| **分析栈（合计）** | **约 14–16 的负载贡献** | 见下 |

**核心结论：链节点需要稳定的磁盘 IO，分析负载是突发且重 IO 的，两者不能共享同一块盘。**

---

## 2. 实测数据足迹

| 项 | 实测 |
|---|---|
| ClickHouse 数据卷 | **823 GB** |
| PostgreSQL 数据卷 | **732 GB** |
| 合计 | **1.55 TB** |
| `market-web` 内存 | 220–440 MB |
| `market-projector` 内存 | 37 MB（重负载在它驱动的数据库查询上） |
| 链出块速度 | **9.9 块/秒** |
| 每日池内成交 | 约 849,000 |
| 每日曲线成交 | 约 610,000 |
| 每日套利候选 | 约 240,000（全覆盖口径） |

内存大头不在应用进程，**在数据库**。停机前操作系统页缓存占用 143 GB。

---

## 3. 推荐配置

### 方案 A：两台（推荐）

**机器 1 — 链节点独占**：16 核 / 64 GB / NVMe 2 TB，只跑 `robinhood-nitro`。

**机器 2 — 分析栈**（ClickHouse + PostgreSQL + 应用）：

| 资源 | 最低 | 推荐 |
|---|---|---|
| CPU | 16 核 | **24 核** |
| 内存 | 128 GB | **192 GB** |
| 磁盘 | 2.5 TB NVMe | **4 TB NVMe** |

磁盘必须是 NVMe——Postgres 的瓶颈是 IOPS 不是容量。ClickHouse 合并时需要等于最大分区的空闲空间，所以容量要留够。

### 方案 B：三台（彻底隔离）

节点 / ClickHouse（16 核·64 GB·2 TB）/ PostgreSQL + 应用（16 核·128 GB·2 TB）。好处是 ClickHouse 的突发查询（实测峰值 **432% CPU**）不会影响 Postgres。

### 方案 C：仍然单机（不推荐）

至少 48 核 / 256 GB / **两块独立 NVMe**（节点数据与分析数据分盘），并保留下面的容器限制。

---

## 4. 必须修正的配置

### PostgreSQL

旧机器上 `shared_buffers` 一直是默认的 **128 MB**，而数据库有 732 GB。这意味着 Postgres 几乎不自己缓存，全靠操作系统页缓存，是 IO 放大的主要来源之一。新机器请直接设：

```
shared_buffers = 32GB          # 192GB 内存时；128GB 内存时设 24GB
effective_cache_size = 96GB    # 约为内存的一半
work_mem = 16MB
maintenance_work_mem = 512MB
random_page_cost = 1.1         # NVMe
max_connections = 200
```

高频更新的表（`market.token_stats`、`market.wallet_positions`）需要更积极的 autovacuum 阈值。

### ClickHouse

按默认配置起即可，但要确认 `max_query_size` 至少为默认的 256 KiB（候选扫描器会构造较大的 `IN` 列表，已按 2,000 条分块，留有余量）。

---

## 5. 启动顺序

**重要：不要一次性全开。** 停机期间链上数据没有入库，重启后会有一段追赶期，追赶本身就是高负载。

```bash
cd /path/to/ledgerflow-chainactors-intelligence-v2
export COMPOSE_PROJECT=ledgerflow-chainactors-v2   # 项目名必须一致，否则会建出平行的一套容器

# 1. 数据层
docker compose -p $COMPOSE_PROJECT up -d db clickhouse
# 等 db 健康检查通过

# 2. 链索引器（先让它追上链头，这一步最重）
docker compose -p $COMPOSE_PROJECT up -d rpc-gateway indexer indexer-lazy
# 观察负载，等追赶基本完成再继续

# 3. 市场栈
docker compose -p $COMPOSE_PROJECT up -d market-projector market-web

# 4. 其余服务按需
```

追赶期间用 `/status` 页面和 `market.pipeline_health` 表观察各游标的推进。

---

## 6. 降载开关（2026-09-20 新增）

这些都是为了让这套东西不可能再挤垮邻居。

| 变量 | 默认 | 说明 |
|---|---|---|
| `LEDGERFLOW_MARKET_BACKFILL_ENABLED` | `false` | **回填默认关闭。** 新机器启动时应保持关闭，先让实时投影追上链头，之后再手动打开回填历史 |
| `LEDGERFLOW_LOAD_SOFT_PER_CORE` | `0.7` | 每核负载超过这个值，后台任务开始拉长间隔（最多 8 倍） |
| `LEDGERFLOW_LOAD_HARD_PER_CORE` | `1.1` | 超过这个值，后台任务直接等待（最多 3 分钟后放行一轮，避免完全停滞） |
| `LEDGERFLOW_MARKET_LEADERBOARD_24H_SECONDS` | `300` | 排行榜重建节奏。这条查询实测 **60–112 秒**，是投影器最贵的操作 |
| `LEDGERFLOW_MARKET_LEADERBOARD_7D_SECONDS` | `1800` | |
| `LEDGERFLOW_MARKET_LEADERBOARD_30D_SECONDS` | `7200` | |
| `LEDGERFLOW_MARKET_POOL_TRADER_LIVE_WINDOW_BLOCKS` | `200000` | 实时归属只覆盖链头附近约 5.6 小时；更早的属于历史，只有开回填才走 |
| `LEDGERFLOW_MARKET_PROJECTOR_CPUS` / `_MEMORY` | `6` / `8g` | 容器硬上限 |
| `LEDGERFLOW_MARKET_WEB_CPUS` / `_MEMORY` | `2` / `2g` | 容器硬上限 |

分析栈独占一台机器之后，这些上限可以放宽；仍与别的服务同机时**不要动**。

### 回填的正确做法

```bash
# 确认实时投影已经追上（/status 全绿、各游标滞后 < 60 秒）之后再开
docker compose -p $COMPOSE_PROJECT up -d \
  -e LEDGERFLOW_MARKET_BACKFILL_ENABLED=true market-projector
# 盯住负载；负载闸门会自动降速，但仍建议在低峰时段做
```

待回填的历史：池内成交交易者归属 **约 3,190 万条未解析**（3,753 万总量中已完成 15.1%）。

---

## 7. 换机器时容易踩的坑

1. **项目名必须是 `ledgerflow-chainactors-v2`。** 在仓库目录直接跑 `docker compose up` 会推断出 `ledgerflow` 这个项目名，建出一套平行容器并在端口上撞车（已经踩过一次）。永远显式带 `-p`。
2. **`.env` 是符号链接**，指向 `/home/ubuntu/ledgerflow/.env`，迁移时要一起带上。
3. **`LEDGERFLOW_MARKET_WEB_BASE_URL` 必须设成真实的对外域名**（`https://ledgerflow.top`）。不设的话社交卡片的图片地址会变成 `127.0.0.1`，所有分享链接都不出图，同时会话 Cookie 也不会带 `Secure`。
4. **迁移数据卷**：`ledgerflow-chainactors-v2_postgres_data`（732 GB）与 `ledgerflow-chainactors-v2_clickhouse_data`（823 GB）。按 1 Gbps 估算，1.55 TB 传输约需 4 小时；能物理搬盘就搬盘。
5. **nginx 与 Cloudflare**：`/etc/nginx/sites-enabled/ledgerflow.conf` 把 `ledgerflow.top` 反代到 `127.0.0.1:3001`，域名在 Cloudflare 后面。换机器时要同步改 nginx 的 upstream 或迁移 nginx 本身。

---

## 8. 迁移后的验收

```bash
curl -s https://ledgerflow.top/health                     # 200
curl -s https://ledgerflow.top/status | grep -c 'fail'    # 期望 0 项异常
```

数据库里直接看：

```sql
select check_key, status, detail from market.pipeline_health where status <> 'ok';
select checkpoint_key, now() - updated_at as stale from market.projector_checkpoints;
```

全部检查为 `ok`、各游标滞后小于一分钟，才算迁移完成。
