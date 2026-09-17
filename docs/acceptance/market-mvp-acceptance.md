# LedgerFlow Market MVP 验收报告

计划：[`docs/product/market-mvp-plan.md`](../product/market-mvp-plan.md)
分支：`feat/market-mvp`
运行环境：Robinhood Chain 4663，本地 RPC，`ledgerflow-chainactors-v2` 数据库
状态图例：`PASS` 通过 · `PARTIAL` 部分通过（附限制）· `FAIL` 未通过 · `PENDING` 未开始

每条验收项都要附**可复现证据**（SQL、curl、测试名或截图路径）。数值来自实测，不填写预估值。

---

## 总览

| 里程碑 | 状态 | Tag | 完成日期 |
|---|---|---|---|
| M0 基线与工程化 | PARTIAL | `market-mvp-baseline` | 2026-09-17 |
| M1 行情数据底座 | PENDING | | |
| M2 发币 FOMO 前端 | PENDING | | |
| M3 地址 / 集群盈亏 | PENDING | | |
| M4 聪明钱前端 | PENDING | | |
| M5 套利估算 + 前端 | PENDING | | |
| M6 清算索引 + 前端 | PENDING | | |
| M7 毕业后行情 | PENDING | | |
| M8 加固与验收 | PENDING | | |

---

## M0 基线与工程化

| # | 验收项 | 标准 | 状态 | 证据 |
|---|---|---|---|---|
| 0.1 | 现有工作入库 | 191 个未提交文件入库，生成报告目录被忽略 | PASS | commit `9f9841e`，tag `market-mvp-baseline` |
| 0.2 | 独立开发分支 | `feat/market-mvp` 存在且基于基线 | PASS | `git log feat/market-mvp` |
| 0.3 | 基线可编译 | `cargo check` 市场相关二进制通过 | PASS | `cargo check --bin ledgerflow_market_web --bin ledgerflow_protocol_market_projector` |
| 0.4 | 开发计划 / 验收报告 | 文档入库 | PASS | 本文件与计划文档 |
| 0.5 | CLAUDE.md | 仓库导航、命令、约束 | PASS | `CLAUDE.md` |
| 0.6 | 改版前截图 | 5 个页面 1440/400 宽度 | PASS | `docs/product/screenshots/mvp-before-*`（10 张） |
| 0.7 | 开发实例 | 3401 端口运行新代码，3001 不受影响 | PENDING | |

## M1 行情数据底座

| # | 验收项 | 标准 | 状态 | 证据 |
|---|---|---|---|---|
| 1.1 | 迁移 | `0056+` up/down 可执行，旧表不变 | PENDING | |
| 1.2 | USD 价格序列 | WETH、ETH(0x0)、USDG 及 Top10 报价资产最近 24h 每分钟有价格（无成交分钟前值填充，最大陈旧度标注） | PENDING | |
| 1.3 | 仿冒资产隔离 | 同名非规范 USDC/USDG 不参与 USD 换算 | PENDING | 单元测试 + SQL |
| 1.4 | 1m K 线 | 随机 20 个币，K 线成交笔数/买卖量与 trade facts 原始聚合完全一致 | PENDING | 对账 SQL |
| 1.5 | K 线延迟 | 最新 K 线落后最新 trade fact ≤ 60 s（p95） | PENDING | |
| 1.6 | 币统计 | 24h 活跃币 100% 有 `market_token_stats` 行；价格、USD 成交额、市值、涨跌、毕业进度非空率 ≥ 95%（可定价报价资产） | PENDING | |
| 1.7 | 窗口衰减 | 24h 无成交的币 24h 成交额 = 0，不出现在涨幅榜 | PENDING | |
| 1.8 | 回补 | 30 天 V2 成交回补完成，DB 负载可控（投影期间 3001 页面仍 < 1 s） | PENDING | |
| 1.9 | 幂等与重启 | 重启 worker 后 K 线无重复、无缺口 | PENDING | 测试 + SQL |
| 1.10 | 旧快照 | 停止每分钟追加；保留清理生效 | PENDING | |

## M2 发币 FOMO 前端

| # | 验收项 | 标准 | 状态 | 证据 |
|---|---|---|---|---|
| 2.1 | 数字格式 | 无原始长小数；USD 缩写（$1.2K/$3.4M），百分比 1 位小数，涨绿跌红 | PENDING | 截图 |
| 2.2 | 行情条 | ETH 价格、24h 成交额、1h 新币、24h 毕业、今日最大涨幅 | PENDING | |
| 2.3 | 总览榜单 | 热门 / 涨幅 / 新币 / 即将毕业四榜，每榜 ≥ 5 条真实数据 | PENDING | |
| 2.4 | 大单流 / 刚毕业 | 大额买入实时流、最近毕业列表 | PENDING | |
| 2.5 | 发币表格 | 价格、5m/1h/24h 涨跌、成交额、市值、买家、毕业进度条、迷你走势；Tab + 列排序 + 搜索 + 分页 | PENDING | |
| 2.6 | 币详情 | 价格图（1m/5m/1h）、指标、最近成交、大户盈亏、创建者/集群 | PENDING | |
| 2.7 | 自动刷新 | 15 s 局部刷新，无整页闪烁 | PENDING | |
| 2.8 | 性能 | `/`、`/launches` 热缓存 TTFB p95 < 300 ms | PENDING | 压测输出 |
| 2.9 | 中英文 / 移动端 | `lang=zh/en`；400px 无横向滚动（表格容器除外） | PENDING | 截图 |

## M3 地址 / 集群盈亏

| # | 验收项 | 标准 | 状态 | 证据 |
|---|---|---|---|---|
| 3.1 | 持仓对账 | 随机 20 个钱包×币，买卖数量、成本、收入与原始 facts 手算一致 | PENDING | |
| 3.2 | PnL 口径 | 平均成本法已实现 + 按最新价未实现；手续费计入；gas 不计（标注） | PENDING | 单元测试 |
| 3.3 | 钱包榜 | 24h/7d/30d × 发币领域，Top 100 有 PnL、胜率、最佳一笔 | PENDING | |
| 3.4 | 集群榜 | 集群 PnL = 成员 PnL 之和（去重）；质量过滤后排序 | PENDING | |
| 3.5 | 新发现集群 | 按首次发现时间列出，附当前收益 | PENDING | |

## M4 聪明钱前端

| # | 验收项 | 标准 | 状态 | 证据 |
|---|---|---|---|---|
| 4.1 | 领奖台 | 前三名卡片：PnL、胜率、最佳一笔倍数 | PENDING | |
| 4.2 | 排行榜 | 领域 Tab × 窗口，排名、地址/集群、PnL、交易数、胜率、最近活跃 | PENDING | |
| 4.3 | 新集群流 | 三领域新集群 + 收益 | PENDING | |
| 4.4 | 地址详情 | PnL 汇总、持仓、交易、所属集群 | PENDING | |
| 4.5 | 集群详情 | 成员贡献排行、收益、证据交易 | PENDING | |
| 4.6 | 标签 | 所有金额带 已验证/估算 标签，方法说明可达 | PENDING | |

## M5 套利估算

| # | 验收项 | 标准 | 状态 | 证据 |
|---|---|---|---|---|
| 5.1 | 估算正确性 | 对 468 条已验证套利复算，同 token 净利误差 = 0（或解释差异） | PENDING | 回归测试 |
| 5.2 | 覆盖率 | 最近 24h 候选 ≥ 90% 已估算；USD 可定价率报告 | PENDING | |
| 5.3 | 延迟 / 负载 | 估算落后候选 ≤ 5 min；RPC 经 gateway，无生产错误增加 | PENDING | |
| 5.4 | 套利页 | 统计卡、机器人排行、最大单笔、实时流、已验证单独标识 | PENDING | |

## M6 清算

| # | 验收项 | 标准 | 状态 | 证据 |
|---|---|---|---|---|
| 6.1 | 合约校验 | 仅 `eth_getCode` + `idToMarketParams` 校验通过的合约入库 | PENDING | |
| 6.2 | 历史完整 | 全历史 Liquidate 事件数与独立 `eth_getLogs` 扫描一致 | PENDING | |
| 6.3 | 解码 | 偿还/没收/坏账数量与原始日志一致（测试夹具） | PENDING | |
| 6.4 | 清算页 | 统计、清算人排行、最新清算、最大奖励 | PENDING | |

## M7 毕业后行情

| # | 验收项 | 标准 | 状态 | 证据 |
|---|---|---|---|---|
| 7.1 | PoolId | ≥ 95% 已毕业币推导出 PoolId，与 PoolManager `Initialize` 事件一致 | PENDING | |
| 7.2 | 池成交 | 毕业后 Swap 进入 K 线；价格在毕业点连续（偏差 < 5%） | PENDING | |
| 7.3 | 已毕业 Tab | 毕业后涨跌、成交额可见 | PENDING | |

## M8 加固与最终验收

| # | 验收项 | 标准 | 状态 | 证据 |
|---|---|---|---|---|
| 8.1 | 测试 | `cargo fmt --check`、`cargo clippy`、`cargo test --all-targets` 通过 | PENDING | |
| 8.2 | 24h 浸泡 | 所有新 worker 无崩溃；延迟指标在阈值内；DB 增长可控 | PENDING | |
| 8.3 | 页面性能 | 全部页面热缓存 p95 < 300 ms，冷 < 1.5 s | PENDING | |
| 8.4 | 生产切换 | 经确认后 3001 切换到新版本；可回滚到 `market-mvp-baseline` 镜像 | PENDING | |
| 8.5 | 最终截图 | 改版前后对比 | PENDING | |

---

## 已知限制（持续更新）

- Pons V1 已于 2026-08-12 停止发币，其池内成交不在 MVP 范围。
- 钱包 PnL 仅覆盖 Pons 曲线（及 M7 后的毕业池）成交，不含钱包间转账与其他 DEX；标注为估算。
- 报价资产 USD 价格来自本地链上成交推导，流动性极低的报价资产可能无价格，此时显示“未知”，不计入 USD 汇总。
