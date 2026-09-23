# research_pipeline 现行文档

本索引只列当前主链的操作资料。未列出的 Markdown 可以作为历史或专项研究材料保留，但不构成当前命令、能力或准入依据。

项目扩展的多文件流式 ABI、输出/state writer、`causal_plan` 正式 Feature/Label 用法与
当前不支持的时间来源见[项目扩展合同](../project_extensions/README.md)。

- [首次使用指南](getting-started.md)
- [AI 工作协议](ai_workflow.md)
- [架构与边界](architecture.md)
- [Catalog 与 PIT 准入](catalog.md)
- [列式数据平面](data_plane.md)
- [ResearchPackage](research_package.md)
- [Runtime 与恢复](runtime.md)
- [Result 与 VerificationResult](evidence.md)
- [命令行](cli.md)
- [运维](operations.md)
- [安装与发布构建](release.md)
- [分钟参考规则来源边界](minute_rule_provenance.md)

仓库根目录的 [架构边界](../ARCHITECTURE.md)、[项目扩展](../EXTENSIONS.md)、
[参与贡献](../CONTRIBUTING.md)、[安全报告](../SECURITY.md)、
[第三方材料边界](../THIRD_PARTY_NOTICES.md) 和 [Apache License 2.0](../LICENSE)
共同定义公开项目的代码、协作与分发边界。

## 当前流程

```text
capabilities / operator / artifact / recipe / catalog 发现
  → package init
  → package lint
  → package admit
  → run / inspect / resume / retry-node / rerun-from
  → Result
  → verify
  → VerificationResult
  → report / compare / export-result / Dashboard
```

## 能力状态

能力状态以 [`../src/research_pipeline/capabilities.json`](../src/research_pipeline/capabilities.json) 为唯一机器来源。

<!-- CAPABILITIES_TABLE:START -->
| capability | 状态 | 命令 | 证据 / 信任 | 边界说明 |
| --- | --- | --- | --- | --- |
| `catalog.lock` | `local_only` | `catalog` | `local_acceptance` / `local_only` | Catalog 编译、锁定和漂移检查由显式来源文件驱动；公开仓库不附带个人 Catalog 或独立发布验收记录。 |
| `research_package.plan` | `local_only` | `package lint`<br>`package admit` | `local_acceptance` / `local_only` | ResearchPackage 只能声明受控合同，不接受自由 SQL、动态模块或项目 runner。lint 一次返回声明、算子、指标、结果和准入缺口；admit 从显式只读数据源生成漂移/PIT 闭包并在发布/加载时校验准入事实。 |
| `runtime.recovery` | `local_only` | `resume`<br>`retry-node`<br>`inspect`<br>`rerun-from` | `local_acceptance` / `local_only` | resume、retry-node、inspect 和 rerun-from 只消费当前 invocation、计划内算子闭包和已验证 checkpoint；同一次 execute 复用 Supervisor 已冻结的工件验证结果，新的 resume 进程、rerun child 或新 run 必须重新完整验证。多请求 data 节点另以 run 内 partial index 逐项复验已提交 DatasetArtifactRef，只重做失效 request；partial 不进入正式输出。身份变化必须重新 admit 并新建 run。该边界仍为 local_only。 |
| `evidence.consume` | `local_only` | `verify`<br>`report`<br>`export-result`<br>`compare` | `local_acceptance` / `local_only` | verify 直接从自包含 Result 生成结构化 VerificationResult；report、compare、export-result 和 Dashboard 只消费 VerificationResult 与 ResultStore，不依赖 run-root。直接 compare 只比较已验证指标事实并明示未检查 package 合同；package compare 由 delivery 唯一检查 metric/claim 合同。不同 plan 只作说明，口径不一致时整次拒绝且不输出部分 delta。export-result 仅复制并复核已验证 Result，不重新执行研究。该合同尚未通过独立发布验收，因此保持 local_only。 |
| `evidence.validity_recompute` | `local_only` | `verify` | `local_acceptance` / `local_only` | 独立 verify 从已封存的 canonical 六表与 Bar TCA 四表复核金融守恒、费用与 lineage；金融 oracle 使用有界 Arrow 批次和受配额 DuckDB 扫描，篡改或资源不足均阻止生成 VerificationResult。公开源码提供合同测试，不附带个人真实数据 Result 或独立发布验收；能力保持 local_only，不代表策略盈利、实盘成交或可交易性。 |
| `operator_graph.generic_run` | `local_only` | `run`<br>`resume`<br>`retry-node`<br>`inspect` | `local_acceptance` / `local_only` | 正式 run 由 Runtime v2 调度，节点返回按端口索引的 typed refs，checkpoint 绑定全部端口；成功后唯一 finalize 自包含 Result，再由独立 verify 生成 VerificationResult。该边界尚缺独立发布验收，因此仍是 local_only。 |
| `research.walk_forward_model` | `local_only` | `package admit`<br>`run` | `local_acceptance` / `local_only` | 公共模型七阶段保留 purge/embargo、fold 内预处理、validation 选模、test 与唯一候选 locked holdout；输入 Feature/Label 必须由当前研究包提供。split 先检查 Label row group 与时间可见性，再读取开发目标；holdout 打开后失败仍消耗访问资格。本地验证不代表真实策略、可交易性或已发布模型结论。 |
| `minute_line.complete` | `local_only` | `run` | `local_acceptance` / `local_only` | 四类中国市场资产可按当前 Catalog、已完成分钟 bar、PIT 快照与历史规则进入同一研究主链；随包规则仅覆盖文档列明的参考标的及窗口，范围外无默认规则。公开源码提供合同与合成示例，不附带真实分钟数据或研究结果；能力保持 local_only，不代表全历史、全品种或实盘可交易。 |
| `minute_line.real_data_smoke` | `local_only` | `run` | `local_acceptance` / `local_only` | 公开包提供四资产分钟输入和只读 Result/VerificationResult 合同，不附带供应商原始数据或个人真实运行收据。真实数据观察须由使用者在自己的来源、窗口和规则下重新执行并独立验证；当前能力只到 local_only，不宣称供应商历史版本精确重放或分钟交易仿真。 |
| `simulation.bar_tca` | `local_only` | `package admit`<br>`run` | `local_acceptance` / `local_only` | Bar TCA 只消费统一 SimulationResult 的正式订单与成交和账本身份，不二次撮合。分钟路径要求决策时可见基准、已完成执行 bar、可见容量与正式 fill，缺任何必要事实均失败关闭。随包参考规则只有有界中国市场标的窗口；公开源码不包含个人真实研究结果，能力保持 local_only。 |
| `capability.discovery` | `local_only` | `capabilities --format json`<br>`operator list/describe/scaffold/validate/build`<br>`artifact describe`<br>`recipe list/describe/scaffold`<br>`catalog dataset/field search`<br>`package lint` | `local_acceptance` / `local_only` | capabilities 命令逐字段读取本清单；operator、artifact、catalog 和 package lint 的发现结果来自正式 registry、schema、Catalog Lock 或 package compiler。当前没有获准的公共 recipe，使用 package init 创建通用起点；项目完整拓扑由 ResearchPackage 声明。operator scaffold 生成可验证的最小项目算子；正式 Feature/Label 需必填 causal_plan，且来源必须可由已准入请求证明。validate/build 只复验显式源码闭包，不扫描目录或自动安装。 |
| `resource.governance` | `local_only` | `run --resource-state-dir` | `local_acceptance` / `local_only` | 数据节点预算先编译为 DuckDB、batch/writer 与进程余量；当前 provider 支持包络下限为 256 MiB，低于下限在对象统计和扫描前拒绝。完整矩阵消费者用 footer 做数据页前的明显超界拒绝；内部 worker 默认 1 个，显式指定不能超过 CPU 或 max_workers 容量，并共用节点总预算。全新隔离进程中的真实 DuckDB/Parquet/Arrow 探针回归整个进程树 RSS、读取量与 temp 峰值，不把局部公式称为任意 allocator 的硬上界。多个 CLI/worker 的 FIFO 租约与父子令牌仍共用总容量；资源不足不改变样本、频率、参数或 seed。合成探针不进入正式运行校准总体。 |
<!-- CAPABILITIES_TABLE:END -->
