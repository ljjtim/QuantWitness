# QuantWitness

QuantWitness 是面向个人研究者和 AI 协作者的量化研究编排框架，Python import 名保留为 `research_pipeline`。它强调 PIT、防前视、可复现、typed DAG、不可变 Result、独立 VerificationResult 和项目扩展隔离；它不是交易执行平台，也不承诺任意研究都能直接运行。

安装发行包后使用 `quantwitness` 命令；源码运行仍可使用 `python -m research_pipeline`：

```powershell
python -m pip install quantwitness
quantwitness --help
python -m research_pipeline --help
```

框架把研究声明、只读数据准入、算子运行、结果封存和独立验证连成一条流程；研究项目不携带任意 Python runner，也不从旧计划或旧证据恢复。

项目扩展必须是可信本地代码：框架核验正式输入输出和工件内容，并提供worker超时及进程清理；它不是安全沙箱，不拦截任意Python在staging外的副作用。数据库只读仍须遵守。

分钟金融独立验证将历史关联和分组交给受配额DuckDB，Python只保留当前金融状态。实际支持规模以验证记录为准，不承诺任意数据规模的绝对内存上界。

算子实现身份按共同变化的语义族及明确共享依赖计算，避免无关适配器修改使数值节点缓存失效；严格字节模式保留整个构建一致的要求。恢复边界见 [Runtime 与恢复](docs/runtime.md)。

## 唯一闭环

安装要求 Python 3.10 或更新版本；开发使用 `python -m pip install ".[dev]"`。正式构建只从 `pyproject.toml` 生成元数据，命令与最低版本验收见 [安装与发布构建](docs/release.md)。

```text
研究问题与口径冻结
  → capabilities / operator / artifact / recipe / catalog 发现
  → package init
  → package lint
  → package admit
  → run / inspect / resume / retry-node / rerun-from
  → Result
  → verify
  → VerificationResult
  → report / compare / export-result / Dashboard
```

最短命令示例：

```powershell
python -m research_pipeline catalog dataset search daily --catalog-lock <持久Catalog-Lock目录> --format json
python -m research_pipeline package init work/my_research --json
python -m research_pipeline package lint --package work/my_research --catalog-lock <搜索结果.catalog_context.lock_reference> --json
python -m research_pipeline package admit --package work/my_research --catalog-lock <搜索结果.catalog_context.lock_reference> --data-db <source:prod只读DuckDB> --output work/plan --json
python -m research_pipeline run --plan work/plan --data-db <只读DuckDB> --artifact-root work/artifacts --handoff-out work/handoff.json --run-root work/run --result-store work/results --clock 2026-01-01T00:00:00+08:00 --root-seed 0 --json
python -m research_pipeline verify --result <Result目录> --result-store work/results --output work/verification-result.json --json
python -m research_pipeline report --verification-result work/verification-result.json --result-store work/results --json
```

`package init` 只创建未填写研究事实的四份声明草稿。先明确来源、市场适配、指标、数据请求、研究时点和算子图；草稿 `lint` 会指出当前阻断的字段，未补齐前不能 `admit`。上述流程中的后续步骤以填写完整且通过严格校验的包为前提。

`catalog dataset/field search` 的机器结果会返回本次实际加载的
`catalog_context.lock_reference`、Catalog 身份和 source profile/environment；dataset 命中项还会
返回当前 binding。直接复用结果中的 `next_commands`，为每个 profile 填入显式只读路径；框架
不会猜测本机数据库位置。公开发行包不附带供应商或个人数据库的 Catalog；使用者通过
`catalog validate/compile` 生成自己的持久 Catalog Lock，并在搜索、scaffold、lint 和 admit
之间复用。

直接采集的 raw 分钟数据不要求调用方补造供应商历史版本清单。`package admit` 校验
completed bar、Catalog 的 `raw + collected + pit_allowed` 语义、实际 schema、有界时区范围和
稳定排序；Runtime 记录本次实际来源 revision 与读取内容身份。复权分钟和历史截面状态仍按
各自 PIT 快照失败关闭。分钟 `AUDIT` 只能观察，不能流入正式特征、标签、统计或仿真。

正式 Feature/Label 不能只靠整体时间声明。每行 Feature 必须带
`decision_time`、`max_source_observation_time`、`max_source_available_time` 和
`source_partition_ids`；每行 Label 必须带 `decision_time`、
`first_actual_observation_time`、`last_actual_observation_time` 和 `available_time`。
核心从实际输入生成这些时间事实，项目扩展不能自行填写。Label 的整体窗口起点可以等于
决策时点，但第一条实际观测必须严格晚于决策时点。

独立 `verify` 对每张正式 Feature/Label 的全部分区执行完整行键外部排序，分批检出重复，
不会把所有键保存在 Python 集合中，也不相信输入已全局有序。它与金融复核依次使用
`--verification-memory-bytes`、`--verification-temp-bytes` 和
`--verification-scratch-root`；重复、空键或资源不足都会阻止生成 VerificationResult。

日期型 QueryIR `as_of=YYYY-MM-DD` 按 `fixed_clock` 自身时区解释为该本地日的排他结束，
即次日 `00:00`；分钟型 `as_of` 保留带时区的绝对时点语义。`package admit` 会把
`available.daily.v1=next_session_open` 的规则写入已准入计划；项目日频算法仍须按真实交易日
序列计算可见时点。列式数据平面不声称对日频行另做逐行 PIT 过滤。

期货日线和结算的 `session_close/completed_session_close` 由同一个 temporal compiler 处理。
`package admit` 必须按 availability policy、合约和请求日期，从供应商时段来源、期货开市日
和合约映射三者交集中唯一选择 policy，编译逐合约、逐交易日的完成时刻，并把每段 policy ID、
revision、hash、时区和覆盖写入计划；provider 与项目期货算法只消费这些计划事实，不自行拼接
固定收盘时间。同一 bundle 可以发布多个互不冲突的合约或规则时段；缺覆盖、同日重复匹配和
revision 漂移都在准入或计划复核时失败。当前正式发布范围仅为 `AG2406.XSGE` 的
2024-01-03—04；其他合约或日期因缺少同源期货日历证据而拒绝准入。
期货分钟分区还绑定当前发布的 session bundle 身份；项目 Worker 不接受缺失或不同身份的
分区规则。其他规则 bundle 尚未进入当前平台能力清单，不能用替换本地文件的方式扩大覆盖。

实际参数以 `python -m research_pipeline <命令> --help` 为准。

## 公开项目资料

- [架构边界](ARCHITECTURE.md)
- [项目扩展](EXTENSIONS.md)
- [参与贡献](CONTRIBUTING.md)
- [安全报告](SECURITY.md)
- [第三方材料边界](THIRD_PARTY_NOTICES.md)
- [Apache License 2.0](LICENSE)
- [版权与随附声明](NOTICE)
- [完整文档索引](docs/index.md)
- [示例目录](examples/README.md)

## 创建研究与新增算子

- 当前没有获准的公共 Recipe；用 `package init` 创建中性 operator graph 声明草稿，完整图、请求、指标、来源和结论上限均由项目填写。完整项目用法见独立的 `examples/`；项目示例不随 `package init` 自动生成。
- 完整拓扑、资产画像、模型、窗口与结果表由项目的 ResearchPackage 和扩展维护，
  不作为公共 Recipe 或公共 Operator 注册；空 Recipe 发现不影响通用包创建和准入。
- 注册表已有同语义算子时直接复用；参数不同不是复制算法的理由。
- 注册表缺少算法时，先用 `operator scaffold` 生成由当前声明 schema 驱动的最小项目 extension，再用 `operator validate/build` 复验并创建 bundle。bundle 交给 `package lint/admit` 后进入计划闭包，运行和恢复不再重复传路径。
- 项目 extension 可声明必填 JSON 参数 `causal_plan`，输出正式 `research.feature-set.v1` 或 `research.label.v1`：package 冻结完整键、来源、决策时点和窗口；核心受限 iterator 记录实际交付数据并合并继承状态来源，再为扩展的键和值附加时间事实。缺少这一合同仍在 validate/build 拒绝。当前仅支持能由已准入请求证明时间角色的输入，逐决策 revision/interval、日频 next-session-open 和 session-close 输入仍明确拒绝。用法见 `project_extensions/README.md`。
- 项目算子只有在两个异构真实项目已复用，并具备独立 oracle、前视/标签/边界攻击测试且不含项目私有语义时，才可人工评估晋级 core。当前不自动评分、不自动晋级。
- `operator list/describe` 的机器发现合同为 `research-machine-discovery-v2`，会标明 `builtin`、`legacy_exit` 或 `promoted`。普通发现只核对 Runtime manifest 的分类、identity 与定义冻结值，不读取私有项目或晋级测试目录。人工晋级前，外层专用审查显式接收两个真实项目目录与测试证据根，核对目标算子的实际消费、可编译性、异构算子图以及不同的 oracle/攻击 pytest 节点，并执行这些节点。审查者还要线下核实项目真实性与证据含义；示例或临时合成包不是实际晋级证据。刷新 capability/remediation baseline 不能代替晋级。
- framework boundary 扫描完整生产 core 并识别业务节点读取全图、按兄弟节点或 dataset 猜输入、直接下传全体已准入计划和固定项目身份分支。业务节点只能从当前参数的 request 绑定投影 admitted plan；完整请求集合由列式物化调度器负责。`capability_containment_gate.py --mode snapshot` 在有批准晋级记录时要求显式复用项目目录与证据根，随后执行晋级审查、结构门和 node-local 变形测试；普通 `check` 不追溯私有审查输入。不接受将失败状态同步冻结为新 baseline。
- 当前公共 manifest 有 24 个已审查的 `builtin` 定义；项目算子只通过本次 Package 的扩展 bundle 准入，不进入公共注册表。新增公共算子必须通过正式晋级证据，不能借刷新 baseline 放行。

模板只影响新生成的 package。已经生成的 package 不自动迁移；上述合同变化后，
应使用项目自有模板或手工修改声明，重新 `package lint`、`package admit` 并新建 run。
FeatureSetArtifact、LabelArtifact 以及 Runtime 的旧 v1 合同不再兼容；
`research-runtime-checkpoint-v1/v2` 也不能恢复。受影响研究必须重新 admit 并新建 run，不能
转换或手改旧 plan/checkpoint。

## 每一步校验由谁消费

- `package lint`：由 AI 或操作者消费；一次返回 package、字段、算子、指标、ResultSpec、已声明资源预算和准入缺口。它不授予运行资格。
- `package admit`：由 `run` 消费；自动观察显式只读数据源的 Catalog 漂移和 PIT 合同，生成不可变计划并在发布/加载时校验准入事实。
- Runtime 校验：由 `resume`、`retry-node`、`rerun-from` 和 ResultAssembler 消费；身份、clock、seed、实现或 checkpoint 漂移时拒绝复用。节点间只用已验证的 typed ExternalArtifact 交接；同一次 execute 由 Supervisor 完整验证一次并复用冻结文件列表，新的 run/resume 进程重新验证。ResultAssembler 从 Runtime 输出索引汇总数据引用，不读取 artifact/handoff 阶段旁路。
- 多请求 data 节点恢复：每个已提交 DatasetArtifactRef 按 request 原子记录在 run 内 partial index；retry/resume 逐项复验并只重做失效 request。partial 不进入最终 ExternalArtifact、checkpoint 或 Result。
- Data Plane 资源准入：QueryIR 的 `max_rows/max_bytes` 只限制输出，不再冒充进程内存上界。admit 把数据节点预算编译为 DuckDB、batch/writer 和进程余量三部分；当前已校准的 provider 进程树支持包络下限是 256 MiB，低于下限会在对象统计和扫描前拒绝。正式 provider 仍设置 DuckDB memory/temp、Arrow batch 和 CPU；Runtime 现有 ResourceObservation 记录实际进程树 RSS 与 scratch 峰值。该包络由全新进程中的真实 DuckDB/Parquet/Arrow 代表查询回归，不承诺任意第三方 allocator 的数学硬上界。
- 下游分区消费：完整矩阵消费者先读 Parquet footer，明显超预算时在数据页读取前拒绝；项目 Worker 在本次节点资源预算内自行管理并行。内部 worker 默认 1 个，显式指定不能超过 CPU 或 `max_workers` 容量。
- `inspect` 恢复诊断：只读重放事件并复用正式 checkpoint 校验，返回每个节点的最后错误、尝试余额、checkpoint 拒绝原因、独立 finalize 状态和唯一下一条命令。旧 run 缺 finalize 投影时明确显示 `unknown`，不根据 Runtime succeeded 猜 Result 已发布。
- Result 完整性：由 `verify` 消费；表字节、schema、行数、lineage、指标可达性和验证闭包不一致时不生成 VerificationResult。金融 oracle 以稳定 Arrow 批次和受 memory/temp 配额约束的只读 DuckDB 扫描 canonical/TCA；不再因 Result 超过固定 512 MiB 而直接拒绝，也不会把完整表转成 Python 行。资源不足会使整次验证失败，不会跳过金融复核。
- 五类 validity：由 `verify` 重算；`report`、`compare`、`export-result` 可消费 `status=fail` 结果用于诊断，Dashboard 只接收 `status=pass`。`export-result` 只复制并复核已验证 Result，不重新执行研究。按研究和 claim 适用，确实不适用时记录稳定 `N/A` 原因。
- 跨方案比较：直接 `compare` 只比较已经验证的指标单位、方向、窗口、样本量、状态和实际 claim
  事实，返回 `verified_metric_facts_only` 并明确未检查 package 合同；完整研究语义比较使用
  `package compare`，由 `packages.delivery` 唯一检查两侧 metric/claim 合同。不同 plan 只作说明，
  任一必要事实不一致时整次拒绝且不输出部分 delta。项目指标只从各自 Result 冻结的
  Verifier 定义读取；两侧同名指标的定义摘要不同则不可比较，不进入公共 Metric discovery。
- ETF 日频金融上下文：由 ResultAssembler 封存到 Result，再由 `verify` 独立复算交易单位、
  T+0/T+1、费用和持仓结算桶；Runtime 自报的 profile、hash 或 pass 状态不能替代复核。

没有消费者、失败后也不会改变动作的检查不进入当前主链。

## 数据与金融边界

- 研究数据库始终只读；框架不会发布或改写数据库。
- 财报按公告/可见日，成分、行业、ST、停牌、复权和市场规则按历史快照或有效区间对齐。
- 盘中研究只使用当时已完成的 bar；标签不能成为特征祖先。正式日频 Label 按
  `label_end_time + horizon_sessions` 写成同质 Parquet row group，模型 split 在首次扫描目标列前
  读取 footer 验证该边界；逻辑过滤不能替代物理边界。holdout 只支持“冻结唯一候选后确认”
  和“冻结完整候选族及校正规则后一次性确认”两种模式。无值预检失败不消耗资格；在返回第一批
  holdout 值前必须原子记录 `opened`，此后读取或计算失败仍永久消耗。完整候选族无论成功失败
  都会 `retired`；没有实际参与预声明选择规则的区间统一称为 `diagnostic`。
- Runtime 成功不等于研究结论可信，正式消费必须同时拥有 Result 与通过的 VerificationResult。
- 模型候选只有显式 `CandidateFitRejected` 这类稳定领域拒绝可以记入 TrialLedger 后继续；
  `NameError`、`KeyError`、依赖 API 变化和其他未知错误必须使 fit 节点失败，不能缩小冻结候选族后仍发布。
- 真实研究归档也必须同时保留 ResultStore 中的结果本体和对应 VerificationResult；验收摘要、报告文本或任务状态都不能替代二者。当前 `gc` 只处理 `staging/cache`，不自动判断或删除 ResultStore 与完成态 run-root。
- 研究主链只读消费已正式发布的因子，不导入因子 Publisher，也不承担因子发布或数据库写入；发布由项目侧独立授权执行。
- 框架保留项目无关的 Catalog/PIT、typed ports、Runtime、Result/VerificationResult envelope 和已准入的通用因子原语；具体模型、关系表组合和研究画像归项目侧，不能作为公共内建能力调用。

- 项目 Verifier 与 Operator bundle 分开冻结身份、版本、源码摘要和授权输入。`package lint/admit --verifier-bundle` 冻结身份，`verify --verifier-bundle` 显式提供同一 bundle；Verifier 只能读取 Result 已封存且显式授权的表和支持工件，不能读取完整 ResultStore、运行目录或未授权兄弟工件；项目结论以统一 VerificationResult envelope 返回。
- 项目源码、ResearchPackage、因子定义和历史验收材料保留在项目侧，不进入公开 core 包。
- ResearchPackage 不会根据出现的日频、模型、因子或事件算子自动补齐或强制完整整图。
  当前只执行 typed ports、DAG、Feature/Label 祖先、ResearchSemantics、时间、PIT、seed、
  资源和金融规则等通用不变量。由于没有满足两个异构正式项目复用条件的完整流程，当前不提供
  公共 workflow profile 字段或注册层；单项目完整流程由自身 package/extension 维护。
- 因子准入使用`AdmittedQueryPlan v5`绑定当前published publication、validated计算运行、
  Catalog正文和请求表`factor_storage_state`。准入与物化分别在共享读租约内复验；物化租约
  一直持有到不可变工件提交和最终引用检查结束。旧v4计划须重新admit，新环境不解释旧计划。
  已提交的自包含Result不因计划版本升级被改写。实际计算实现从数据库`manifest_hash`认证的
  发布审计链读取`compute_code_hash`，不把重建启动前的`code_commit`冒充最终实现。

## 能力状态

能力状态只以 [`src/research_pipeline/capabilities.json`](src/research_pipeline/capabilities.json) 为机器真相源。`planned` 只能发现，`local_only` 不能写成已经独立发布验收。

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

## 文档

- [首次使用](docs/getting-started.md)
- [AI 工作协议](docs/ai_workflow.md)
- [ResearchPackage](docs/research_package.md)
- [Runtime 与恢复](docs/runtime.md)
- [Result 与 VerificationResult](docs/evidence.md)
- [命令行](docs/cli.md)
- [运维](docs/operations.md)
- [现行文档索引](docs/index.md)

历史验收和退役设计可以留在仓库中取证，但不属于当前操作入口。
