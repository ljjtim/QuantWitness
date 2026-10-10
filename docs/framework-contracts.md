# 框架约定：从研究输入到结果交付

这是一份面向进阶使用者和扩展开发者的完整参考。它说明数据、算法、运行和结果之间必须满足的约定；第一次运行不必逐条读完，可以先看 [文档导航](index.md)。

关键原则只有三个：按当时可见的数据研究，按已经固定的配置执行，让结果保留来源并接受独立检查。

## 参数与行为说明

QuantWitness 是面向个人研究者和 AI 协作者的量化研究编排框架，Python import 名保留为 `research_pipeline`。它强调 PIT、防前视、可复现、typed DAG、不可变 Result、独立 VerificationResult 和项目扩展隔离；它不是交易执行平台，也不承诺任意研究都能直接运行。

安装发行包后使用 `quantwitness` 命令；源码运行仍可使用 `python -m research_pipeline`：

```powershell
python -m pip install quantwitness
quantwitness --help
python -m research_pipeline --help
```

第一次使用建议从 [Workspace 合成研究入门](workspace-quickstart.md) 开始：运行已有股票横截面项目、独立验证、生成报告，再改变样本范围比较两次结果。完整公开源码提供四个自包含项目；仅安装 wheel 不包含 `examples/`，请使用公开源码中的示例。创建自己的研究时再按 [首次使用指南](getting-started.md) 填写 ResearchPackage。

完整示例源码来自 [QuantWitness 公开仓库](https://github.com/ljjtim/QuantWitness)。首次使用可下载仓库的 Code → Download ZIP，或运行：

```powershell
git clone https://github.com/ljjtim/QuantWitness.git
Set-Location QuantWitness
python -m pip install -e ".[dev]" "pyarrow==21.0.0"
```

源码、示例和文档使用同一 checkout；离线试用使用交付的完整公开源码目录，不把仅含核心文件的 `research_pipeline-source.zip` 当作示例包。运行前记录 `git rev-parse HEAD`，离线目录则记录交付清单中的源码提交号。

Workspace 可用 `init --from-package` 从完整研究包初始化，`execute` 顺序委托 lint、admit、run、verify、report，并从包中读取固定时点和种子。Workspace 自动管理运行输出路径，`workspace run` 支持与普通 `run` 相同的 `--reuse-run-root`、`--require-reused-node` 和 `--reuse-failed-run-root`，沿用原有身份校验及恢复边界。执行失败时保留 JSON 诊断，按返回的 `inspect` 命令查看节点状态；恢复成功后使用本次返回的 Result 位置继续验证。

框架把研究声明、只读数据准入、算子运行、结果封存和独立验证连成一条流程；研究项目不携带任意 Python runner，也不从旧计划或旧证据恢复。

项目扩展必须是可信本地代码：框架核验正式输入输出和工件内容，并提供worker超时及进程清理；它不是安全沙箱，不拦截任意Python在staging外的副作用。数据库只读仍须遵守。

分钟金融独立验证将历史关联和分组交给受配额DuckDB，Python只保留当前金融状态。项目 Worker 与 Verifier 的资源预算覆盖 Supervisor、当前 Worker 及其后代；复制准备也计入 Verifier 预算，详见[资源预算](project_resource_budgets.md)。实际支持规模以验证记录为准。`verify --verification-process-slots` 可显式声明独立复核的进程槽；默认 2，Windows 虚拟环境启动器可能需要 3。

滚动模型在每个测试时点只使用已可见的验证结果冻结候选，标签可见时间参与训练准入，详见[滚动模型合同](walk_forward_model.md)。`artifact describe` 返回正式指标、单位、字段及 ResultSpec 目录；Catalog 搜索必须显式提供持久 Lock。

算子实现身份按共同变化的语义族及明确共享依赖计算，避免无关适配器修改使数值节点缓存失效；严格字节模式保留整个构建一致的要求。 依赖身份使用 v2 规范分发记录，排除安装器标记和路径相关入口包装器；旧 v1 身份不自动迁移。恢复边界见 [Runtime 与恢复](runtime.md)。

在聚宽数据库仓库中，`data_factor.duckdb` 默认只读。研究主链不导入 `FactorPublisher`，不会发布或改写数据库；正式因子库仍只能由 `factor_calc.publish` 写入，因子发布必须走独立授权流程。

## 唯一闭环

安装要求 Python 3.10 或更新版本；开发使用 `python -m pip install ".[dev]"`。正式构建只从 `pyproject.toml` 生成元数据，命令与最低版本验收见 [安装与发布构建](release.md)。

sdist 与源码 zip 包含公开操作文档及其本地链接依赖；发布工具、完整发布验收和可执行合成示例请使用独立公开源码仓库。发行产物不包含个人研究源码、真实研究数据或私有 Catalog Lock。

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
  → report / compare / analysis / export-result / Dashboard
```

最短命令示例：

```powershell
python -m research_pipeline catalog dataset search daily --catalog-lock <持久Catalog-Lock目录> --format json
python -m research_pipeline package init work/my_research --json
python -m research_pipeline package expand-variants --base work/my_research --manifest work/variants.yaml --output-root work/variants --json
python -m research_pipeline package lint --package work/my_research --catalog-lock <搜索结果.catalog_context.lock_reference> --json
python -m research_pipeline package admit --package work/my_research --catalog-lock <搜索结果.catalog_context.lock_reference> --data-db <source:prod只读DuckDB> --output work/plan --json
python -m research_pipeline run --plan work/plan --data-db <只读DuckDB> --artifact-root work/artifacts --handoff-out work/handoff.json --run-root work/run --result-store work/results --clock 2026-01-01T00:00:00+08:00 --root-seed 0 --json
python -m research_pipeline verify --result <Result目录> --result-store work/results --output work/verification-result.json --json
python -m research_pipeline report --verification-result work/verification-result.json --result-store work/results --output work/verification-report.md --format markdown --json
```

需要对 Result 中的时间序列表做逐年统计、复合收益或回撤时，先在仓库外编写显式
`AnalysisRequest`，再使用 `analysis run` 生成独立 `AnalysisResult`。框架不会根据列名、单位、
项目或市场猜收益语义；多份结果只通过 `analysis compare` 按显式指标和方向比较。

`package init` 只创建未填写研究事实的四份声明草稿。先明确来源、市场适配、指标、数据请求、研究时点和算子图；草稿 `lint` 聚合独立可判断的字段缺口，未补齐前不能 `admit`。上述流程中的后续步骤以填写完整且通过严格校验的包为前提。

多个研究只差少量既有算子参数时，可以用仓库外变体清单先展开多个完整包；展开器不接受任意 YAML 合并、Python 或 SQL。每个输出包仍须独立 lint、admit、run 和 verify。新 run 可重复提供 `--reuse-run-root <已完成run>`，机会式复用节点身份完全一致、内容复验通过且声明为 `pure/cacheable` 的 checkpoint；未命中时正常执行。若指定节点禁止回退重算，可重复增加 `--require-reused-node <节点>`：Runtime 会在任何 Worker 启动前完整复验指定节点及其必要上游，这些显式节点可以不是 `pure/cacheable`，但任一节点不可复用时整次 run 直接拒绝。修正 ResultSpec 等不改变 DAG 的计划后，也可显式提供 `--reuse-failed-run-root <失败run>`，把该失败运行中已成功的 checkpoint 完整复验并复制到新 run，再从首个未成功节点继续。两种来源不能混用，均不会自动扫描磁盘或把 ResultStore 当作上游缓存。

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

- [架构边界](../ARCHITECTURE.md)
- [项目扩展](../EXTENSIONS.md)
- [参与贡献](../CONTRIBUTING.md)
- [安全报告](../SECURITY.md)
- [第三方材料边界](../THIRD_PARTY_NOTICES.md)
- [Apache License 2.0](../LICENSE)
- [版权与随附声明](../NOTICE)
- [完整文档索引](index.md)
- [示例目录](../examples/README.md)

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

研究复现由项目 Verifier 声明和验证。公共命令不接收独立研究 proof，包声明不含 `reproduction` 开关。变更后的声明与实现须重新准入并产生新运行身份，历史 Result 保持原样。

模板只影响新生成的 package。已经生成的 package 不自动迁移；上述合同变化后，
应使用项目自有模板或手工修改声明，重新 `package lint`、`package admit` 并新建 run。
FeatureSetArtifact、LabelArtifact 以及 Runtime 的旧 v1 合同不再兼容；
`research-runtime-checkpoint-v1/v2` 也不能恢复。受影响研究必须重新 admit 并新建 run，不能
转换或手改旧 plan/checkpoint。

## 每一步校验由谁消费

- `package lint`：由 AI 或操作者消费；一次返回 package、字段、算子、指标、ResultSpec、已声明资源预算和 `required_inputs`。尚缺数据源或输出目录时不生成带占位符的伪命令，也不授予运行资格。
- `package admit`：由 `run` 消费；自动观察显式只读数据源的 Catalog 漂移和 PIT 合同，生成不可变计划并在发布/加载时校验准入事实。
- Runtime 校验：由 `resume`、`retry-node`、`rerun-from`、显式 `run --reuse-run-root`、`run --reuse-failed-run-root` 和 ResultAssembler 消费；身份、clock、seed、实现或 checkpoint 漂移时拒绝复用。普通跨运行来源必须是已经成功发布 Result 的完成态 run，只机会式复用 `pure/cacheable` 节点，未命中时正常执行；同时提供 `--require-reused-node` 时，指定节点及其必要上游可以不是 `pure/cacheable`，但必须在任何 Worker 启动前全部通过节点身份、输入和 checkpoint 内容复验，否则拒绝整次 run。失败 run 来源则必须处于失败终态，且新旧 DAG、节点身份环境、clock 和 seed 完全一致，只导入事件状态为成功且内容复验通过的 checkpoint。命中后以普通字节复制 ExternalArtifact 并提交目标 run 自己的 checkpoint；存在同身份但损坏的候选时失败关闭。节点间只用已验证的 typed ExternalArtifact 交接；同一次 execute 由 Supervisor 完整验证一次并复用冻结文件列表，新的 run/resume 进程重新验证。项目 Worker 失败时保留稳定错误码和脱敏、限长的根异常摘要；ResultSpec 选中的 Parquet 前缀在生产节点提交后立即校验。ResultAssembler 从 Runtime 输出索引汇总数据引用，不读取 artifact/handoff 阶段旁路。
- 项目准入事实：项目 Worker 只能读取节点参数显式绑定的 request。普通列式输入携带已验证数据表及准入元数据；原始 1 分钟扫描输入只投影与当前 admitted plan 对齐的准入元数据，不开放分钟文件。Worker 必须完整消费，Supervisor 复核来源身份、schema 和 metadata-only 轨迹。
- 项目参数 ABI：薄声明可登记 `parameter_preflight: {module, function}`。package 编译会在打开数据库和执行 DAG 前，把真实冻结后的 `ProjectOperatorContext` 交给独立短进程；JSON 对象表现为只读 `Mapping`，列表表现为 `tuple`。预检函数只接收 context，必须返回 `None`。未登记钩子的历史 bundle 仍可读取，但不会获得项目源码级参数解释预检。
- 多请求 data 节点恢复：每个已提交 DatasetArtifactRef 按 request 原子记录在 run 内 partial index；retry/resume 逐项复验并只重做失效 request。partial 不进入最终 ExternalArtifact、checkpoint 或 Result。
- Data Plane 资源准入：QueryIR 的 `max_rows/max_bytes` 只限制输出，不再冒充进程内存上界。admit 把数据节点预算编译为 DuckDB、batch/writer 和进程余量三部分；当前已校准的 provider 进程树支持包络下限是 256 MiB，低于下限会在对象统计和扫描前拒绝。正式 provider 仍设置 DuckDB memory/temp、Arrow batch 和 CPU；Runtime 现有 ResourceObservation 记录实际进程树 RSS 与 scratch 峰值。该包络由全新进程中的真实 DuckDB/Parquet/Arrow 代表查询回归，不承诺任意第三方 allocator 的数学硬上界。
- JSON 数组 VIEW：受支持的“单数组 + 有限分类”形状由 admit 从只读对象定义冻结为结构化执行证据，provider 用投影式 `UNNEST(from_json(...))` 读取原始表，避免 `json_each` 横向连接放大临时工作集。日期、分类、元素输出和 QueryIR 过滤仍按原 VIEW 语义执行；形状识别不完整时失败关闭。
- 下游分区消费：完整矩阵消费者先读 Parquet footer，明显超预算时在数据页读取前拒绝；项目 Worker 在本次节点资源预算内自行管理并行。Operator 资源声明同时包含 CPU 与进程槽；逐分区和 causal Worker 共用节点 wall-time 截止时间，不按调用次数重置。
- `inspect` 恢复诊断：只读重放事件并复用正式 checkpoint 校验，返回依赖等待、资源等待、执行、checkpoint、Result finalize 或终态阶段，以及节点时长、心跳、进程健康、资源申请/租约、最后错误、尝试余额和 checkpoint 拒绝原因。共享资源池读取不会清理或改写治理文件。`run --json` 失败时会直接从同一份正式 diagnostic 事件返回 `failed_node`、`root_error`、`run_root` 和完整 `inspect` 命令，不另写第二套错误日志。完整后续命令同时提供 `next_command` 和 `next_command_argv`；缺少操作者选择时返回 `required_inputs`。旧 run 缺 finalize 投影时明确显示 `unknown`，不根据 Runtime succeeded 猜 Result 已发布。
- Result 完整性：由 `verify` 消费；表字节、schema、行数、lineage、指标可达性和验证闭包不一致时不生成 VerificationResult。新项目 Result 会内嵌 Plan 中已经冻结并复验的 Verifier bundle，可直接 verify；历史项目 Result 仍接受显式 `--verifier-bundle`，不会被迁移或改写。金融 oracle 以稳定 Arrow 批次和受 memory/temp 配额约束的只读 DuckDB 扫描 canonical/TCA；不再因 Result 超过固定 512 MiB 而直接拒绝，也不会把完整表转成 Python 行。资源不足会使整次验证失败，不会跳过金融复核。
- 五类 validity：由 `verify` 重算；统计矩阵默认要求列满秩，列数多于有效观察行的宽矩阵必须由独立项目 Verifier 复算行数、非零有效行数、列数和秩，并达到该矩阵可实现的秩上限。`report`、`compare`、`export-result` 可消费 `status=fail` 结果用于诊断，`analysis` 和 Dashboard 只接收 `status=pass`。`analysis` 只派生结构化分析事实，不提升原有 claim；`export-result` 只复制并复核已验证 Result，不重新执行研究。按研究和 claim 适用，确实不适用时记录稳定 `N/A` 原因。
- 通用 Result 分析：`analysis run` 通过同一 verified context 复核 Result 根身份和所选表文件，只投影请求中的日期列和值列。值语义、频率、期间数、单位、费用口径、缺失值和年度完整性政策都由请求显式声明；公共核心不包含项目、因子、市场、币种、阈值或固定业务列名。`analysis compare` 只消费规范 AnalysisResult；分析规格、实际窗口、列类型或 claim 事实不一致时整次不给排名。
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

- 项目 Verifier 与 Operator bundle 分开冻结身份、版本、源码摘要和授权输入。`package lint/admit --verifier-bundle` 把复验后的 bundle 封入 Plan；新 Result 再把该闭包封入 `verifiers/<bundle_hash>`，所以正式 `verify` 不依赖外部路径。历史 Result 仍可显式提供冻结身份一致的 bundle。一个 Verifier 可以声明供多个项目变体共用的指标定义超集，当前 Package 只把自己 MetricContract 选中的定义编入正式可达性证明，未选指标不会成为正式指标。项目源码闭包只纳入 UTF-8 Python 源码，并忽略解释器生成的 `__pycache__`，使同一源码在导入前后保持相同身份。Verifier 只能读取 Result 已封存且显式授权的表和支持工件，不能读取完整 ResultStore、运行目录或未授权兄弟工件；项目结论以统一 VerificationResult envelope 返回。
- 新建 MetricDefinition 使用 v3，并显式冻结 `quantity`、`numerator`、`denominator`、`observation_timing` 和 `aggregation`。这些测量语义进入定义摘要、Verifier bundle、Result 和比较身份；同名同单位但口径不同的指标不能混比。历史 v2 定义仍可只读加载，框架不会替旧结果猜测缺失口径。
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

能力状态只以 [`src/research_pipeline/capabilities.json`](../src/research_pipeline/capabilities.json) 为机器真相源。`planned` 只能发现，`local_only` 不能写成已经独立发布验收。

完整能力表见[当前能力状态](index.md#精确能力状态)。

## 文档

- [首次使用](getting-started.md)
- [AI 工作协议](ai_workflow.md)
- [ResearchPackage](research_package.md)
- [Runtime 与恢复](runtime.md)
- [Result 与 VerificationResult](evidence.md)
- [命令行](cli.md)
- [运维](operations.md)
- [现行文档索引](index.md)

历史验收和退役设计可以留在仓库中取证，但不属于当前操作入口。

Qlib 模型链采用六节点；模型与处理器是同一候选/fold 的文件工件。ResultSpec 选定模型 inventory 时封存其明确引用文件，恢复不依赖训练工作目录。HTML 研究报告使用 Qlib 诊断算法，并保留来源、方法和验证状态。

## 归档来源身份

封存来源使用 CLI plan v6，冻结完整 input_snapshot_manifest；对应 invocation v14 保存显式
清单路径并在恢复时与计划内容核对。原数据库来源继续使用原合同，历史运行不自动迁移。
归档来源准入证据包含原工件引用和当前归档物理绑定，进入原有 admission/DAG 身份链；
节点缓存不能跨来源变更复用。数据和分钟适配器的源码身份包含 archived_inputs 实现。
