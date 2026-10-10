# 读懂结果包和独立检查单

Result 回答“算出了什么、用什么算的”；VerificationResult 回答“按哪些规则检查、发现了什么问题”。报告把两者展示给人看。

运行成功不等于验证通过。先检查验证状态，再阅读数值和覆盖范围。自定义公式还需要对应的独立检查实现，不能靠文件完整性检查证明经济结论正确。

## 参数与行为说明

研究与独立验证的公开产物保持两层；只读消费层可以另外生成独立 AnalysisResult：

- Result：自包含的计算结果、输入闭包、lineage、运行摘要、validity facts 和金融控制材料。
- VerificationResult：独立 verifier 对 Result 完整性、适用门禁和结论上限的结构化判断。
- AnalysisResult：用外部显式 AnalysisRequest 对通过验证的 Result 表做通用时间序列统计；不修改前两层，也不提高 claim。

## Result

ResultAssembler 只选择 ResultSpec 声明的已提交 typed outputs。它不打开数据库、不重算研究、不改变 claim。发布前会重放 Runtime 事件链并核对完整成功终态；事件链留在 run-root 作为具体执行审计，Result 只封存稳定终态摘要，资源排队和 attempt 时序不改变 Result 身份。结果目录包含 `result.json`、`COMMITTED`、公共表和声明的支持文件；同一 run 只能有一个不可覆盖结果。

ResultStore 读取时验证实际文件字节、Parquet schema、行数、路径边界、manifest 和语义身份。同一次操作复用已验证 snapshot，避免重复完整扫描。声明项目 Verifier 的新 Result 还包含 `verifiers/<bundle_hash>`：发布时从已准入 Plan 复制并复验，读取时按 bundle manifest 核对精确源码闭包和冻结身份。历史 Result 不回填该目录，仍通过显式外部 bundle 验证。

凡是正式结论可达的 Feature/Label，ResultSpec 编译器都会自动加入对应逐行因果时间表；只在
diagnostic 分支、且不是正式结论祖先的 Feature/Label 不封存。Feature 表逐行保存
`decision_time`、`max_source_observation_time`、`max_source_available_time`、
`source_partition_ids`；Label 表逐行保存 `decision_time`、
`first_actual_observation_time`、`last_actual_observation_time`、`available_time`。
正式因果输出缺少 Parquet、核心字段、唯一行键或时间关系不成立时，Result finalize 或
`verify` 失败。表身份按来源节点、端口、Artifact 类型和路径共同区分，不能因路径相同误删
另一张正式时间表。

### 序列模型的封存与复核

选中Qlib序列模型清单时，Result收录配置、模型对象、Processor、显式权重，以及模型使用的`sequence/context.parquet`、`targets.parquet`和`members.parquet`。所有文件必须属于该清单引用的已提交工件。三份固定窗口文件可作为模型支持文件，普通Parquet研究表仍使用ResultSpec表声明。

独立复核只读取配置、窗口Parquet和holdout账本，不反序列化模型或重新训练。研究级原始Feature、Label、完整末端、上下文、成员和排除记录必须逐行绑定Result。验证器从原始输入重建窗口资格，检查实际样本的证券、会话、决策时间和目标值，再核对每个模型的历史输入是其train/valid末端的精确子集。GRU配置复核固定Qlib/Torch版本、输入维度、CPU和单条batch、实际参数、窗口声明及negative_mse曲线。

正式ResearchPackage接受显式完整窗口的GRU候选，公开示例的development/model模式同时绑定原始Feature、Label及四张序列表。缺少窗口声明、窗口长度不一致或非raw标签的序列评价在准入阶段拒绝；Result独立验证进一步核对实际样本与封存模型。

## 金融与统计口径

期货账本以精确持仓成本处理增仓、部分平仓、反转和逐日结算，独立金融复核从成交现金流与结算事件重算，不复用账本的均价算法。以乘数 15 先后按 100、110 各买入一手、按 120 结算，累计盈亏为 450。

股票和 ETF 买入遵守整手规则；卖出允许全部余额清仓，或一次卖出全部零股余数，保留整手持仓。110 股可以全部卖出，也可以卖出 10 股后保留 100 股；不能把 10 股余数拆成多次零股委托。容量约束下的实际成交同样遵守该规则。

分钟收益标签按实际价格事件确定经济区间：开盘价对应 `bar_start`，收盘价对应 `bar_end`，午休及跨会话保留真实时间间隔。统计重叠窗口、HAC 与 embargo 使用这个区间；逐行因果时间仍单独验证未来观测及其可见时间。

公共统计模块 DSR 按 Bailey 与 López de Prado（2014）式（2），以零均值调用一般最大值辅助函数：门槛为候选非年化 Sharpe 样本标准差（`ddof=1`）乘以 Euler 常数加权的两个标准正态分位数系数，不添加候选均值。有效试验数 10、标准差 0.1 时门槛约为 0.157459830134575。公共方法身份为 `deflated_sharpe_ratio_v2`，公式身份为 `bailey_lopez_de_prado_2014_eq2`；一般辅助函数保留式（1）的非零均值形式，返回约定仍为 `p_value=1-DSR`。相关矩阵参与率只是有效独立试验数的近似，不能把它当成已识别的真实独立试验数。

论文原例（100 次独立试验、候选年化 Sharpe 方差 1/2、年化 Sharpe 2.5、每年 250 期、1250 个样本、偏度 -3、峰度 10）的非年化门槛为 0.113172001865，DSR 为 0.900396834449。候选族及选中序列必须使用同一观察期内当时可获得的数据，不能用未来候选结果估计门槛。公共接口至少需要两个非常数候选和八个对齐样本，候选 Sharpe 离散度为零时门槛为零。DSR 始终是统计诊断，不证明策略有效。

## 如何判断计算准确性

判断应绑定具体公式、输入口径和市场规则。下面几种证据回答不同的问题：

| 证据 | 能回答的问题 | 仍需单独确认的内容 |
| --- | --- | --- |
| 手算样例、已知数值 | 给定输入是否得到规定结果，例如成交后现金、期货结算盈亏、收益和回撤 | 样例之外的边界条件及市场规则覆盖 |
| 独立金融或统计实现 | 不复用生产算法时，能否按同一合同重建数值 | 合同和公式本身是否适合该研究 |
| 守恒与篡改负例 | 账本失衡、单位错误、时间或内容篡改能否被检出 | 没有违反已检查约束的其他错误 |
| 中断恢复与逐表一致 | 同一研究在恢复后是否保持结果 | 两次计算是否共同采用错误公式 |
| 完整候选统计与封存结果一致 | 指定规模下是否可重复计算 | 统计推断假设、选择偏差处理及策略有效性 |

项目 Verifier 应从封存输入独立重算正式指标，并针对单位、方向、可见时间和关键金融规则设置负例。独立实现不能只调用生产计算函数；两个实现采用同一公式时，还需要手算或原始方法依据确认公式，而不能只凭二者一致判断正确。

### 因子、评价与成交的边界

因子原值、因子评价和交易回测是三件事。IC／Rank IC 的独立复核证明给定 `factor_value` 与收益标签的评价计算，不证明上游每个因子公式。因子算法还需明确窗口、缺失值、排序并列、复权和可见日期，再用小型已知输入验证；无须通过全历史重算来代替公式验证。

账本守恒和费用复核证明声明成交下的财务计算。真实市场是否能按该数量与价格成交，取决于执行价格、容量、停牌、涨跌停、交易时段等模型及其数据覆盖。公开横截面合成示例展示研究闭环，不包含完整实盘成交模型。

公共统计函数的验证范围不自动覆盖项目扩展的同名指标。项目若采用不同候选集合、有效试验数或年化口径，应在指标语义中明确；比较前确认含义一致。验证通过也不提升能力清单的发布状态或证明策略未来盈利。

## verify

```powershell
python -m research_pipeline verify --result <Result目录> --result-store <ResultStore> --output <VerificationResult.json> --verification-memory-bytes <字节> --verification-temp-bytes <字节> --verification-scratch-root <临时目录> --json
```

Verifier 从 ResultStore 的已验证 snapshot 重算：

1. 数据 PIT 与来源修订；
2. 标签和 split 隔离；
3. 统计、多重检验和选择偏差；
4. 稳健性与负控制；
5. 金融可交易性、账本守恒和 Bar TCA。

逐行时间复核不相信 Feature/Label 的整体声明：Feature 的最大源观测时间和最大源可见时间都
不得晚于决策；Label 必须满足
`decision_time < first_actual_observation_time <= last_actual_observation_time <= available_time`。
Label 整体窗口的声明起点允许等于决策时点，但不能用它替代第一条实际观测严格晚于决策的
逐行证据。项目事件窗口等非预测结果由项目 Verifier bundle 复核，不作为 core 的固定 schema 分支。

行键唯一性按同一 manifest 的全部 Parquet 分区检查，不跨不同正式表合并。完整键仍由
`_causal_time_key_columns` 统一选择，包括日频 Feature 的窗口和特征标识、Label 的期限。
verifier 自行执行受预算约束的 DuckDB 全局排序，再以 8,192 行 Arrow 批次比较，跨批只保留
上一键；这个顺序来自本次排序，不依赖 ResultSpec 或 Runtime 的排序声明。重复错误包含
键列和值，空键和资源不足同样阻止生成 VerificationResult，临时文件在成功和失败时清理。
因果排序与金融复核顺序执行，共用现有 `FinancialOracleBudget` 和 verify 的三个资源参数。
DuckDB 临时盘配额必须在连接的临时目录初始化后设置，避免初始化重置配额。

S-04 的固定资源验收为 2,000 万键、4 个 Parquet 分区；独立 probe 测量的是正式唯一性阶段。
完整 ResultStore → verify → VerificationResult 的通过、跨分区重复和资源不足另由集成测试覆盖。

项目 Feature/Label 也走相同 Result 门禁：Runtime 从冻结 causal 工作项实际交付的数据与
继承状态生成核心时间事实，扩展仅返回键和值。独立 verifier 不信任扩展声明，也不因输出
来自项目 bundle 而省略时间不等式或全局行键唯一性检查。

项目专属表集合、数值重算和结论不进入 core 内建 verifier。项目通过独立 Verifier bundle
声明实际读取的 Result 表和支持工件；Package 准入冻结 verifier identity、版本、源码摘要和
授权输入，新 Result 随后内嵌该 Plan 闭包，`verify` 默认从 Result 自身执行，只向 worker 提供
本次 Result 中已封存且显式授权的内容。历史 Result 仍可显式提供身份一致的 bundle。项目 verifier 的
状态、findings 和 outcome hash 进入通用 VerificationResult，executor 自报成功不能替代独立
复核。具体项目是否迁移到当前 Verifier bundle ABI，由项目自行决定，不属于 core 内建能力。

普通统计矩阵继续以列满秩为通过条件。候选数大于有效观察行数时，列满秩在数学上不可达；
这类宽矩阵必须由项目 Verifier 从正式 Result 表独立重建矩阵，并通过
`project-verifier-output-v2` 返回总行数、非零有效行数、列数和实际秩。公共 statistics gate
同时核对 validity facts 中的列数和秩，并要求实际秩等于
`min(列数, 非零有效行数)`。缺少证据、字段不一致或仍存在额外线性退化都会失败关闭；项目
Operator 自报的行数不能替代独立复算。

门禁按 ResearchPackage、算子图和 claim 触发。真正不适用时记录稳定 `N/A` 原因；缺材料、未执行或不认识的原因不能冒充 `N/A`。

金融 verifier 可以共用表 schema，但不复用生产仿真的守恒、摘要或 TCA 计算。当前 v11 以稳定 Arrow 批次读取 canonical/TCA，并把排序、连接和跨行汇总交给受 memory/temp 配额约束的只读 DuckDB；日频 ETF 的费用、T+0/T+1 和持仓桶也走同一受限扫描。它不再使用固定 512 MiB 来源大小门，也不把完整表展开成 Python 行。资源不足会使整次 `verify` 失败且不写 VerificationResult，不会降级或抽样。未提供参数时使用 1 GiB 进程预算和 8 GiB 临时盘预算；进程预算会先扣除当前解释器与 Arrow 批处理余量，再分配给 DuckDB。

固定规模验收 Result 的未压缩列块为 586,133,815 字节、共 285,011 行；全新 verifier 进程会同时记录整个进程树 RSS、实际读取字节、临时盘峰值和耗时。该本地证据说明当前受支持 fixture 能在显式预算内完成，不把它外推为任意 allocator 或任意 Result 形状的数学硬上界。

ETF 日频 Result 还必须封存 `simulation/daily-context.json`。该支持文件闭合受控
`market_rule_profile`、逐标的 rule bundle、用户佣金假设、SimulationResult 和 ledger
身份；缺失时 ResultAssembler 拒绝 finalize。独立 verifier 不读取 run-root 或数据库，直接
从同一已验证 Result snapshot 复验 profile 等于当前受控事实源，并重算交易单位、逐 fill
费用、债券 ETF T+0、股票 ETF T+1 和未结算持仓桶。profile 本身只到
`research_observation/local_only`；佣金是用户研究假设，不冒充交易所规则。

日频融资沿同一支持文件使用 `research-daily-cash-financial-context-v4`，封存信用协议、逐合同本金、自然日计息、融资关联、偿还、担保和风险命令。独立信用 oracle 与订单、现货账户及资金流 oracle 协作，从原始事件重建；不能以生产快照中的债务聚合代替复算。六表信用估值模型为 `cash_plus_positions_and_credit_liabilities_v1`，会话调整在原权益调整上减去本金及应计未付利息。借还本金不属于外部入出金，利息不混入成交费用或 TCA；报告与导出均消费同一Result的净资产及收益序列。输入和适用范围见[现货及信用账户](spot_account.md)。

## 消费

```powershell
python -m research_pipeline report --verification-result <VerificationResult.json> --result-store <ResultStore> --output <报告.md> --format markdown --json
python -m research_pipeline compare --left-verification-result <左.json> --left-result-store <左Store> --right-verification-result <右.json> --right-result-store <右Store> --json
python -m research_pipeline analysis run --verification-result <VerificationResult.json> --result-store <ResultStore> --request <AnalysisRequest.yaml> --output <AnalysisResult.json> --analysis-memory-bytes <字节> --json
python -m research_pipeline analysis compare --analysis-result <左AnalysisResult.json> --analysis-result <右AnalysisResult.json> --measure overall.compounded_return --direction higher_is_better --output <AnalysisComparison.json> --json
python -m research_pipeline export-result --verification-result <VerificationResult.json> --result-store <ResultStore> --output <导出目录> --json
```

所有消费者先通过同一个 verified context 复验 VerificationResult 与 Result，不直接猜目录或解析 Runtime 中间文件。`report`、`compare`、`export-result` 保留对 `status=fail` 的诊断消费；`analysis`、Workspace 与 Dashboard 只接收 `status=pass`。`export-result` 只复制并复核已验证 Result，不重新执行研究。run-root 清理后仍可消费。

`report --output` 在目标同目录按 UTF-8 写临时文件并以不覆盖方式原子发布。目标已存在时失败且
原字节不变。`--format markdown` 写可直接阅读的正文；`--format json` 写
`research-verification-report-v1` 结构。机器 stdout 只保留输出路径、格式、Result ID 和
Verification hash 的短回执。省略 `--output` 时继续返回 stdout 报告。

直接 `compare` 的范围固定为 `verified_metric_facts_only`：它只比较两侧已经验证的 metric ref、
注册表单位与方向、逐指标样本窗口/样本量/状态，以及实际 policy、claim level 和 ceiling，并明确
标注“未检查 package 合同”。plan、package 或 implementation 身份不同只作为说明，不会单独
否决比较。任一必要事实不一致时整次结果不可比，不输出部分 delta；事实一致时每个指标返回
单位、方向、窗口、样本量、左右值和 `right - left` delta。

需要完整研究语义时使用 `package compare`。该入口先由 `packages.delivery` 唯一检查两侧
ResearchPackage 的 metric/claim 合同和各自 Result 绑定，再复用上述指标事实比较，并返回
`package_contract_and_verified_metric_facts`。CLI 不复制第二份包级合同门。

### 通用时间序列分析

`analysis run` 使用外部 AnalysisRequest 选择 Result 中的一张表及其日期列和值列。请求必须显式
绑定 Result ID 和 Verification hash，并声明：

- 请求窗口、`observation / level / simple_return / log_return` 值语义；
- `point / non_overlapping_period` 区间语义、频率和年化期间数；
- 单位、`gross / net / unspecified / not_applicable` 费用口径；
- 空值、不完整年度、预期年度观察数和需要的聚合集。

公共核心不根据列名、schema、表角色、项目名称或单位字样猜收益语义。`observation` 只支持
计数、均值、正负零计数和极值；`level` 还支持首值、末值和绝对变化；只有显式声明为非重叠
期间收益的 `simple_return` 或 `log_return` 才能计算复合收益、几何年化和最大回撤。期间收益
必须使用 `missing=reject`。没有显式 `expected_observations_per_year` 时，年度完整性记录为
`not_assessed`，框架不猜交易日历。

分析表通过 ResultStore 的原摘要、schema 和行数验证进入同一 verified snapshot。计算只投影日期
和值两列，按 Arrow batch 顺序处理；日期重复、倒序、空窗口、非有限数值和不合法简单收益都会
拒绝。`--analysis-memory-bytes` 只控制这两个投影列的批次预算，不代表整个 Python 进程的绝对
内存上限。AnalysisResult 记录来源 Result、Verification、package、plan、claim、表身份、实际
窗口、Arrow 类型、行数闭包、总体统计和逐年统计，并以规范 UTF-8 JSON 原子发布且不覆盖已有
文件。

`analysis compare` 只读取两份或多份规范 AnalysisResult。分析规格、实际窗口、日期/数值 Arrow
类型、claim policy、claim level 或 claim ceiling 任一不一致时，比较结果明确返回不可比原因和
空排名。可比时仍须显式选择固定指标和 `higher_is_better / lower_is_better` 方向；公共核心不从
业务名称推断优劣。

holdout 的正式事实随 Result 保存为 `plan → prepared → opened → terminal`；完整候选族另有
`retired` 终态。`verify` 从这些事实独立复核冻结候选、样本范围、模式、打开顺序和结果绑定，
不读取 run-root。旧 v1 `reservation` 账本不能恢复或冒充当前确认结果。

正式日频 Label 按 `label_end_time + horizon_sessions` 写成同质 Parquet row group。模型 split
在目标扫描前先复用 ExternalArtifact 的逐文件验证，再读取 footer 验证每个 row group 的这两个
边界字段各自只有一个值；随后只对 development 条件扫描目标列，holdout 侧只读取样本键、时间、
可见时间和 horizon。完整 Label 表的语义 hash 重算和 holdout 目标物化只在持久账本成功记录
`opened` 后执行。输出里删除目标列或只记录 Arrow filter 都不能替代这个物理读取边界。

## 正式归档最小集合

一项真实研究要继续被报告、比较、复现、Workspace 或 Dashboard 正式消费，归档时必须同时保留：

- ResultStore 中由 VerificationResult 绑定的完整 Result 目录；
- 对应的结构化 VerificationResult 文件。

验收摘要、自然语言报告、任务完成状态或单独保存的指标表都不能替代这两项。缺少 VerificationResult 时，消费者不知道哪项独立验证结论可以信；缺少 Result 时，VerificationResult 也无法重新核对实际表和支持文件。

确认 `report` 和 `export-result` 能只靠上述两项成功后，run-root 才可以进入人工清理规划。当前 `research_pipeline gc` 只处理工件根下的 `staging/` 与 `cache/`，不会替操作者判断或清理 ResultStore、完成态 run-root 和历史项目轮次；这些目录的移动或删除需要单独规划和授权。

## 结论边界

VerificationResult 失败时，可用 `report` 或 `export-result` 定位原因，但不能进入 Dashboard。修对应数据、算法或 claim 后产生新 run；不得手改 Result 或验证文件。`planned`/`local_only` 能力、未运行真实数据或缺独立验收时，文档必须保留相应上限。

共享期货账户以 `research.shared-futures.*.v1` 九张金融表及单行 context 表封存原始声明与市场事件。独立复核检查共享权益、方向桶、冻结、风险与换月，HTML报告支持 `shared-futures-portfolio-report-v1` 请求；见[共享期货账户](shared_futures.md)。
