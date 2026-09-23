# Result 与 VerificationResult

当前公开产物只有两层：

- Result：自包含的计算结果、输入闭包、lineage、运行摘要、validity facts 和金融控制材料。
- VerificationResult：独立 verifier 对 Result 完整性、适用门禁和结论上限的结构化判断。

## Result

ResultAssembler 只选择 ResultSpec 声明的已提交 typed outputs。它不打开数据库、不重算研究、不改变 claim。结果目录包含 `result.json`、`COMMITTED`、公共表和声明的支持文件；同一 run 只能有一个不可覆盖结果。

ResultStore 读取时验证实际文件字节、Parquet schema、行数、路径边界、manifest 和语义身份。同一次操作复用已验证 snapshot，避免重复完整扫描。

凡是正式结论可达的 Feature/Label，ResultSpec 编译器都会自动加入对应逐行因果时间表；只在
diagnostic 分支、且不是正式结论祖先的 Feature/Label 不封存。Feature 表逐行保存
`decision_time`、`max_source_observation_time`、`max_source_available_time`、
`source_partition_ids`；Label 表逐行保存 `decision_time`、
`first_actual_observation_time`、`last_actual_observation_time`、`available_time`。
正式因果输出缺少 Parquet、核心字段、唯一行键或时间关系不成立时，Result finalize 或
`verify` 失败。表身份按来源节点、端口、Artifact 类型和路径共同区分，不能因路径相同误删
另一张正式时间表。

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
授权输入，`verify` 只向 worker 提供本次 Result 中已封存且显式授权的内容。项目 verifier 的
状态、findings 和 outcome hash 进入通用 VerificationResult，executor 自报成功不能替代独立
复核。具体项目是否迁移到当前 Verifier bundle ABI，由项目自行决定，不属于 core 内建能力。

门禁按 ResearchPackage、算子图和 claim 触发。真正不适用时记录稳定 `N/A` 原因；缺材料、未执行或不认识的原因不能冒充 `N/A`。

金融 verifier 可以共用表 schema，但不复用生产仿真的守恒、摘要或 TCA 计算。当前 v5 以稳定 Arrow 批次读取 canonical/TCA，并把排序、连接和跨行汇总交给受 memory/temp 配额约束的只读 DuckDB；日频 ETF 的费用、T+0/T+1 和持仓桶也走同一受限扫描。它不再使用固定 512 MiB 来源大小门，也不把完整表展开成 Python 行。资源不足会使整次 `verify` 失败且不写 VerificationResult，不会降级或抽样。未提供参数时使用 1 GiB 进程预算和 8 GiB 临时盘预算；进程预算会先扣除当前解释器与 Arrow 批处理余量，再分配给 DuckDB。

固定规模验收 Result 的未压缩列块为 586,133,815 字节、共 285,011 行；全新 verifier 进程会同时记录整个进程树 RSS、实际读取字节、临时盘峰值和耗时。该本地证据说明当前受支持 fixture 能在显式预算内完成，不把它外推为任意 allocator 或任意 Result 形状的数学硬上界。

ETF 日频 Result 还必须封存 `simulation/daily-context.json`。该支持文件闭合受控
`market_rule_profile`、逐标的 rule bundle、用户佣金假设、SimulationResult 和 ledger
身份；缺失时 ResultAssembler 拒绝 finalize。独立 verifier 不读取 run-root 或数据库，直接
从同一已验证 Result snapshot 复验 profile 等于当前受控事实源，并重算交易单位、逐 fill
费用、债券 ETF T+0、股票 ETF T+1 和未结算持仓桶。profile 本身只到
`research_observation/local_only`；佣金是用户研究假设，不冒充交易所规则。

## 消费

```powershell
python -m research_pipeline report --verification-result <VerificationResult.json> --result-store <ResultStore> --json
python -m research_pipeline compare --left-verification-result <左.json> --left-result-store <左Store> --right-verification-result <右.json> --right-result-store <右Store> --json
python -m research_pipeline export-result --verification-result <VerificationResult.json> --result-store <ResultStore> --output <导出目录> --json
```

所有消费者先通过同一个 verified context 复验 VerificationResult 与 Result，不直接猜目录或解析 Runtime 中间文件。`report`、`compare`、`export-result` 保留对 `status=fail` 的诊断消费；Workspace 与 Dashboard 在此基础上只接收 `status=pass`。`export-result` 只复制并复核已验证 Result，不重新执行研究。run-root 清理后仍可消费。

直接 `compare` 的范围固定为 `verified_metric_facts_only`：它只比较两侧已经验证的 metric ref、
注册表单位与方向、逐指标样本窗口/样本量/状态，以及实际 policy、claim level 和 ceiling，并明确
标注“未检查 package 合同”。plan、package 或 implementation 身份不同只作为说明，不会单独
否决比较。任一必要事实不一致时整次结果不可比，不输出部分 delta；事实一致时每个指标返回
单位、方向、窗口、样本量、左右值和 `right - left` delta。

需要完整研究语义时使用 `package compare`。该入口先由 `packages.delivery` 唯一检查两侧
ResearchPackage 的 metric/claim 合同和各自 Result 绑定，再复用上述指标事实比较，并返回
`package_contract_and_verified_metric_facts`。CLI 不复制第二份包级合同门。

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
