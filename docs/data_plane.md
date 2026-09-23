# 列式数据平面

## 正式因子库发布绑定

因子源计划使用`admitted-query-plan-v5`。计划内的`factor_publication`保存当前表实际引用的
publication、compute run、Catalog、实现记录、整表内容身份和覆盖日期；这些字段参与
`plan_hash`及后续DatasetArtifact身份。只存在表或Catalog声明不足以准入：publication必须为
`published`，计算运行必须为`validated`，Catalog快照正文须能复算到同一哈希，请求日期必须
落在当前`factor_storage_state`覆盖范围内。

`factor_compute_run.code_commit`只是重建启动时的Git基线，不单独代表实际计算实现。研究端还会
读取该运行`staging_path`下的五份发布审计JSON，用数据库已经保存的`manifest_hash`复算整条
审计链，再取得真实`compute_code_hash`。审计目录缺失、正文被改、实现摘要与来源/Catalog不
一致时均拒绝准入；不能用后来提交的SHA替换当时计算身份。

因子准入只短暂取得共享读租约。物化时由`data_plane.service`持有完整租约，依次完成发布身份
复验、只读Arrow查询、提交前源与发布复验、不可变工件提交、引用验证和最终复验，再释放租约。
研究端不导入`FactorPublisher`，也不写`data_factor.duckdb`；锁侧文件是发布协议的一部分。
旧v4计划明确拒绝，必须重新admit并建立新run，不修改旧checkpoint或历史Result。

数据平面是研究主链唯一打开数据库的层。它只接受已经通过 Catalog/PIT 准入的 QueryIR，并把查询结果写成带来源身份的 Arrow/Parquet 工件。

## 输入

- 已准入 QueryIR
- Catalog Lock 与当前 binding
- 显式只读数据库
- 有界日期、字段、资产和排序
- 固定 as-of、clock 和资源预算

自由 SQL、动态模块和项目 runner 不属于输入合同。

## 输出

正式 `materialize_dataset_plan()` 生成 DatasetArtifactRef，再通过 `research-data-bundle-v1`
和 ExternalArtifact 交给 Runtime；引用包含物理快照、manifest、schema、来源修订和分区。
日期范围统一使用 `DateRangeV1`。旧整表缓存、共享矩阵、handoff 物化链及兼容别名已删除，
Runtime 只消费当前列式引用，不重新打开研究数据库。

在正式 Runtime 中，Supervisor 对每个引用首次完整验证后冻结 manifest 与文件列表；同一 run
的后续 resolver 和 iterator 只复用这份内存绑定，并继续执行列、分区、路径和时态选择约束。
独立调用数据平面验证器时仍逐次完整验证。新的 run 或新的 resume 进程不会继承该内存绑定。

同一 data 节点的多 request 物化以单个 request 的 DatasetArtifactRef 为最小恢复单位。每项提交后
只在 run 内记录 request id、当前 admitted plan hash、引用和只读数据库指纹；恢复时逐项复验，
不把整个 ResearchPackage 身份复制到每项，也不新增逐 request ExternalArtifact。最终正式
`research-data-bundle-v1` 的内容与一次性成功执行一致，下游不能读取该 partial index。

## 只读保证

- 连接使用只读模式。
- 运行前后核对数据库 size/mtime。
- 输出目录不得与数据库、计划、package 或其他只读输入重叠。
- 查询必须有界，避免无意全表扫描和大内存物化。

## 时间保证

排序、窗口和过滤使用 Catalog 中的事件时间与可见时间语义。分钟 resample 只形成已完成 bar；财务或规则数据不以报告期、交易日标签替代公告/生效时间。

期货 `session_close/completed_session_close` 不把日期字段直接当成可见时刻。准入阶段按
availability policy、显式合约和请求日期，从正式 session-close bundle 唯一选择 policy，
编译逐日 `completed_at`；计划逐合约保存 policy ID、revision、hash、覆盖区间和确切交易日。
同一 bundle 可以包含多个互不冲突的合约或规则时段，Feature/Signal 的逐决策扫描及 SQL
provider 都执行同一计划事实。缺少供应商时段来源、期货开市日、合约映射、时区或日期覆盖，
同一合约日期重复匹配，或执行时 revision 漂移，都会失败关闭，不回退到自然日或固定
`15:00`。当前可执行覆盖只含 `AG2406.XSGE` 的 2024-01-03—04。

## Provider 资源包络

- `QueryIR.budget.max_rows/max_bytes` 只约束最终输出，不能反推 DuckDB、Arrow 或 Python 的总 RSS。
- admit 从真实数据节点 `ResourceBudget` 编译 provider 分配：DuckDB memory、单批 batch/writer 开销、进程余量、CPU 和 temp。当前进程余量为 `max(96 MiB, memory/8)`；这是代表查询校准后的分配策略，不是逐 allocator 的伪精确计数。
- 当前正式支持包络要求数据节点至少批准 256 MiB。低于下限会在对象统计或 provider 打开前拒绝；变长列没有 Catalog 上界、不支持的 VIEW/SQL 形状或窗口工作集无法落入 memory/temp 时也会在扫描前拒绝。
- DuckDB 与 Parquet provider 都按批准分配设置 DuckDB memory/temp、Arrow batch 和 CPU，不会为通过而缩短日期、删列、抽样或改变研究输入。Runtime 继续用现有 ResourceObservation 记录整个进程树 RSS 和 scratch 峰值，不新增资源 proof。
- 贴身回归为定宽 20 万行、256 字节变长字符串 16 万行、排序/窗口 30 万行；每类由 pytest 启动全新进程，走真实临时 DuckDB/Parquet/Arrow，并同时观察子进程及后代 RSS、Arrow 读取量、进程 I/O 与 temp/spill。
- `VerifiedDataset.parquet_uncompressed_bytes()` 只读取已验证当前绑定的 Parquet footer，为确需完整 pandas 矩阵的消费者提供数据页前下界；它只负责明显超界早拒绝，实际批次和 pandas 保留量仍由 `PandasFrameBudget` 逐步记账。

## 失败动作

schema、来源、PIT 或路径身份失败时停止生成工件并返回到 package admit；已有旧工件不能靠改 manifest 重新获得资格。
