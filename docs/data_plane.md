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
研究端不导入因子 Publisher，也不写因子数据库；锁侧文件是发布协议的一部分。
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

SQL 与逐决策工件统一先选当时可见的最新修订，再判断该修订的有效区间，随后筛选业务条件，
按 `QueryIR.sort` 排序后执行 `QueryIR.limit`。例如同一公告从盈利 10 修订为亏损 -10，
两版都已可见时，筛选正利润不会返回旧版。只在整个实体内恒定的键条件可提前下推；
新版已经关闭的有效区间不会回退到旧版继续生效。业务过滤不能掩盖最新修订冲突或有效区间重叠。过滤和排序使用的隐藏字段保留到选择完成，
返回结果仍只含请求的公开列。

逐决策来源物化保留截至 QueryIR `as_of` 的版本事实，不提前套用结果 `limit`；
原始事实流仍受 `max_rows/max_bytes` 约束。消费时跨批次、跨文件完成实体选择，
利用已准入的主键升序前缀和组内查询排序保持稳定结果。


期货 `session_close/completed_session_close` 不把日期字段直接当成可见时刻。准入阶段按
availability policy、显式合约和请求日期，从正式 session-close bundle 唯一选择 policy，
编译逐日 `completed_at`；计划逐合约保存 policy ID、revision、hash、覆盖区间和确切交易日。
同一 bundle 可以包含多个互不冲突的合约或规则时段，Feature/Signal 的逐决策扫描及 SQL
provider 都执行同一计划事实。缺少供应商时段来源、期货开市日、合约映射、时区或日期覆盖，
同一合约日期重复匹配，或执行时 revision 漂移，都会失败关闭，不回退到自然日或固定
`15:00`。当前可执行覆盖只含 `AG2406.XSGE` 的 2024-01-03—04。

## Provider 资源包络

- `QueryIR.budget.max_rows/max_bytes` 约束 provider 输出；逐决策模式下包含保留的全部版本事实，不能反推 DuckDB、Arrow 或 Python 的总 RSS。
- admit 从真实数据节点 `ResourceBudget` 编译 provider 分配：DuckDB memory、单批 batch/writer 开销、进程余量、CPU 和 temp。当前进程余量为 `max(96 MiB, memory/8)`；这是代表查询校准后的分配策略，不是逐 allocator 的伪精确计数。
- 当前正式支持包络要求数据节点至少批准 256 MiB。低于下限会在对象统计或 provider 打开前拒绝；变长列没有 Catalog 上界、不支持的 VIEW/SQL 形状或窗口工作集无法落入 memory/temp 时也会在扫描前拒绝。
- 对“单个 JSON 字符串数组 + 有限分类关系”的受支持 VIEW，admit 会从只读对象定义冻结原始表、日期列、JSON 列、输出列和本次分类值。DuckDB provider 按该证据使用 `UNNEST(from_json(...))` 的投影展开，避免执行 `json_each` 横向连接产生与正式输出无关的巨大中间结果；无法完整识别的形状仍在准入阶段拒绝，不猜测改写。
- DuckDB 与 Parquet provider 都按批准分配设置 DuckDB memory/temp、Arrow batch 和 CPU，不会为通过而缩短日期、删列、抽样或改变研究输入。Runtime 继续用现有 ResourceObservation 记录整个进程树 RSS 和 scratch 峰值，不新增资源 proof。
- 贴身回归为定宽 20 万行、256 字节变长字符串 16 万行、排序/窗口 30 万行；每类由 pytest 启动全新进程，走真实临时 DuckDB/Parquet/Arrow，并同时观察子进程及后代 RSS、Arrow 读取量、进程 I/O 与 temp/spill。
- 逐决策事实查询没有 SQL `LIMIT`，排序预算按完整来源上界计算，即使只有可见性过滤、没有修订窗口也一样；降低最终结果 `limit` 不会降低该预算。超出 memory/temp 时在 provider 数据扫描前拒绝。后置业务字符串条件不能替代来源版本字符串长度上界。
- `VerifiedDataset` 的普通、逐决策和原始事实扫描均关闭线程并行、批次预读与 Parquet 整段预缓冲，fragment 预读限制为 1；每批继续检查批准的行数和字节上限。消费者暂停迭代时，不为整个多文件来源提前排队读取。
- `VerifiedDataset.parquet_uncompressed_bytes()` 只读取已验证当前绑定的 Parquet footer，为确需完整 pandas 矩阵的消费者提供数据页前下界；它只负责明显超界早拒绝，实际批次和 pandas 保留量仍由 `PandasFrameBudget` 逐步记账。

## 失败动作

schema、来源、PIT 或路径身份失败时停止生成工件并返回到 package admit；已有旧工件不能靠改 manifest 重新获得资格。

## 物化身份与重用边界

普通数据 provider/compiler 使用 v2，分钟数据使用 v3；这些版本进入逻辑快照身份。旧实现若已在源筛选阶段丢弃修订事实，不能通过新读取器恢复缺失版本，必须重新物化。旧 request partial 不作为当前执行的复用证据；正式节点仍按实现身份、计划和输入闭包判断是否可复用，不修改既有 Result。

普通 as-of 与逐决策查询的扫描内存都按 `required_scan_fields` 计量，包括选择版本、判断区间和业务筛选所需的隐藏列。输出列较少不会降低这些扫描成本。

## 封存输入

`package admit`、`run`、`workspace run/execute` 支持 `--input-snapshot-manifest`，与
`--data-db/--source-db` 互斥。归档清单按 request_id 引用原 AdmittedQueryPlan、已提交的
DatasetArtifactRef 或 raw 分钟 PartitionedDatasetRef、明确允许根以及当前批准的 binding_id。
研报正文的 `--source-archive-root` 与行情归档是不同输入。

清单格式为 `archived-input-manifest-v1`，路径相对于清单文件解析，准入时转为绝对路径并内联原计划：

```json
{
  "contract_version": "archived-input-manifest-v1",
  "requests": {
    "daily_bar": {
      "kind": "dataset",
      "original_plan": "old-plan/queries/daily_bar.json",
      "root": "old-artifacts/data",
      "reference": {"...": "完整 DatasetArtifactRef 内容"},
      "binding_id": "approved_archive_daily"
    }
  }
}
```

`reference` 必须是原工件完整引用对象；示例省略其字段。raw 分钟使用 `kind=minute`，
`reference` 为完整 PartitionedDatasetRef，`root` 为明确分钟根。清单本身不批准物理绑定。

归档来源只支持逻辑查询等同重用，不能扩大列、证券或日期。当前 Catalog 必须为快照真实字段
声明独立物理绑定并完成审批；投影快照不能证明原数据库整表 schema。来源原有可见性、修订
政策和 claim 上限继续适用，不因离线文件存在而获得新的 PIT 保证。

普通快照通过 Arrow 在节点预算内按主键排序，再发布绑定新准入的正式快照；超预算拒绝。
分钟沿用现有文件引用扫描，并与冻结清单的文件、范围和股票池核对。该分支不连接 DuckDB
或 SQLite，结果报告 `input_source=archived_snapshot`、`database_opened=false`，不报告
未经执行的 `database_unchanged`。清单内容冻结进正式计划；恢复使用同一来源，清单变化须重新准入。
