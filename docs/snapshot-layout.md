# Parquet 快照布局

布局策略 `adaptive-bounded-v1` 将输入 batch、文件和行组三者分开。输入 batch 控制取数工作集；布局控制文件数量和有界缓冲；`row_group_size` 控制文件内行组。

## 默认策略

| 参数 | 默认值 | 含义 |
| --- | ---: | --- |
| `small_table_max_rows` | 65,536 | 小表累计行数上限 |
| `max_buffer_bytes` | 8 MiB | 待写 Arrow 数据的字节上限 |
| `row_group_size` | 65,536 | 单次行组累计行数上限 |
| `target_file_rows` | 262,144 | 单文件累计行数上限 |
| `target_file_bytes` | 32 MiB | 单文件累计未压缩 Arrow 数据上限 |
| `compression` | `zstd` | Parquet 压缩 |

小表同时满足行数和字节上限时，输出 `small/part-000000000000.parquet`。超过任意上限后，已缓冲数据随即按月写出。普通大表后续按 `row_group_size` 与 `max_buffer_bytes` 累积有界块，在块内按月份聚合，同月保持来源顺序，再按 `year=YYYY/month=MM` 增量写入。每月组写完立即释放待写副本；月份变化或文件达到行数、字节上限时关闭当前文件，相同月份后续再次出现时继续该月编号。证券优先主键的文件数因而受块内月份数约束，不随证券逐月切换开文件。最多同时打开一个 Parquet writer。文件字节上限度量未压缩 Arrow 数据，不能据此推断压缩文件长度。

空表仍受 `result_cardinality` 约束。允许空结果时发布保留 schema 的单个空文件；默认拒绝。单行 Arrow 数据超过缓冲字节上限时直接拒绝，不临时放宽上限。

## 扫描顺序与时态事实

普通快照保持原月份布局的扫描顺序：先按年月排列，同月内保留来源输入顺序。小表在有界缓冲内合并月份，跨输入 batch 的划分不改变该顺序。对日期领先的主键，该顺序与准入主键顺序一致。

证券领先的主键可能跨多个月份，月份扫描顺序不等于完整 QueryIR 排序。`ArtifactResolver.iter_batches` 提供分片扫描、投影与过滤，不承诺 QueryIR 的全局排序。使用 `ParquetColumnarProvider` 重读物化结果时，SQL 显式按 `plan.query.sort` 排序；归档物化也会在已有预算内按主键排序。其他消费者需要全局顺序时应使用其既有的明确排序合同，不能推断目录扫描已经有序。

`temporal_source` 始终写入连续编号的 `primary-key-stream`，不按月份重排，不选择或合并版本。隐藏的可见时间、修订字段及所有版本保留。`iter_batches_at` 仍在真实消费者时点做可见性与版本选择。

## 冻结与恢复身份

`AdmittedQueryPlan` v6 的 `snapshot_layout` 保存完整策略，包括策略 ID、writer 版本、压缩、分区语义、行组和所有行数/字节门槛。它进入 plan hash；逻辑快照绑定 plan hash；物理 manifest 再绑定实际文件及同一策略。`snapshots` 与 `snapshot_layout` 同时进入数据物化算子及 Runtime adapter 的显式源码闭包，布局实现变化会改变 checkpoint 实现身份。直接传入与计划不一致的策略会在创建 staging 前拒绝。

codec 读取没有 `snapshot_layout` 的旧 v6 计划时保持旧身份，不能用当前默认补齐。此类计划不能重新物化新布局，应重新准入并创建新运行。旧 manifest 和已提交工件继续只读验证，不改身份、不重写文件。已有 checkpoint 是否可用仍由当前计划、实现与正式工件闭包判断。

## 内存边界与取舍

待写缓冲既限制行数，也限制 Arrow 字节，并复制输入 slice，避免仅保留少量行却间接持有整个来源 batch。待写 RecordBatch 片段累计至 64 个时在同一缓冲内合并，限制极小输入 batch 引起的元数据对象堆积。`combine_chunks`、月份重排和小表切换到大表时存在有界副本共存，不能把 `pending.nbytes` 当作进程峰值。

provider 分配在输出 batch 之外预留三份 `max_buffer_bytes`，默认 24 MiB，用于布局复制、合并和交接工作集。普通大表不同时积压下一块；块数据、月份 take 副本和 writer 输入最多三份数据共存，块合并期间的输入片段与合并结果也在该预留内。当前输入 batch、Parquet 编码工作区、Python 日期和索引对象、schema、文件元数据及解释器仍由 provider 与进程余量覆盖；实际进程树资源治理继续生效。该预留不等于精确 RSS 上限，也不自动扩大已批准资源。

小表单行组减少文件打开和元数据工作，同时会扩大单月过滤的候选解码行数；这项取舍受小表门槛限制。大表保留月份分区与统计量裁剪。性能比较必须同时列出全量读取、日期筛选、发布耗时及缓冲/进程内存，局部吞吐不能代表完整研究链耗时。
