# Runtime 与恢复

Runtime 只执行已经准入的 typed operator DAG。调用方不能传入 callable、动态模块或另一套 action dispatcher。

项目Worker在导入扩展入口前通过安装元数据核对dependency_lock，缺包或实际版本不符即拒绝。静态bundle构建不要求安装依赖，也不自动安装；依赖校验不拦截运行时动态导入。

项目扩展在可信本地代码前提下使用独立 worker。框架保留正式输入输出路径、内容完整性、超时和进程树清理，不用全局文件/导入 monkeypatch 或 audit hook 伪装安全沙箱。正常临时文件和替换操作可用；staging 外任意副作用由可信代码负责。受控 junction 和 hardlink 不因链接形态拒绝，正式路径仍按解析后的真实位置校验。

ExternalArtifact提交时，staging内部的链接项会先复制为普通内容，再原子移动，避免绝对junction因目录改名失效；hardlink保持原有内容校验，不重复复制。越出工件根或形成目录循环仍拒绝。

## 不可变身份

研究运行身份绑定：plan、package、Catalog/PIT、数据 revision、core 与项目实现、clock、seed、mode 和父子 run lineage。机器容量、worker 数和可选跨进程治理只写入调用及运行记录，恢复时必须与首次调用一致，但不改变研究身份。

节点实现身份按共同变化的算子族绑定源码：Runtime 适配器位于 `runtime/adapters/`，横截面执行器及适配器位于 `application/grid_*.py`。`OperatorDefinition` 显式列出实现文件和共享语义依赖；`runtime_adapter_ref.dependency_modules` 参与摘要，运行前也按同一清单复核。某族修改只改变该族和真实依赖它的定义，共享模块修改则使全部消费者失效。同族不按单函数进一步拆分，不自动推断调用图。

数值兼容模式（`numerical`）不把整包构建摘要当成节点缓存身份；严格字节模式（`byte_exact`）仍要求整个构建一致。因此新构建可能要求严格字节节点重算，但其产物内容相同不会仅因上游 execution ID 改变而使下游数值节点失效。实际内容变化才沿输入引用传播。registry 改变后仍须重新准入；节点兼容不等于旧计划可以直接恢复，也不增加跨 run 自动搜索 checkpoint 的功能。

## 执行记录

- invocation：恢复所需的显式输入和计划内 bundle 闭包。
- event log：run、node、attempt 和 checkpoint 状态。
- checkpoint：节点执行身份、输入、实现、环境、clock、seed 和 typed 多端口输出。
- external artifacts：原子提交的大工件。
- completion metadata：ResultAssembler 和 Verifier 需要的运行摘要与 validity 引用。
- result finalize projection：只供 `inspect` 区分 `pending/succeeded/failed`；成功时绑定已发布 Result，失败时保留脱敏错误和真实发布状态。它不是 Result，也不进入 verify。

## 数据引用与节点交接

每个数据生产节点把 `research-data-bundle-v1` 写进自己的 ExternalArtifact `result.json`。普通
列式工件和 raw 分钟月分区引用都按 `request_id` 使用同一合同；普通数据节点只物化非分钟请求，
分钟 scan 只提交 Parquet 分区引用，不复制 raw 行。下游只从 typed 入边读取，ResultAssembler
再从本次 Runtime 输出索引复验并合并全部数据 bundle，不读取 `handoff` 或节点阶段旁路。

期货日频信号节点按 `market_request_id` 取得同一 AdmittedQueryPlan，并用 Runtime 的固定时钟
执行逐决策选择。信号 `decision_time` 来自计划中该合约、该交易日的 session 完成事实；计划
缺失、覆盖不全或 bundle revision 已变化时停止运行，不由领域代码补固定收盘时间。

同一次 `run` 调用由 Runtime Supervisor 持有唯一的工件验证会话。DatasetArtifact、
PartitionedDataset 和 ExternalArtifact 首次绑定时完成内容、footer、manifest 与目录闭包检查，
随后普通算子复用冻结的 manifest 和文件列表；列裁剪、分区 key、路径 containment、请求和
时态身份仍在每次消费时检查。项目 Worker 只收到绑定本次 `run_id/attempt_id` 的临时输入描述，
不重新计算输入内容摘要；该描述不会进入 checkpoint、Result 或磁盘信任链。

验证会话只活在一次 `RuntimeExecutionService.execute` 调用中。`resume` 启动的新进程、
`rerun-from` child 和任何新 run 都会重新完整验证；不存在跨 run 的验证缓存或新 proof 文件。

普通列式 data 节点包含多个独立 request 时，每个成功的 `materialize_dataset_plan` 会把
`request_id + admitted plan hash + DatasetArtifactRef + 数据库只读指纹` 原子写入 run 内的
节点恢复索引。后续 request 失败后，`retry-node` 或中断后的 `resume` 会逐项比较当前 plan 并用
新的 run 内验证会话复验引用；只有相同且完整的 request 会跳过 provider。变化或损坏只淘汰
对应项。全部完成后仍只提交一个正式 data ExternalArtifact/checkpoint，恢复索引不会被复制进去。

`rerun-from` 会先验证父 run、checkpoint 和 ExternalArtifact，再把目标节点之前的正式工件导入
child ExternalArtifactStore。目标节点及后继只消费这些导入工件；不会复制父 `artifact` 或
`handoff` 目录，也不会为证明父目录未变而再次扫描整棵目录。内容漂移由已有 commit/checkpoint
验证直接拒绝。rerun 后代继续使用最初运行的持久 holdout 账本位置；已经打开、失败消耗或退役的
holdout 不会因为 child 输出目录变化而重新获得读取资格。根锚点作为 invocation 的不可变字段逐代
继承并核对；父 invocation 缺失、血缘循环或任一层锚点冲突时，在 Runtime 节点启动前拒绝。

## 恢复选择

| 情况 | 命令 | 行为 |
| --- | --- | --- |
| 同身份进程中断 | `resume` | 复用已验证 checkpoint，继续当前 run |
| retry policy 允许的节点失败 | `retry-node` | 同身份增加 attempt，不改变输入 |
| 终态 run 需要从节点重算 | `rerun-from` | 创建新 child run，父 run 不变 |
| package、数据、实现、clock 或 seed 变化 | 重新 admit + `run` | 新计划、新 run，不复用旧身份 |

```powershell
python -m research_pipeline inspect --run-root <run目录> --json
python -m research_pipeline resume --run-root <run目录> --json
python -m research_pipeline retry-node --run-root <run目录> --node <node-id> --json
python -m research_pipeline rerun-from --run-root <父run目录> --output-run-root <子run目录> --node <node-id> --json
```

`inspect` 不改事件、attempt、checkpoint 或 ResultStore。它返回每个节点的
`attempts_used/max_attempts/attempts_remaining`、最后一条脱敏错误和使用同一恢复身份得到的
checkpoint 复验结果，并且只给一个 `recommended_action/next_command`。只有错误码在节点
retry policy 中且仍有余额时才建议 `retry-node`；同类故障耗尽后才建议 `rerun-from`，
非重试错误返回 package/算子修正路径，身份漂移也不会被当成可重试故障。

## 项目算子

运行准入直接加载并验证不可变计划的 registry、DAG、clock/seed、PIT as-of 和
ExecutionEstimate/QueryIR 对齐，不要求额外的 admission proof 文件。来源正文继续逐文件
校验，返回实际来源事实，不对小型验证摘要再套一层 hash。

项目入口 ABI 是同步 `run(context, inputs, output_root)`。`context` 是不可变的
`ProjectOperatorContext`，`inputs` 是 Supervisor 已复验、再由当前 attempt 临时描述约束的文件输入或受控分钟分区流；入口只能在 Worker 提供的
`output_root` 下写工件，并返回 `ProjectArtifactCommit` 字典或列表。commit 必须逐端口闭合
`artifact_type/relative_path/content_hash/schema_hash/byte_size`，额外文件、漏端口、类型或字节漂移
都会由 Worker 拒绝。

admit 把已验证 bundle 和实现摘要写入计划闭包。run、resume、retry-node 和 rerun-from 从计划重建同一 registry；bundle 缺失、内容变化或源码漂移都会失败。

非分钟多文件输入使用有界 `ProjectTableInput`，输出/state 以 staging 描述流式提交；
`ProjectOutputRoot.write_batches/write_state` 每批限制 32 MiB，表格固定 8,192 行 row group。
正式 Feature/Label 通过 `causal_plan` 冻结源列、逻辑月份、键和窗口，核心每批最多调度
8,192 键并记录实际交付，合并 inherited state lineage 后由 ExternalArtifact 核心附加时间。
正式路径最低 256 MiB，不能按预算改变 carry 的键批边界。扩展没有源路径，不自报时间事实；
越界读取、继承状态超窗、缺键、伪造时间或输出超出核心合并预算都会失败。
完整声明及当前支持范围见 `../project_extensions/README.md`。

## 资源治理

普通单次运行默认总容量为 16 GiB 内存、当前进程可用的全部逻辑 CPU 和 64 GiB scratch 上限；scratch 只是临时文件硬上限，不会提前占用磁盘。OperatorDefinition 的资源声明用于最低准入，adapter 另收到本次可用上限；当前 DAG 拓扑串行，重型节点可使用整次运行容量，轻节点不要求实际占满。

数据节点的 memory 是一次 provider 调用可使用的进程树支持包络，不是 QueryIR 输出上限，也不是把字符串和引用长度相加得到的“Runtime 总硬上界”。admit 将其编译为 DuckDB、batch/writer 与进程余量；当前支持下限为 256 MiB，低于下限在对象统计和扫描前失败。provider 用本次有效预算设置 DuckDB memory/temp、Arrow batch 与 CPU；只有显式 resource governor 存在时，Runtime 才创建 ResourceUsageSampler，观测当前进程及后代 RSS 和 scratch 峰值并交给治理记录。默认 run 不启动无人消费的采样线程。代表查询探针只做测试回归，不写新的 proof，也不进入真实成功运行的资源校准总体。

完整矩阵消费者先用 Parquet footer 下界做数据页前拒绝，再由运行时预算核对实际 Arrow/pandas 保留量。

内部 worker 默认 1 个；显式 `--workers` 不得超过本次 CPU 或 `max_workers` 容量。项目 Worker 在节点总资源预算内管理任务并行；进程内 reservation 与内部 worker 共用本次总容量，不会把 16 GiB 重复分给每个 worker。

默认路径不创建跨命令状态或租约。只有调用方显式提供共享 resource state dir 时，多个独立 CLI 才申请跨进程 FIFO 额度；等待超过显式 timeout 后拒绝，不改变金融语义。可选 RSS 遥测失败记录 `measurement_unavailable`，不能冒充已测得；需要清理失控 Worker 时仍保留进程树终止，无法递归观察时报告 `direct_process_only` 残留风险。旧 invocation 和 run record 不兼容当前资源合同，必须重新运行。

## Result 边界

Runtime succeeded 只说明计算完成。ResultAssembler 还要按 ResultSpec 选择已提交输出、检查指标 producer、输入 revision 和实现身份，再原子发布唯一自包含 Result。`inspect.finalize.status` 单独显示这一阶段；旧 run 缺该文件时是 `unknown`，不会猜成成功。若 finalize 后续步骤失败但 Result 已原子发布，投影会保留真实 Result 引用并建议进入 verify；未发布才返回 package 修正路径。可信消费必须继续运行独立 verify。
