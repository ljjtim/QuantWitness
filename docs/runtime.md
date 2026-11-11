# 运行、中断与恢复

运行已经准入的研究，并在中断后继续。

## 开始前

- 输入：已准入计划、固定输入、算子 bundle 和运行预算。
- 输出：正式节点工件、检查点、运行记录和 Result。
- 命令中的尖括号内容是待替换参数。

## 使用顺序

1. 固定计划、输入、clock 和 seed。
2. 使用 `run` 创建新的运行目录。
3. 使用 `inspect` 查看唯一建议动作。
4. 按建议使用 `resume`、`retry-node` 或 `rerun-from`。
5. 对已发布 Result 生成报告。

长时间研究不应因为一次中断就全部重跑。框架把研究分成有依赖顺序的步骤，完成一步就保存可核验的检查点。

进程意外中断时先用 inspect 查看，再按建议 resume。如果某个节点失败并允许重试，用 retry-node。恢复会确认输入与实现是否匹配，不能用旧结果冒充新研究。


## 常用名称

| 名称 | 含义 |
| --- | --- |
| ResearchPackage | 数据、算法、样本、指标和资源的完整研究声明 |
| Catalog Lock | 已确认的数据字段、来源和可见性说明 |
| PIT（按时点可见） | 在过去某时刻只使用当时已知的信息 |
| QueryIR | 限定字段、范围、排序和预算的数据请求 |
| DAG（计算步骤图） | 节点之间的先后依赖 |
| Artifact（工件） | 带类型、内容身份和来源引用的产物 |
| checkpoint（检查点） | 通过正式验收的节点输出及恢复依据 |
| Result（正式结果包） | 正式表、运行摘要和配置、实现、输入引用 |
| holdout（最终留出集） | 开发选择之外的最终评价区间 |
| Supervisor | 监督执行、发布检查点和记录事件的主进程 |
| Worker | 执行节点计算的独立进程 |
| manifest（清单） | 记录文件、类型、范围和内容身份的文件 |
| schema（结构） | 表的字段名称、类型和约束 |

## 处理规则

Runtime 只执行已经准入的 typed operator DAG。调用方不能传入 callable、动态模块或另一套 action dispatcher。

正式主 DAG 默认 `--mode deterministic_serial --workers 1`。`run`、`workspace run/execute` 可显式声明 `bounded_parallel` 或 `partitioned_batch`。workers 默认仍为 1，限制主 DAG 在途节点数，也作为单个模型节点的候选并发上限，不提高资源总量。

恢复使用原 invocation 的 mode/workers、计划和 checkpoint，不改写执行配置。已有Result由报告、分析、比较和导出直接消费。

- `bounded_parallel`：依赖满足的内建节点依照稳定顺序进入有界执行，空出的额度用于后续 ready 节点。
- `partitioned_batch`：当前批次完成并提交后再派下一批，输出仍按节点身份交接。
- 项目节点保持独占，复用现有项目 Worker 和分区恢复。专用 `execute_minute_partitions` 的分钟分区并行保持原合同。
- 选择、最终训练、holdout 以及其他非纯节点保持独占，不能借并行绕过系列锁和单次消费。

内建节点使用独立 spawn 进程计算。Supervisor 是唯一事件写者和 checkpoint 发布者。工件完整验证及 external 提交通过绑定 run/attempt 的临时通道回到 Supervisor。同一运行继续复用首次验证结果，验证会话不跨进程序列化。

实际并发受总 CPU、内存、scratch 和进程槽共同限制，节点声明预算不能相加超卖。并行模式启用共享资源治理时，先为整次运行取得总容量租约，再向节点分配子租约。同一资源池内其他运行按原排队规则等待可用容量。

该方式保留明确总包络，暂不跨运行细分空闲节点额度。进程总容量计共享 Supervisor 加活动节点专属进程树，POSIX 另计一个共享 resource_tracker。显式资源治理的 process-slots 至少容纳 Supervisor 与最大的单节点专属树。

没有共享治理时，上限由 workers 和计划内进程树声明派生。每个内建节点只采样自己的子进程树及暂存，避免把其他节点计入本节点。首个失败后停止补交，收齐在途任务并保留已成功 checkpoint，恢复仍复验原身份。

模型 fit 在节点额度内对 candidate×fold 使用 spawn 池，训练输入、处理器、模型和随机状态由各进程隔离。CPU、进程槽、父进程驻留和每个子进程的输入副本共同限制实际并发。资源不足时降低调度并发或明确拒绝，不修改候选、数据或 seed。

拟合结果和 trial ledger 按原 candidate/fold 顺序汇总。资源计数和 worker PID 属于运行诊断，不进入模型研究指标。

项目Worker在导入扩展入口前通过安装元数据核对dependency_lock，缺包或实际版本不符即拒绝。静态bundle构建不要求安装依赖，也不自动安装。依赖校验不拦截运行时动态导入。

项目扩展在可信本地代码前提下使用独立 worker。框架保留正式输入输出路径、内容完整性、超时和进程树清理。不用全局文件/导入 monkeypatch 或 audit hook 伪装安全沙箱。正常临时文件和替换操作可用。

staging 外任意副作用由可信代码负责。受控 junction 和 hardlink 不因链接形态拒绝，正式路径仍按解析后的真实位置校验。

ExternalArtifact提交时，staging内部的链接项会先复制为普通内容，再原子移动，避免绝对junction因目录改名失效。hardlink保持原有内容校验，不重复复制。越出工件根或形成目录循环仍拒绝。

随机发布事务令牌只用于 staging 与原子提交，不参与内容身份。相同正式文件、schema、行数、名称和类型在不同目录、进程或调度方式下生成相同 ArtifactRef。耗时、RSS、scratch 和进程数属于运行资源观测，不写入正式数据工件，也不改变下游节点、checkpoint 或 Result 身份。

Windows 可能暂时拒绝同盘目录发布，或发生共享冲突。项目算子 bundle 和 ExternalArtifact 对这类错误最多重试 5 次。总等待时间为 0.31 秒。每次仍使用原子重命名。

目标已经存在、源目录消失或其他错误立即失败，持续占用则保留原异常。该处理不改变工件内容身份、恢复校验或已封存结果。

## checkpoint 发布与资源验收

节点输出先写入 staging，并完成 manifest、内容大小、内容摘要和当前节点身份复验，再执行剩余时间检查。启用资源治理时，最终采样同时统计本次工作目录与 checkpoint 暂存目录，不计入其他 attempt。验收通过后才写入 `COMMITTED` 并原子发布正式目录。

验收前中断或被拒绝时，暂存内容不能恢复为成功。发布后尚未记录成功事件的中断，仍可按正式身份复验后接续。

当前checkpoint合同为`research-runtime-checkpoint-v4`，发布前完成资源验收和正式交接。恢复、跨运行复用及rerun导入只接受当前合同。输入、实现或配置变化后重新准入。已消费的holdout资格不会因新运行恢复。

分钟并行任务按 worker 数分配节点临时空间，实际验收还受对应租约额度约束。在途任务不超过 worker 数。发现失败后停止补交、取消未开始任务，等待运行项退出并保留成功 checkpoint，见[分钟分区预算与调度](minute-resource-budget.md)。已发布 Result 的 Workspace 接续从正式结果补齐完成索引，见[执行接续](workspace-continuation.md)。

## 不可变身份

依赖 distribution 使用 v2 规范内容记录身份。记录排除安装标记。记录还排除已声明入口在解释器 scripts 目录中的环境路径包装器。

完整定义见[发布说明](release.md#依赖分发身份)。同一内容记录在不同安装路径下不会改变依赖身份。数值模式仍绑定声明的数值后端，严格字节模式仍绑定全部依赖与构建制品。

旧 v1 摘要不会自动转换为 v2。恢复或复用时，已绑定依赖的身份不一致必须按现行门禁拒绝。需要重新准入并新建运行，不能编辑旧 checkpoint 或把旧摘要标成 v2。已封存 Result 按其原有内容身份继续只读消费，不因当前依赖锁升级而重写。

研究运行身份绑定：plan、package、Catalog/PIT、数据 revision、core 与项目实现、clock、seed、mode 和父子 run lineage。机器容量、worker 数和可选跨进程治理只写入调用及运行记录。恢复时必须与首次调用一致，但不改变研究身份。

节点实现身份按共同变化的算子族绑定源码：Runtime适配器位于`runtime/adapters/`，数据物化执行及合同位于`application/grid_data.py`与`grid_data_contract.py`。`OperatorDefinition` 显式列出实现文件和共享语义依赖。`runtime_adapter_ref.dependency_modules` 参与摘要，运行前也按同一清单复核。

某族修改只改变该族和真实依赖它的定义，共享模块修改则使全部消费者失效。同族不按单函数进一步拆分，不自动推断调用图。

数值兼容模式（`numerical`）不把整包构建摘要当成节点缓存身份。严格字节模式（`byte_exact`）仍要求整个构建一致。因此新构建可能要求严格字节节点重算。但其产物内容相同不会仅因上游 execution ID 改变而使下游数值节点失效。

实际内容变化才沿输入引用传播。registry 改变后仍须重新准入。节点兼容不等于旧计划可以直接恢复，也不会自动搜索磁盘上的 checkpoint。

当前计划只使用 `node-local-v4` 节点局部身份。配置通过已验证的不可变节点内容摘要绑定完整声明。身份继续绑定实际引用的 request/query、typed 输入和实现。

固定时钟、seed 和环境兼容事实也参与绑定。任意层级的显式 request_id、request_ids 及其带前缀字段在参数冻结时收集，包含策略参数。构建配置身份不再重复展开和扫描大规则。

无关指标、ResultSpec、研究语义或兄弟请求不使该节点失效。`plan-global-v1`、`node-local-v2` 和 `node-local-v3` 不再提供身份计算或恢复入口。受影响研究须重新准入并新建 run，历史 Result 保持只读。

`run --reuse-run-root <完成态run>` 可重复提供，按命令顺序尝试显式来源。来源必须在现行节点身份下 Runtime 成功且 Result finalize 成功。普通用法只机会式复用 `pure/cacheable` 节点。

命中时 Runtime 重新验证 checkpoint、typed 输出、ExternalArtifact 文件、schema、行数和 marker，以普通字节复制到目标 run，再记录来源 run 和 checkpoint manifest。没有同身份候选时正常执行。发现同身份候选损坏时失败关闭。

一次 checkpoint 导入中的源 ExternalArtifact 解码与复制共享临时验证会话，同一源内容只完整复验一次。新一次导入重新建立会话。目标复制内容仍重新验收，原运行或前一次调用的验证不会成为本次信任依据。rerun-from 的本次父工件导入也使用独立会话。

需要保证昂贵节点绝不回退执行时，可重复提供 `--require-reused-node <节点>`。这是调用方明确选择的严格复用：指定节点及其必要上游可以不是 `pure/cacheable`。但必须在任何 Worker 启动前全部通过节点局部身份、输入 ArtifactRef、implementation digest、operator definition digest、cache profile、checkpoint、typed 输出和 ExternalArtifact 内容复验。

任一节点不可复用就拒绝整个 run。裸 ResultStore、硬链接、可写目录引用、全盘扫描和常驻缓存服务都不属于该功能。

`run --reuse-failed-run-root <失败run>` 用于修正 ResultSpec 等不改变计算 DAG 的计划后继续执行。未指定必需复用节点时，来源必须是失败终态，新旧 DAG、节点身份环境、clock 和 seed 必须完全一致。Runtime 只导入事件状态为成功的 checkpoint，并重新验证 checkpoint 内容、typed 输出和 ExternalArtifact。

首个未成功节点及其下游在新 run 中重新执行。该参数不能与 `--reuse-run-root` 或 `rerun-from` 混用，也不会放宽普通跨运行缓存的 `pure/cacheable` 合同。

失败来源也可配合重复的 `--require-reused-node <节点>` 使用。此时允许修改其余 DAG，但指定节点及其全部上游必须在原事件链中成功。且局部节点合同、当前输入、实现、definition、cache profile、clock、seed、checkpoint、typed 输出和 ExternalArtifact 全部通过预检。环境兼容性仍使用各节点原有画像，`byte_exact` 不降级。

全部指定节点导入后才进入 Runtime。缺少任何一个即拒绝整次运行，不回退执行。目标 `recovery-plan.json` 记录原 run、每个来源 checkpoint 和目标 run，来源记录保持只读。

`resume` 与 `retry-node` 从冻结 invocation 恢复同一来源和强制节点列表，并重新完成复验。

LEAN日线和一分钟节点将正式执行表、指标及必要输入引用交给Result。配置、实现、输入与输出合同参与恢复身份，checkpoint不能跨身份复用。Result发布后保持只读，报告失败不重新执行金融节点。

LEAN HTML 报告继续绑定并正式验证八张执行表及指标表。读取预算用于实际展示的数据。未参与图表和明细的 intents、trace 不物化列数据、不解码 JSON，也不在报告内存中保留。收益、成交、资金、四桶持仓与账户事件的展示内容保持原口径。

## 执行记录

节点事件的 `running` 到 `succeeded` 包含节点执行、checkpoint 发布及成功状态交接，不能视为纯算子耗时。整次 run 墙钟扣除节点时间与结果组装后，只得到剩余差额。归因须结合进程创建、存活记录和事件时间戳，不能把差额直接命名为调度阶段。

- invocation：恢复所需的显式输入和计划内 bundle 闭包。
- event log：run、node、attempt 和 checkpoint 状态。
- checkpoint：节点执行身份、输入、实现、环境、clock、seed 和 typed 多端口输出。
- external artifacts：原子提交的大工件。
- completion metadata：ResultAssembler 需要的运行摘要和正式输出引用。
- result finalize projection：只供 `inspect` 区分 `pending/succeeded/failed`。成功时绑定已发布 Result，失败时保留脱敏错误和真实发布状态。它是诊断投影，正式消费者读取 Result。

资源超限诊断在 `failure_context` 保留原 `code=resource_exceeded` 和 `exceeded`。每个超额维度包含整数 `actual`、`limit`，维度限于 `memory_bytes`、`temp_bytes`、`process_slots`、`wall_milliseconds`。字节数、进程数与毫秒各使用字段原单位。该上下文随错误摘要保存，不改变预算、重试资格或 checkpoint 身份。

完整事件链属于具体执行的审计记录，保留在 run-root。ResultAssembler 在发布 Result 前重放事件并核对 run、node 终态和链头。不可变 Result 只保存稳定终态摘要和正式输出身份，不复制受排队、租约取得和 attempt 时序影响的链头。相同 plan、clock、seed、实现、输入和正式输出在串行或受控并行调度下生成相同 Result 身份。

## 事件增量会话

一次 Runtime 执行持有事件库的排他写者锁。进入会话时完整读取日志，验证事件内容、hash chain、run 身份及状态转换。然后建立仅驻留内存的投影、command_id 和节点事件索引。

后续追加只处理新事件，幂等命令直接返回原事件。相同 command_id 的不同 run、载荷或关联对象仍拒绝。

每条新事件继续写入日志、flush 和 fsync，然后更新内存状态。正常结束写者会话时，将完整状态一次写入可丢弃的 `projection.json`。活动会话或中断后，该文件可以滞后或缺失。

日志始终是公开读取、恢复和 inspect 的依据。写入失败后丢弃内存状态，下一次操作从日志重建。截断日志明确拒绝。

写者会话退出时释放锁和索引。新命令、新进程、恢复及只读 inspect 都重新验证日志，不继承旧会话的信任。

公开事件和状态快照与内部缓存隔离。Runtime 用节点索引查询当前节点的尝试及提交事件，避免为一个节点反复扫描全部历史。普通追加不复制、排序或序列化全部历史 command_id。完整投影只在会话正常结束时生成，公开 replay 仍返回完整状态和命令集合。

## 数据引用与节点交接

每个数据生产节点把 `research-data-bundle-v1` 写进自己的 ExternalArtifact `result.json`。普通
列式来源和raw分钟月分区引用都按`request_id`使用同一合同。普通数据节点依照`persist_inputs`选择直接来源或非分钟快照，
分钟 scan 只提交 Parquet 分区引用，不复制 raw 行。下游只从 typed 入边读取，ResultAssembler
再从本次 Runtime 输出索引复验并合并全部数据 bundle，不读取 `handoff` 或节点阶段旁路。

同一次 `run` 调用由 Runtime Supervisor 持有唯一的工件验证会话。DatasetArtifact、
PartitionedDataset 和 ExternalArtifact 首次绑定时完成内容、footer、manifest 与目录闭包检查，
随后普通算子复用冻结的 manifest 和文件列表。列裁剪、分区 key、路径 containment、请求和
时态身份仍在每次消费时检查。

项目 Worker 只收到绑定本次 `run_id/attempt_id` 的临时输入描述，
不重新计算输入内容摘要。该描述不会进入 checkpoint、Result 或磁盘信任链。

验证会话只活在一次 `RuntimeExecutionService.execute` 调用中。`resume` 启动的新进程、
`rerun-from` child 和任何新 run 都会重新完整验证。不存在跨 run 的验证缓存或新 proof 文件。

普通列式 data 节点包含多个独立 request 时，每个成功的 `materialize_dataset_plan` 会把
`request_id + admitted plan hash + DatasetArtifactRef + 数据库只读指纹` 原子写入 run 内的
节点恢复索引。后续 request 失败后，`retry-node` 或中断后的 `resume` 会逐项比较当前 plan 并用
新的 run 内验证会话复验引用。只有相同且完整的 request 会跳过 provider。

变化或损坏只淘汰
对应项。恢复索引当前为`data-request-partial-index-v2`。恢复只接受当前身份及完整内容复验通过的请求。

全部完成后仍只提交一个正式 data ExternalArtifact/checkpoint，恢复索引不会被复制进去。

`rerun-from` 会先验证父 run、checkpoint 和 ExternalArtifact，再把目标节点之前的正式工件导入
child ExternalArtifactStore。目标节点及后继只消费这些导入工件。不会复制父 `artifact` 或
`handoff` 目录，也不会为证明父目录未变而再次扫描整棵目录。

内容漂移由已有 commit/checkpoint
验证直接拒绝。rerun 后代继续使用最初运行的持久 holdout 账本位置。已经打开、失败消耗或退役的
holdout 不会因为 child 输出目录变化而重新获得读取资格。

根锚点作为 invocation 的不可变字段逐代
继承并核对。父 invocation 缺失、血缘循环或任一层锚点冲突时，在 Runtime 节点启动前拒绝。

## 规则、权益与金融事实交接

LEAN节点从已准入计划和包内`minute_scope`取得完整规则与日历，从正式`snapshot`取得公司行动。运行核对冻结来源绑定，再按实际使用时点选择可见版本。修改规则、行情或权益输入后重新准入。

股票和ETF分别使用`price_scale=2`、`price_scale=3`，现金统一以人民币分记账。公司行动按登记持仓及生效、付款、股份到账和可卖时点处理。simulation端口交付orders、intents、daily、trace与metrics，必要账户、策略状态、安装锁与策略输入随Result保存。行情、规则和公司行动保留持久输入与计划引用。

## 恢复选择

checkpoint 的内容、manifest 和 `COMMITTED` 标记都在同盘 staging 写入并持久化后，
才整体原子发布到正式目录。staging 或没有提交标记的未成功节点残留可以重算。
已提交内容损坏、或成功节点丢失提交标记时拒绝恢复。

`created`、`planned` 中断可通过 `resume` 按现有事件补齐启动，不重复创建 attempt。
恢复前核对 owner 的 PID 和进程创建时间：活跃 owner 返回等待建议，不能接管或把其
attempt 标记为 lost。进程退出或显式释放 owner 后才可恢复。心跳覆盖整个 Runtime 调用，
节点间不会释放 owner。

`rerun-from` 与失败 run 复用只复制成功且通过当前身份和内容复验的节点。失败、未开始的节点及其下游进入重算范围。指定节点及其下游仍强制重算。

独立成功分支可复用，独立未开始分支正常执行。成功节点的 checkpoint 复验失败时明确拒绝。

完成元数据保留 `node_completion_metadata[node_id]` 原始命名空间。聚合的工件摘要、证明摘要和计数使用 `node_id/字段名`。不同参数的同类节点不会互相覆盖。

输出端口仍保存在 `outputs[node_id][port]`。元数据和完整收尾记录准备成功后才提交 run 成功事件。

研究复现使用原ResearchPackage、冻结输入、clock、seed与实现重新执行。输出按业务键和声明精度比较。

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

`inspect` 不改事件、attempt、checkpoint、ResultStore 或共享资源治理文件。`inspect` 从现有事件投影节点阶段。阶段包括 `waiting_for_dependencies/ready/waiting_for_resources/executing/checkpointing/finalizing`。

返回值包含每个节点的开始时间、阶段时间、时长和心跳。返回值还包含进程健康、资源申请/预留及 `attempts_used/max_attempts/attempts_remaining`。最后 1 条脱敏错误和 checkpoint 复验结果也随回执返回。

复验使用同一恢复身份。共享治理存在时还会只读列出各 run 的 FIFO 请求和活跃租约。

它只给一个
`recommended_action`。可执行建议同时提供 PowerShell 字符串和 argv。只有错误码在节点
retry policy 中且仍有余额时才建议 `retry-node`。

同类故障耗尽后才建议 `rerun-from`，
非重试错误返回 package/算子修正路径，身份漂移也不会被当成可重试故障。

`run --json` 失败时会读取刚写入的同一份 diagnostic 事件，在首屏返回 `failed_node`、
`root_error`、`run_root` 和可复制的 `inspect` 命令。框架不会为此创建平行 traceback 或
第二套日志。

## 项目算子

项目薄声明可选登记：

```yaml
parameter_preflight:
  module: operator
  function: preflight
  parameters: [submit_time, sell_cutoff]
```

`preflight(context)` 使用与正式 Worker 相同的 `ProjectOperatorContext` 冻结规则：对象是只读
`Mapping`，列表是 `tuple`。它在 package 编译阶段由独立短进程执行，不接收输入工件、输出
目录或数据库路径，必须返回 `None`。这用于提前发现项目代码把只读映射误写成可变 `dict`、
列表形状假设错误或参数组合无法解释。

它不执行正式算法，也不替代 Operator 参数 schema。`parameters` 可选，显式列出预检需要的已声明参数。只向预检进程交付这些值，未提供的可选参数仍保持默认。

省略该字段时交付全部参数。正式 Worker 仍接收完整参数，投影声明随 bundle 身份封存。没有登记钩子的 bundle 不启动预检。

运行准入直接加载并验证不可变计划的 registry、DAG、clock/seed、PIT as-of 和
ExecutionEstimate/QueryIR 对齐，不要求额外的 admission proof 文件。来源正文继续逐文件
校验，返回实际来源事实，不对小型验证摘要再套一层 hash。

项目入口 ABI 是同步 `run(context, inputs, output_root)`。`context` 是不可变的
`ProjectOperatorContext`，`inputs` 是 Supervisor 已复验、再由当前 attempt 临时描述约束的文件输入或受控分钟分区流。入口只能在 Worker 提供的
`output_root` 下写工件，并返回 `ProjectArtifactCommit` 字典或列表。commit 必须逐端口闭合
`artifact_type/relative_path/content_hash/schema_hash/byte_size`，额外文件、漏端口、类型或字节漂移
都会由 Worker 拒绝。

项目 Worker 失败时保留稳定错误码，同时返回脱敏、限长的根异常类型和消息，不持久化完整
traceback。非分钟表输入的列投影错误会列出缺失列和已验证 schema 的可用列。未知文件会列出
当前提交清单中的相对路径。

节点提交后，系统立即核对 ResultSpec 所选输出的 Parquet 前缀。路径不匹配时，节点直接失败。系统不会等整张 DAG 完成后才返回 Result finalize 错误。

项目算子可以从普通列式输入或显式绑定的 `data.minute-bars.1m.v1` 扫描输入读取正式 request
准入事实。日期型 `as_of` 同时投影为按固定时钟时区解释的次日零时排他
`as_of_cutoff`，带时区时间则保留原绝对时点。分钟路径由 Supervisor 将已验证工件中的
request、plan hash、source revision 和来源
快照与当前 admitted plan 对齐后，只向 Worker 投影元数据。

它不交付分钟文件，也不改变已有
`data.minute-bars.v1` 分区读取合同。只有节点参数声明的 request 可见，Worker 必须完整读取并
返回 metadata-only 消费轨迹，Supervisor 再核对来源身份和 schema。

admit 把已验证 bundle 和实现摘要写入计划闭包。run、resume、retry-node 和 rerun-from 从计划重建同一 registry。bundle 缺失、内容变化或源码漂移都会失败。

非分钟多文件输入使用有界 `ProjectTableInput`，输出/state 以 staging 描述流式提交。`ProjectOutputRoot.write_batches/write_state` 每批限制 32 MiB，表格固定 8,192 行 row group。目录工件默认保留输出端口前缀。

若公共工件合同要求文件位于 ExternalArtifact 根目录，使用根级发布声明。扩展在 `commit_directory` 中声明 `publish_at_artifact_root=True`，Worker 与 Supervisor 仍逐文件核对闭合和内容。根级 `manifest.json` 和 `COMMITTED` 由 ExternalArtifact 核心保留，项目输出不能以任何大小写形式占用。

正式 Feature/Label 通过 `causal_plan` 冻结源列、逻辑月份、键和窗口。核心每批最多调度 8,192 键，并记录实际交付。核心合并 inherited state lineage 后，ExternalArtifact 核心附加时间。

正式路径最低 256 MiB，保持每批8,192键的既有调用边界。independent只声明无继承状态，不代表横截面运算可以任意拆并。扩展没有源路径，不自报时间事实。

越界读取、继承状态超窗、缺键、伪造时间或输出超出核心合并预算都会失败。完整声明及当前支持范围见 `../project_extensions/README.md`。

## 资源治理

普通单次运行默认总容量为 16 GiB 内存、4 个 CPU 槽 和 64 GiB scratch 上限。scratch 只是临时文件硬上限，不会提前占用磁盘。OperatorDefinition 的 `resource_profile` 必须显式声明 memory、CPU、scratch、进程槽和节点 wall-time。

声明是节点执行包络，也是共享准入的 reservation。Runtime 不再把整次 CLI 容量替换成每个节点的申请。总容量只负责拒绝无法承载的声明和协调多个运行。

数据节点的 memory 是一次 provider 调用可使用的进程树支持包络。不是 QueryIR 输出上限，也不是把字符串和引用长度相加得到的“Runtime 总硬上界”。admit 将其编译为 DuckDB、batch/writer 与进程余量。当前支持下限为 256 MiB，低于下限在对象统计和扫描前失败。

provider 用本次有效预算设置 DuckDB memory/temp、Arrow batch 与 CPU。串行路径仅在显式 resource governor 存在时创建 ResourceUsageSampler。观测当前 Runtime 进程、attempt 开始后新建的后代进程以及 scratch 峰值并交给治理记录。并行路径始终采样各节点的专属进程树与暂存，超限即停止该节点。

采样开始前已经存在的控制台宿主等 Runtime 后代不属于当前节点，不能污染节点 RSS 和进程数。默认串行 run 不启动无人消费的采样线程。代表查询探针只做测试回归，不写新的 proof，也不进入真实成功运行的资源校准总体。

完整矩阵消费者先用 Parquet footer 下界做数据页前拒绝，再由运行时预算核对实际 Arrow/pandas 保留量。

串行路径的 `process_slots` 覆盖 Supervisor、Worker 及其受支持的子进程树。并行路径将共享 Supervisor 与节点专属树分开计入总量。内建算子默认声明 1 个槽，spawn 计算至少预留当前解释器所需启动槽。

项目算子必须在薄声明中显式填写。启用共享治理时，`--resource-process-slots` 必须至少容纳 DAG 中最大的单节点声明。并行模式还须加共享 Supervisor 及 POSIX tracker 的槽。

CPU 和进程槽分别准入，不再用进程数推高 CPU 申请。

节点在排队时记录为 `waiting_for_resources`，取得租约并真正开始执行后才进入 `running`。事件同时记录申请向量、排队时间、取得租约时间和等待毫秒数，`inspect` 可以区分等待中断与执行中断。

普通 run 也会在 run-root 原子维护 `runtime-liveness.json`。该文件记录当前 run/node/attempt、
等待资源、执行、checkpoint 或 finalize 阶段、进程身份、最近心跳和预留资源。它用于诊断和活跃 owner 接管判定，
不进入事件 hash chain、checkpoint、Result、恢复身份或跨运行复用身份。

`starting` 覆盖计划加载和节点启动前阶段，准入完成前 `run_id` 可为 `null`。同一 owner 持续覆盖 Runtime 和 Result finalize，到本次调用返回或抛错才写入 `stopped`。同进程内重入和跨进程接管均须等待当前调用释放。

进程存活由 PID 与进程创建时间共同判定。没有算子提供的真实总量时 `progress` 固定为 `null`，框架不按耗时猜百分比。

`wall_seconds` 覆盖整个节点 attempt，不包含排队时间。Runtime 在取得资源后建立统一截止时间。逐分区 Worker 和 causal 键批只获得剩余秒数，不能为每次调用重新获得完整时限。项目 Worker 超时会被终止并清理进程树。

串行内建同步 adapter 在返回边界核对截止时间。并行内建节点在截止时间到达时终止专属进程树。

默认路径不创建跨命令状态或租约。只有调用方显式提供共享 resource state dir 时，多个独立 CLI 才申请跨进程 FIFO 额度。串行运行按节点声明申请，并行运行先申请整次 run 的总容量，再分配节点子租约。

各维度之和未超过总容量时可以同时执行，超过任一维度时后到请求等待。等待超过显式 timeout 后拒绝，不改变金融语义。启用共享治理时，最终资源采样覆盖 checkpoint 提交标记、目录发布和内容复验。

系统再核对 Supervisor 与本 attempt 子进程的合计用量。用量包括 RSS、scratch、进程数和耗时。

通过后才记录 checkpoint 成功事件。超出预算或租约时记录失败，
撤销本 attempt 刚发布的 checkpoint，避免恢复复用未通过资源门禁的输出。资源测量不可用时记录 `measurement_unavailable` 并阻断提交。

需要清理失控 Worker 时仍保留进程树终止，无法递归观察时报告 `direct_process_only` 残留风险。资源观测保留实际测量值，不会静默改写后续 reservation。共享状态使用 `research-resource-governor-v3`，资源观测保持 v4。

升级后必须使用新的仓库外 state dir，旧状态不自动迁移。项目声明必须包含`process_slots`，并经build、lint、admit进入新run。

活动租约与已结束租约、观测历史分开保存，心跳只读写活动状态。历史日志先持久化，再由活动状态原子提交有效前缀。中断后仍以原提交位置恢复。

项目 Supervisor 接管采样后，同一 Worker 出生身份在 50 毫秒周期内共用真实进程读数。通信门禁、scope 交接、批次验收和节点峰值使用同一读数。更换批次磁盘范围不重置进程周期。首次观测、新PID或出生身份变化及实际进程退出仍立即实测，节点最终退出强制刷新。

磁盘按各自范围和周期计量，批次和最终验收独立强制刷新，不因复用进程读数而跳过。共享读数和峰值由同一可重入锁保护，scope退出后恢复后台采样。测量失败及超限继续阻止成功。

状态文件、版本和恢复说明见[资源历史](resource-history.md)。

## Result 边界

Runtime succeeded 表示计算完成。ResultAssembler 按 ResultSpec 选择已提交输出、指标、输入与实现，原子发布正式 Result。inspect.finalize.status 单独记录 pending/succeeded/failed。

结果已发布时建议 report --result，报告失败不重跑研究。节点成功但组装未发布时原 resume 只重试组装。

项目指标由 spec/research.yaml 的 metric_definitions 提供。计划只组合当前 MetricContract 选择的定义，明确单位、频率、观察时点和聚合。正式报告直接消费指标表，算法正确性在开发用例中验收。

项目算子默认只在同一 run 恢复。显式 reuse_scope: cross_run 还须满足确定性或固定 seed、artifact_write_scope: output_only 和完整 bundle 身份，正式输出不能依赖 project/run/node/attempt 等易变 ID。

内建节点资源档位来自实际 OperatorDefinition，项目预算与运行总容量由调用方声明。资源不足拒绝，不自动提高额度。具体见[资源预算](project_resource_budgets.md)。

## 分区工件跨运行复制

恢复导入外部工件时保留已封存文件的修改时间与内容。分钟分区身份包含`mtime_ns`，导入后仍按原分区身份复验，不重写分区引用或放宽漂移检查。原工件保持只读，ArtifactRef不因复制改变。

### 因果事实列式传输

Worker的核心时间事实使用`causal-facts-v1`描述：逐行标量写入attempt内Parquet，相同`source_partition_ids`列表只存一份，各行保存列表编号。JSON只承载路径、行数、键列、lineage和共享来源清单。Supervisor读取后恢复同一有序事实，再执行既有核心合并、来源和时间检查。特征值、时间字符串精度、标签成熟边界及最终证据含义保持。

非分钟Parquet按观测列的行组最小／最大月份跳过确定不相交的数据。缺少统计、存在空时间或非日期时间列时保留原读取。原始来源身份仍完整核验，实际交付仍受冻结月份、窗口和可见时间约束。

新编解码模块属于Worker adapter身份。新研究重新准入，历史Result只读，不能跨实现身份恢复旧checkpoint。性能测量同时报告真实数值等价、分段耗时、传输大小及完整运行验收。不以压缩率替代完整链路成功。

## 紧凑因果键声明

`causal_plan.work_items`支持两种互斥键表示：原有`key_rows`，或`key_axes`与`key_order`。`key_axes`按`key_columns`顺序给出每列的非空唯一值轴。`key_order`给出从外层到内层的循环列索引。

每列值仍只能是有限JSON标量，产品键与其他工作项的键禁止重复。时间窗口、来源分区和工作项排序规则保持不变。

例如`key_columns=[entity_id,observation_session,window_sessions,feature_id]`时，`key_axes=[股票列表,[信号日期],[1],因子列表]`及`key_order=[3,0,1,2]`表示按因子、再按股票遍历。运行时仅展开当前至多8,192个键，扩展看到的每批`key_rows`和原表示相同。冻结计划保留轴表示，新旧表示具有不同计划身份。不能拿新身份接续旧checkpoint，历史Result保持只读。

### 因果 Worker 会话与组装恢复

因果项目节点可显式声明 `worker_lifecycle="stateless_session"`，在一个节点 attempt 内串行复用 Worker。仅适用于非分钟输入且不依赖隐式模块状态的算子。分钟输入显式请求此模式时在启动前拒绝。

每批仍建立独立上下文和时间事实，保持 8,192 键边界，跨批状态必须由原合同显式传递。会话固定节点、实现、输入和来源身份，固定输入核验在初始化时执行。批次失败、超时或节点结束关闭整个进程树。

未声明的算子继续每批独立进程，未知值拒绝。

会话首批完整交接并校验静态参数、来源、实现与时钟。随后批次只传当前工作项、剩余预算、显式状态及继承来源事实。调度器与Worker共享各自进程内已经校验的深度不可变参数，仍逐批校验窗口、键与预算。静态声明或来源身份变化拒绝继续。

普通可变输入须重新冻结比较，不能仅凭对象地址复用。该优化不改变项目参数或输入可见范围。

全部节点成功但 Result 组装失败且尚未发布时，公开 `resume --run-root` 可仅重试组装。恢复先核对原计划、当前实现、DAG、时钟、seed、审计身份和全部成功 checkpoint。任何不符都拒绝，不回退节点执行。

每次组装尝试追加事件，保留原终态。已发布 Result 不重开。

## 成功节点的载荷释放

释放对象包括本次 attempt 的请求数据副本和普通／分区 Worker 输出。全部端口和状态必须完成正式交接。资源验收、checkpoint 发布、Worker 退出及 attempt 与 node 成功也必须完成。

系统满足这些条件后，才按登记逐文件释放。源文件必须有已复验的正式 external 替代位置。

失败、部分完成、保护标记和未登记对象继续保留。释放报告记录实际删除的文件数与逻辑字节，回收失败不改变已成功研究状态。

目录载荷释放使用同一次遍历检查后代保护标记和源文件集合。逐文件复用源文件大小读数，正式替代工件复验、路径归属和大小核对继续执行。

恢复使用正式 checkpoint 和 external，不为补清理重新执行节点。Result 保留正式输出和持久输入引用。成功因果键批按独立交接记录释放工作副本。失败批、未交接状态和历史未登记目录继续保留。目录类别、保护规则和只读 GC 预览见 [产物生命周期](artifact-lifecycle.md)。

项目策略节点显式声明`execution_backend=lean`、冻结策略bundle和安装锁，经策略声明及范围准入进入LEAN。成功节点复验并复用正式工件。未完成LEAN节点整体重跑。恢复身份覆盖策略协议、bundle、引擎、插件、环境、输入和结果合同，详见[策略事件协议](strategy-protocol.md)。

普通数据观察逐请求生成行数和样本范围。默认直接来源不预扫描完整查询，不生成`data.row_count`观察表。多请求包以ResultSpec选择主指标请求，正样本由实际指标消费者检查。辅助请求可按声明为空，不把多个请求相加成一个样本量。

大型研究包优先使用PyYAML原生安全解析器，环境没有原生扩展时使用同库的Python安全解析器。两者均只读取声明数据。规则和交易日历按证券建立索引。来源身份只在解析器创建时读取一次，历史版本和PIT选择条件保持一致。

运行入口从已完成完整性复验的计划对象构造节点参数，不再次读取整份计划JSON。策略身份计算和节点参数内容保持一致，节点及总资源限额不变。

`rerun-from`失败时，CLI诊断与inspect建议引用新建的子运行，父运行的历史诊断保持在原目录。

分钟规范化仅保留当前证券的当前交易会话，连续分钟复用开盘前已选定的政策及其会话身份。跨证券、跨会话或时段外记录重新执行日历解析，历史修订可见性规则不变。

分钟停牌检查从既有证券规则索引读取完整版本流。系统仍检查分钟内每个状态变更时点和实际撮合时点。不扫描其他证券规则，也不按当前最新状态覆盖历史状态。

DAY订单日终到期处理在同次调用中复用日历索引和冻结范围摘要。每只证券仍独立选择当日可见的日历版本，并检查共同会话终点。跨交易日重新选择版本，不沿用前一日会话。

资源监督复用目录枚举取得的文件信息，按目录归入各个声明范围。重叠根只扫描一次。每次采样重新枚举，文件增长与删除会反映在本轮读数中。采样频率、结束测量、进程树、内存、临时空间、超时门禁及测量失败处理保持。

## 节点准备与执行诊断

LEAN宿主监管完整进程树，并独立检查节点累计时长和单次策略回调限时。内存、临时空间或进程超额时终止并清理已观察后代。未正常完成的引擎不提交成功工件。

LEAN结果摘要记录输入准备、引擎执行和结果写出耗时。完整流程还包含准入、正式提交、Result封存及报告。

同一编译流程中，不可变节点只计算一次内容身份。图身份使用节点引用（`immutable-node-content-v1`），计划身份使用已验证图引用（`validated-recipe-ref-v1`）。发布文件保留完整参数，独立加载仍从原文重建节点与图，再核对计划身份。

变更参数会改变身份。旧身份计划须重新准入，不能用于新实现恢复。

## 非分钟因果输入准备

因果Worker按来源时区提取年月并裁剪已允许的月份，不把逐行日期转换成月份字符串。日频Arrow日期列先按批次提取唯一日期，再按冻结日历和时间规则计算观测与可见时点。交付掩码按原行位置展开，保留所有证券行、重复日期和原次序。实际交付日期、来源分区、核心时间事实及窗口门禁保持原合同，不跨运行保存准备结果。

日频时间转换按不可变日期内容复用交易日历和后续交易日映射，进程内最多保留8组。日期内容变化重新构建，逐批日期格式、时区、交易日归属及可见时点判断继续执行。

分钟流在完整消费时完成来源身份和状态计数，输出manifest按实际访问构造并在本次流内复用。未完整消费、提前关闭或读取失败的流不提供manifest。仅消费行情批次的调用不计算未使用的计划与语义摘要。

普通Worker的进程树每次只枚举一次候选后代。再逐父子边核对当前父进程出生时间及子进程不早于父进程，排除旧父PID复用链。已观测且出生身份相符的存活孤儿继续计量。Windows策略Worker的进程采样查询本会话Job成员，计入Supervisor、启动进程、Worker及已观测的存活后代。

启动绑定前沿用进程树扫描。采样查询句柄即时关闭，Worker持有的Job继续负责退出时清理后代，内存、进程数和临时空间额度保持。

共享资源池的父子心跳与公开状态读取在同进程内按状态目录串行。再取得跨进程锁，两层等待均有时限。子租约续约同时更新仍存活祖先的心跳。本进程父租约维护已经失败时，子租约不能代为恢复。

续约失败立即记录脱敏首因，节点资源交接与并行监督轮询传播故障。并停止失效租约下的计算进程树。串行计算在已有资源检查交接点停止。不支持中断的同步调用需返回后才能交接故障。

失败诊断保留首因类别与消息，失败后的资源观测或释放错误单独记录。租约过期时间仍为原配置，默认30秒。

DAG计算进程的后代归属、RSS和进程数共用同一真实快照。Supervisor与后台在采样完成后的50毫秒内复用读数。后台不为已有周期读数重复争锁或枚举全机。

首次绑定、绑定PID或出生身份改变，以及根进程退出时立即测量。已观测且仍存活的孤儿继续计入节点。

临时空间按独立的一秒周期计量，新增目录范围、external提交及节点最终退出强制刷新。最终退出也强制刷新进程读数。资源峰值与越界拒绝规则保持一致。

正式模型生产节点的成功checkpoint绑定实际工件。本次运行完整验收文件后，可复用生产时计算的表逻辑声明。绑定时核对元数据身份、表文件集合、Parquet页脚行数及分区声明。显式模型导入仅向split交接经验证的内建生产证明，模型小表继续普通验收。

恢复与跨运行复用重新检查来源。裸目录或没有正式生产证明的工件仍首次完整验收记录。并行Worker沿Supervisor描述通道取得本次来源的声明副本，不携带可变验证会话。

消费时继续核对来源文件和表声明变化，不重新构造整张宽表的Python标量。全表、列投影、批次回调与分区流式消费者共用同一只读事实查询。没有证明时沿原单遍记录验收，不额外预扫。

读取继续检查实际行数、分区顺序及fold角色，按角色筛选时不读取未请求的最终留出分区。

`run --no-node-timeout`取消主DAG节点累计运行时长限制，有效预算以`wall_seconds=0`记录，调用参数随运行冻结，恢复沿用原设置。算子声明预算保持原值。内存、临时空间、进程额度、资源租约等待与单次策略回调限时继续生效。

`--reuse-failed-run-root` 配合显式 `--require-reused-node` 也支持已中断的非终态来源。来源必须具有绑定该run的有效存活记录，原owner与事件写者均已退出。导入前后核对来源身份和事件链稳定。

框架只读来源，不补写失败状态。指定成功节点及其祖先继续按当前实现、输入、PIT和checkpoint完整验收。活动运行或缺少身份记录时拒绝。
