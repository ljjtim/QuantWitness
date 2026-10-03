# Runtime 与恢复

Runtime 只执行已经准入的 typed operator DAG。调用方不能传入 callable、动态模块或另一套 action dispatcher。

项目Worker在导入扩展入口前通过安装元数据核对dependency_lock，缺包或实际版本不符即拒绝。静态bundle构建不要求安装依赖，也不自动安装；依赖校验不拦截运行时动态导入。

项目扩展在可信本地代码前提下使用独立 worker。框架保留正式输入输出路径、内容完整性、超时和进程树清理，不用全局文件/导入 monkeypatch 或 audit hook 伪装安全沙箱。正常临时文件和替换操作可用；staging 外任意副作用由可信代码负责。受控 junction 和 hardlink 不因链接形态拒绝，正式路径仍按解析后的真实位置校验。

ExternalArtifact提交时，staging内部的链接项会先复制为普通内容，再原子移动，避免绝对junction因目录改名失效；hardlink保持原有内容校验，不重复复制。越出工件根或形成目录循环仍拒绝。随机发布事务令牌只用于 staging 与原子提交，不参与内容身份；相同正式文件、schema、行数、名称和类型在不同目录、进程或调度方式下生成相同 ArtifactRef。耗时、RSS、scratch 和进程数属于运行资源观测，不写入正式数据工件，也不改变下游节点、checkpoint 或 Result 身份。

## 不可变身份

依赖 distribution 使用 v2 规范内容记录身份，排除安装标记及已声明入口在解释器 scripts 目录中的环境路径包装器，完整定义见[发布说明](release.md#依赖分发身份)。同一内容记录在不同安装路径下不会改变依赖身份；数值模式仍绑定声明的数值后端，严格字节模式仍绑定全部依赖与构建制品。

旧 v1 摘要不会自动转换为 v2。恢复或复用时，已绑定依赖的身份不一致必须按现行门禁拒绝；需要重新准入并新建运行，不能编辑旧 checkpoint 或把旧摘要标成 v2。已封存 Result 与 VerificationResult 按其原有内容身份继续只读消费，不因当前依赖锁升级而重写。

研究运行身份绑定：plan、package、Catalog/PIT、数据 revision、core 与项目实现、clock、seed、mode 和父子 run lineage。机器容量、worker 数和可选跨进程治理只写入调用及运行记录，恢复时必须与首次调用一致，但不改变研究身份。

节点实现身份按共同变化的算子族绑定源码：Runtime 适配器位于 `runtime/adapters/`，横截面执行器及适配器位于 `application/grid_*.py`。`OperatorDefinition` 显式列出实现文件和共享语义依赖；`runtime_adapter_ref.dependency_modules` 参与摘要，运行前也按同一清单复核。某族修改只改变该族和真实依赖它的定义，共享模块修改则使全部消费者失效。同族不按单函数进一步拆分，不自动推断调用图。

数值兼容模式（`numerical`）不把整包构建摘要当成节点缓存身份；严格字节模式（`byte_exact`）仍要求整个构建一致。因此新构建可能要求严格字节节点重算，但其产物内容相同不会仅因上游 execution ID 改变而使下游数值节点失效。实际内容变化才沿输入引用传播。registry 改变后仍须重新准入；节点兼容不等于旧计划可以直接恢复，也不会自动搜索磁盘上的 checkpoint。

新计划使用节点局部身份：节点只绑定自身声明、实际引用的 request/query、参数、typed 输入、实现、固定时钟、seed 和现有环境兼容事实。无关指标、ResultSpec、研究语义或兄弟请求不再使该节点失效。旧计划继续按原身份投影 inspect、resume 和 retry，但不能作为跨运行复用来源。

`run --reuse-run-root <完成态run>` 可重复提供，按命令顺序尝试显式来源。来源必须在现行节点身份下 Runtime 成功且 Result finalize 成功。普通用法只机会式复用 `pure/cacheable` 节点；命中时 Runtime 重新验证 checkpoint、typed 输出、ExternalArtifact 文件、schema、行数和 marker，以普通字节复制到目标 run，再记录来源 run 和 checkpoint manifest。没有同身份候选时正常执行；发现同身份候选损坏时失败关闭。

需要保证昂贵节点绝不回退执行时，可重复提供 `--require-reused-node <节点>`。这是调用方明确选择的严格复用：指定节点及其必要上游可以不是 `pure/cacheable`，但必须在任何 Worker 启动前全部通过节点局部身份、输入 ArtifactRef、implementation digest、operator definition digest、cache profile、checkpoint、typed 输出和 ExternalArtifact 内容复验；任一节点不可复用就拒绝整个 run。裸 ResultStore、硬链接、可写目录引用、全盘扫描和常驻缓存服务都不属于该功能。

`run --reuse-failed-run-root <失败run>` 用于修正 ResultSpec 等不改变计算 DAG 的计划后继续执行。来源必须是失败终态；新旧 DAG、节点身份环境、clock 和 seed 必须完全一致。Runtime 只导入事件状态为成功的 checkpoint，并重新验证 checkpoint 内容、typed 输出和 ExternalArtifact；首个未成功节点及其下游在新 run 中重新执行。该参数不能与 `--reuse-run-root` 或 `rerun-from` 混用，也不会放宽普通跨运行缓存的 `pure/cacheable` 合同。

失败来源也可配合重复的 `--require-reused-node <节点>` 使用。此时允许修改其余 DAG，但指定节点及其全部上游必须在原事件链中成功，且局部节点合同、当前输入、实现、definition、cache profile、clock、seed、checkpoint、typed 输出和 ExternalArtifact 全部通过预检。环境兼容性仍使用各节点原有画像，`byte_exact` 不降级。全部指定节点导入后才进入 Runtime；缺少任何一个即拒绝整次运行，不回退执行。目标 `recovery-plan.json` 记录原 run、每个来源 checkpoint 和目标 run，来源记录保持只读。`resume` 与 `retry-node` 从冻结 invocation 恢复同一来源和强制节点列表，并重新完成复验。

## 执行记录

- invocation：恢复所需的显式输入和计划内 bundle 闭包。
- event log：run、node、attempt 和 checkpoint 状态。
- checkpoint：节点执行身份、输入、实现、环境、clock、seed 和 typed 多端口输出。
- external artifacts：原子提交的大工件。
- completion metadata：ResultAssembler 和 Verifier 需要的运行摘要与 validity 引用。
- result finalize projection：只供 `inspect` 区分 `pending/succeeded/failed`；成功时绑定已发布 Result，失败时保留脱敏错误和真实发布状态。它不是 Result，也不进入 verify。

完整事件链属于具体执行的审计记录，保留在 run-root。ResultAssembler 在发布 Result 前重放事件并核对 run、node 终态和链头；不可变 Result 只保存稳定终态摘要和正式输出身份，不复制受排队、租约取得和 attempt 时序影响的链头。相同 plan、clock、seed、实现、输入和正式输出在串行或受控并行调度下生成相同 Result 身份。

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
对应项。恢复索引当前为 `data-request-partial-index-v2`；旧版本整体视为未完成请求并重新物化，
不会复用缺失历史修订的旧事实。重算中断后，已经按当前语义完成的请求可继续逐项复用。
全部完成后仍只提交一个正式 data ExternalArtifact/checkpoint，恢复索引不会被复制进去。

`rerun-from` 会先验证父 run、checkpoint 和 ExternalArtifact，再把目标节点之前的正式工件导入
child ExternalArtifactStore。目标节点及后继只消费这些导入工件；不会复制父 `artifact` 或
`handoff` 目录，也不会为证明父目录未变而再次扫描整棵目录。内容漂移由已有 commit/checkpoint
验证直接拒绝。rerun 后代继续使用最初运行的持久 holdout 账本位置；已经打开、失败消耗或退役的
holdout 不会因为 child 输出目录变化而重新获得读取资格。根锚点作为 invocation 的不可变字段逐代
继承并核对；父 invocation 缺失、血缘循环或任一层锚点冲突时，在 Runtime 节点启动前拒绝。

## 恢复选择

checkpoint 的内容、manifest 和 `COMMITTED` 标记都在同盘 staging 写入并持久化后，
才整体原子发布到正式目录。staging 或没有提交标记的未成功节点残留可以重算；
已提交内容损坏、或成功节点丢失提交标记时拒绝恢复。

`created`、`planned` 中断可通过 `resume` 按现有事件补齐启动，不重复创建 attempt。
恢复前核对 owner 的 PID 和进程创建时间：活跃 owner 返回等待建议，不能接管或把其
attempt 标记为 lost；进程退出或显式释放 owner 后才可恢复。心跳覆盖整个 Runtime 调用，
节点间不会释放 owner。

`rerun-from` 与失败 run 复用只复制成功且通过当前身份和内容复验的节点。
失败、未开始的节点及其下游进入重算范围；指定节点及其下游仍强制重算。
独立成功分支可复用，独立未开始分支正常执行；成功节点的 checkpoint 复验失败时明确拒绝。

完成元数据保留 `node_completion_metadata[node_id]` 原始命名空间，聚合的工件摘要、
证明摘要和计数使用 `node_id/字段名`，不同参数的同类节点不会互相覆盖。
输出端口仍保存在 `outputs[node_id][port]`。元数据和完整收尾记录准备成功后才提交 run 成功事件。

公共 `run`、`workspace run` 不接受 `--acceptance-proof`，旧 invocation 中的该字段也会
在计算前明确拒绝。研究复现由 ResearchPackage 声明的项目 Verifier 承担。

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

`inspect` 不改事件、attempt、checkpoint、ResultStore 或共享资源治理文件。它从现有事件投影
`waiting_for_dependencies/ready/waiting_for_resources/executing/checkpointing/finalizing`，并返回
每个节点的开始时间、阶段时间、时长、心跳、进程健康、资源申请/预留、
`attempts_used/max_attempts/attempts_remaining`、最后一条脱敏错误和使用同一恢复身份得到的
checkpoint 复验结果。共享治理存在时还会只读列出各 run 的 FIFO 请求和活跃租约。它只给一个
`recommended_action`；可执行建议同时提供 PowerShell 字符串和 argv。只有错误码在节点
retry policy 中且仍有余额时才建议 `retry-node`；同类故障耗尽后才建议 `rerun-from`，
非重试错误返回 package/算子修正路径，身份漂移也不会被当成可重试故障。

`run --json` 失败时会读取刚写入的同一份 diagnostic 事件，在首屏返回 `failed_node`、
`root_error`、`run_root` 和可复制的 `inspect` 命令；框架不会为此创建平行 traceback 或
第二套日志。

## 项目算子

项目薄声明可选登记：

```yaml
parameter_preflight:
  module: operator
  function: preflight
```

`preflight(context)` 使用与正式 Worker 相同的 `ProjectOperatorContext` 冻结规则：对象是只读
`Mapping`，列表是 `tuple`。它在 package 编译阶段由独立短进程执行，不接收输入工件、输出
目录或数据库路径，必须返回 `None`。这用于提前发现项目代码把只读映射误写成可变 `dict`、
列表形状假设错误或参数组合无法解释；它不执行正式算法，也不替代 Operator 参数 schema。
没有登记钩子的既有 bundle 保持可读。

运行准入直接加载并验证不可变计划的 registry、DAG、clock/seed、PIT as-of 和
ExecutionEstimate/QueryIR 对齐，不要求额外的 admission proof 文件。来源正文继续逐文件
校验，返回实际来源事实，不对小型验证摘要再套一层 hash。

项目入口 ABI 是同步 `run(context, inputs, output_root)`。`context` 是不可变的
`ProjectOperatorContext`，`inputs` 是 Supervisor 已复验、再由当前 attempt 临时描述约束的文件输入或受控分钟分区流；入口只能在 Worker 提供的
`output_root` 下写工件，并返回 `ProjectArtifactCommit` 字典或列表。commit 必须逐端口闭合
`artifact_type/relative_path/content_hash/schema_hash/byte_size`，额外文件、漏端口、类型或字节漂移
都会由 Worker 拒绝。

项目 Worker 失败时保留稳定错误码，同时返回脱敏、限长的根异常类型和消息，不持久化完整
traceback。非分钟表输入的列投影错误会列出缺失列和已验证 schema 的可用列；未知文件会列出
当前提交清单中的相对路径。ResultSpec 指向的节点输出会在该节点提交后立即核对 Parquet
前缀，路径不匹配时节点直接失败，不再等整张 DAG 完成后才在 Result finalize 报错。

项目算子可以从普通列式输入或显式绑定的 `data.minute-bars.1m.v1` 扫描输入读取正式 request
准入事实。日期型 `as_of` 同时投影为按固定时钟时区解释的次日零时排他
`as_of_cutoff`，带时区时间则保留原绝对时点。分钟路径由 Supervisor 将已验证工件中的
request、plan hash、source revision 和来源
快照与当前 admitted plan 对齐后，只向 Worker 投影元数据；它不交付分钟文件，也不改变已有
`data.minute-bars.v1` 分区读取合同。只有节点参数声明的 request 可见，Worker 必须完整读取并
返回 metadata-only 消费轨迹，Supervisor 再核对来源身份和 schema。

admit 把已验证 bundle 和实现摘要写入计划闭包。run、resume、retry-node 和 rerun-from 从计划重建同一 registry；bundle 缺失、内容变化或源码漂移都会失败。

非分钟多文件输入使用有界 `ProjectTableInput`，输出/state 以 staging 描述流式提交；
`ProjectOutputRoot.write_batches/write_state` 每批限制 32 MiB，表格固定 8,192 行 row group。
目录工件默认保留输出端口前缀；公共工件合同要求文件位于 ExternalArtifact 根目录时，扩展可在
`commit_directory` 中显式声明 `publish_at_artifact_root=True`，Worker 与 Supervisor 仍逐文件核对闭合和内容；
根级 `manifest.json` 和 `COMMITTED` 由 ExternalArtifact 核心保留，项目输出不能以任何大小写形式占用。
正式 Feature/Label 通过 `causal_plan` 冻结源列、逻辑月份、键和窗口，核心每批最多调度
8,192 键并记录实际交付，合并 inherited state lineage 后由 ExternalArtifact 核心附加时间。
正式路径最低 256 MiB，不能按预算改变 carry 的键批边界。扩展没有源路径，不自报时间事实；
越界读取、继承状态超窗、缺键、伪造时间或输出超出核心合并预算都会失败。
完整声明及当前支持范围见 `../project_extensions/README.md`。

## 资源治理

普通单次运行默认总容量为 16 GiB 内存、当前进程可用的全部逻辑 CPU 和 64 GiB scratch 上限；scratch 只是临时文件硬上限，不会提前占用磁盘。OperatorDefinition 的 `resource_profile` 必须显式声明 memory、CPU、scratch、进程槽和节点 wall-time。声明是节点执行包络，也是共享准入的 reservation；Runtime 不再把整次 CLI 容量替换成每个节点的申请。总容量只负责拒绝无法承载的声明和协调多个运行。

数据节点的 memory 是一次 provider 调用可使用的进程树支持包络，不是 QueryIR 输出上限，也不是把字符串和引用长度相加得到的“Runtime 总硬上界”。admit 将其编译为 DuckDB、batch/writer 与进程余量；当前支持下限为 256 MiB，低于下限在对象统计和扫描前失败。provider 用本次有效预算设置 DuckDB memory/temp、Arrow batch 与 CPU；只有显式 resource governor 存在时，Runtime 才创建 ResourceUsageSampler，观测当前 Runtime 进程、attempt 开始后新建的后代进程以及 scratch 峰值并交给治理记录。采样开始前已经存在的控制台宿主等 Runtime 后代不属于当前节点，不能污染节点 RSS 和进程数。默认 run 不启动无人消费的采样线程。代表查询探针只做测试回归，不写新的 proof，也不进入真实成功运行的资源校准总体。

完整矩阵消费者先用 Parquet footer 下界做数据页前拒绝，再由运行时预算核对实际 Arrow/pandas 保留量。

`process_slots` 覆盖 Supervisor、Worker 及其受支持的子进程树。内建算子默认声明 1 个槽；项目算子必须在薄声明中显式填写。启用共享治理时，`--resource-process-slots` 必须至少容纳 DAG 中最大的单节点声明。CPU 和进程槽分别准入，不再用进程数推高 CPU 申请。

节点在排队时记录为 `waiting_for_resources`，取得租约并真正开始执行后才进入 `running`。事件同时记录申请向量、排队时间、取得租约时间和等待毫秒数，`inspect` 可以区分等待中断与执行中断。

普通 run 也会在 run-root 原子维护 `runtime-liveness.json`。该文件记录当前 run/node/attempt、
等待资源、执行、checkpoint 或 finalize 阶段、进程身份、最近心跳和预留资源；它用于诊断和活跃 owner 接管判定，
不进入事件 hash chain、checkpoint、Result、VerificationResult、恢复身份或跨运行复用身份。
`starting` 覆盖计划加载和节点启动前阶段，准入完成前 `run_id` 可为 `null`；
同一 owner 持续覆盖 Runtime 和 Result finalize，到本次调用返回或抛错才写入 `stopped`。
同进程内重入和跨进程接管均须等待当前调用释放；进程存活由 PID 与进程创建时间共同判定。
没有算子提供的真实总量时 `progress` 固定为 `null`，框架不按耗时猜百分比。

`wall_seconds` 覆盖整个节点 attempt，不包含排队时间。Runtime 在取得资源后建立统一截止时间，逐分区 Worker 和 causal 键批只获得剩余秒数，不能为每次调用重新获得完整时限；项目 Worker 超时会被终止并清理进程树。当前内建同步 adapter 在返回边界核对截止时间，不能把它宣称为可抢占终止。

默认路径不创建跨命令状态或租约。只有调用方显式提供共享 resource state dir 时，多个独立 CLI 才按各自节点声明申请跨进程 FIFO 额度；各维度之和未超过总容量时可以同时执行，超过任一维度时后到请求等待。等待超过显式 timeout 后拒绝，不改变金融语义。启用共享治理时，最终资源采样覆盖 checkpoint 提交标记、目录发布与内容复验，再核对 Supervisor 与本 attempt 子进程的
合计 RSS、scratch、进程数和耗时。通过后才记录 checkpoint 成功事件；超出预算或租约时记录失败，
撤销本 attempt 刚发布的 checkpoint，避免恢复复用未通过资源门禁的输出；
资源测量不可用时记录 `measurement_unavailable` 并阻断提交。需要清理失控 Worker 时仍保留进程树终止，无法递归观察时报告 `direct_process_only` 残留风险。资源观测保留实际测量值，不会静默改写后续 reservation。资源观测 v4 不读取含 v3 观测的旧共享状态，升级后必须使用新的仓库外 state dir；旧四字段项目声明也必须补齐 `process_slots` 后重新 build、lint、admit 并新建 run。

## Result 边界

Runtime succeeded 只说明计算完成。ResultAssembler 还要按 ResultSpec 选择已提交输出、检查指标 producer、输入 revision 和实现身份，再原子发布唯一自包含 Result。`inspect.finalize.status` 单独显示这一阶段；旧 run 缺该文件时是 `unknown`，不会猜成成功。若 finalize 后续步骤失败但 Result 已原子发布，投影会保留真实 Result 引用并建议进入 verify；未发布才返回 package 修正路径。可信消费必须继续运行独立 verify。
声明项目 Verifier 的新 Result 会从已准入 Plan 复制并复验唯一 bundle，保存于
`verifiers/<bundle_hash>`，读取时将其 manifest、依赖锁、提交标记和源码文件纳入精确文件闭包。
历史 Result 不迁移；没有内嵌闭包时仍要求显式提供同一身份的 Verifier bundle。
同一 Verifier 可为多个 ResearchPackage 变体声明指标定义超集。计划编译时只组合当前
Package MetricContract 实际选择的项目指标；其余定义保留在 Verifier 身份中供独立复核，
但不会进入当前计划的 MetricReachabilityProof 或正式报告。
新建 Verifier 的 MetricDefinition 必须使用 v3 测量语义，明确数量、分子、分母、观察时点和
聚合方式。这些字段进入定义摘要；公式实现仍由独立 Verifier 从正式成员表重算，字符串元数据
不冒充公式证明。历史 v2 Result 继续只读验证和消费，但不自动补写口径。
项目算子默认只能在同一个 run 内恢复。薄声明只有显式写入 `reuse_scope: cross_run`，并同时满足确定性或固定 seed、`artifact_write_scope: output_only` 和完整 bundle 身份时，才会编译为跨运行可复用节点。该声明也承诺正式输出不依赖 project/run/node/attempt 等易变 ID；公共 Runtime 不按项目名放行。
