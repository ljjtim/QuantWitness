# 为研究分配内存、时间和进程

你可以给计算和独立验证设置资源上限，避免一次实验拖垮整台机器。预算决定允许用多少资源，不改变研究的数据时点和金融含义。下面解释默认额度、进程计数及超限后的行为。

## 参数与行为说明

内建节点的低内存默认档为 1 GiB（1,073,741,824 字节），用于 Catalog 准入、日频数据观察有效性、分钟复权快照、分钟观察和两项分钟观察有效性。其他物化、模型与仿真的 2／4／8 GiB 预算保留；日频模型 split 与 locked holdout 声明 16 GiB。该默认值来自 OperatorDefinition 的资源声明，编译进入节点计划；它不覆盖项目显式声明的较小额度，也不自动提高运行总容量。CPU、临时磁盘、进程槽与超限拒绝规则各自保持。预算声明变化要求重新准入，旧冻结计划不静默改写。

## 日频模型长表加载

日频模型 split 与 locked holdout 的默认内存分别为 16 GiB（17,179,869,184 字节）。运行总容量仍须容纳该声明；项目显式预算、CPU、临时空间、进程槽和超限拒绝规则各自生效。新额度编译进入新准入计划，已有冻结计划和已消费的 holdout 保持原样。

Feature 和 opened 后的 Label 每批最多 8,192 行，完整记录先按原 typed canonical 编码核验，再把模型所需的数值、键、时间和 lineage 字段保留到常驻长表。来源列表和其他未用于训练的字段仍参与完整摘要，分区顺序、分区行数及总行数继续核对。模型原始证据没有删列；最终矩阵保留原列序、缺失值、重复键拒绝和可见时间门禁。长表转宽表时只复制必需字段。

投影预检按模型所需列的 Parquet footer 估计常驻矩阵下界，完整校验批的 Arrow→pandas 转换另计入同一工作集。完整输入不同时合并，投影批合并时仍计输入和输出同时存活的空间。

2026-10-12 的真实 Feature 8,192 行小批包含每行 59 个来源分区标识。完整与投影 DataFrame 分别占 4,407,424 与 3,227,776 字节；按同字段周频 13,590,400 行外推，投影常驻约 4.99 GiB，合并时输入与输出约 9.97 GiB。16 GiB 为该工作集及样本组装留出空间。这个外推不是完整周频进程峰值，正式运行与独立验证继续按实测验收。

同批三次编码测量的耗时中位数从 2.892 秒降至 0.972 秒，约 2.97 倍。标量来源列表按一个字节段编码，不缓存可变对象或消费一次性迭代器；原编码字节、摘要和非法输入拒绝保持一致。小批编码速度不代表整个研究流程的加速倍数。

项目 Worker 的节点租约覆盖当前 Supervisor、当前 Worker 及其后代，RSS 与进程槽使用同一范围。当前正式调度逐节点执行，每次租约只计一次 Supervisor，不按 CPU 槽或进程槽倍乘父进程开销。其他运行的进程不进入当前 Worker 树。Worker 正常退出、测量异常或用户中断均进入进程清理；正常退出后仍检查内存、临时空间、进程槽和时间。节点外层采样覆盖 COMMITTED、目录发布与内容复验，成功事件写入前按节点预算与租约两者的较小上限核对峰值；超额或测量不可用进入失败状态。

Windows Python 3.10 的环境身份采集可能通过 `cmd.exe /c ver` 查询系统版本，该短命进程同样占用 Worker 的进程槽。公开合成示例显式声明 4 槽，覆盖 Supervisor、venv 启动器、实际 Worker 和系统版本查询；未增加 CPU 并行度，也不改变核心默认预算。项目自行启动其他进程时仍须按实际树声明额度。

项目 Worker 的内存或临时空间超额保持 `project_worker_resource_exceeded` 错误码，CLI JSON 的 `data.exceeded` 按 `memory_bytes`、`temp_bytes` 返回各超额维度的 `actual` 与 `limit`，单位为字节；同时超额时保留两项。资源测量状态和进程清理状态继续独立返回。

`ResourceBudget` 与进程测量、清理工具由 platform 提供，Runtime 和独立验证共用；原 Runtime 的预算导入入口保持可用。独立项目 Verifier 使用 `ResourceBudget` 的内存、CPU、临时磁盘和墙钟额度，进程槽默认 2，包含父进程和 Worker。`verify_result` 接受 `project_verifier_budget` 和 `project_verifier_process_slots`；未传预算时继承 `financial_oracle_budget` 的内存与磁盘值，默认 1 GiB 内存、8 GiB 临时空间、1 CPU、300 秒。`execute_project_verifier` 直接接收 `budget`、`process_slots` 和 `scratch_root`。

`verify --verification-process-slots N` 显式设置独立 Verifier 的进程槽，最少为 2；省略时仍为 2。Windows 虚拟环境的 Python 启动器可能增加一个进程层级，可按实际进程树声明 3 个槽。预算仍覆盖 Supervisor 和 Worker 后代，不忽略启动器，不修改项目算子的预算。

`workspace execute` 在运行前按 lint 返回的真实实现作用域，提示项目 Worker 与独立 Verifier 的已知启动开销；进程内 core 节点不套用 Worker 槽位建议。建议保留声明值、来源与对应参数，不提高额度。运行失败摘要另行展示实际超限的 actual/limit；没有实测值或测量失败时不推算峰值。

准备阶段按授权列声明估算 Parquet 与支持工件的复制体积，超过临时配额或实际磁盘剩余空间即拒绝。未声明列的表完整复制，采用 1 MiB 块；声明列的表按 8,192 行投影到独立文件，并约束实际写入字节。两条路径均检查资源，正式 Result 的完整性核对继续覆盖原始完整文件，详见[Verifier 输入列投影](verifier-input-projection.md)。执行阶段监督父进程与当前 Worker 树的 RSS、进程数、临时空间和时间，Worker 退出后的末端检查仍包含已经观测到的存活后代；清理等待后代退出，未完成清理不得成功。资源测量不可用、复制失败、Worker 失败、超时或资源超额均抛出稳定错误码，成功结果发布不会继续。

| 失败 | 错误码 |
| --- | --- |
| 内存超额 | project_verifier_memory_exceeded |
| 临时空间超额 | project_verifier_temp_exceeded |
| 复制前实际磁盘不足 | project_verifier_disk_space_exceeded |
| 复制失败 | project_verifier_copy_failed |
| 进程槽超额 | project_verifier_process_slots_exceeded |
| 超时 | project_verifier_timeout |
| 测量不可用 | project_verifier_measurement_unavailable |
| Worker 非零退出 | project_verifier_worker_failed |
| 清理不完整 | project_verifier_cleanup_failed |

E004 Verifier 两遍投影扫描候选 Parquet，每批最多 8,192 行，只保留发现区与评价区数值矩阵、重复键位图和选中候选序列。HAC 沿用各候选在封存表中的原始行序，发现区矩阵按日期排序；BY、DSR、PBO、SPA 的公式与固定随机种子保持不变。待著而救 Verifier 对成员表进行一次分批投影扫描，在隔离输入目录内按日期暂存投影列，再逐截面重算并删除已读取的暂存文件。混合日期行组与乱序分片均不重复扫描源表，截面内沿用原始行序，成员变化只保留相邻日期映射；标签审计按批读取所需两列。项目算法留在项目源码中。

## 已知边界

资源治理采用进程采样，项目 Worker 与 Verifier 的监督循环间隔为 20 毫秒，节点外层采样默认间隔为 50 毫秒，不是操作系统硬限额。未被采样捕获的极短峰值无法作为完整峰值证据。Verifier 输入仍复制至隔离临时目录，必须预留完整授权文件空间；列投影降低计算内存而不减少原始复制量。待著而救还需为成员投影列的日期分片预留临时空间，该空间与输入复制共同计入 Verifier 临时预算。

源码变更产生新的 bundle 身份；既有 Result 使用封存的原始 bundle，不改写历史研究或历史 VerificationResult。
