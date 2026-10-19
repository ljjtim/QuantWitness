# 产物生命周期与回收

Runtime 只自动释放本次成功节点已经交接的载荷。正式 ExternalArtifact、checkpoint、Result、VerificationResult、Verifier 和输入不会因为某次运行成功而删除。回收记录位于 `work/<attempt>/artifact-lifecycle.json`，其中 `released_files`、`released_bytes` 是实际删除成功的文件数和逻辑字节；保留和隔离另行记录。

## 目录用途、提交与恢复

| 对象 | 用途与正式提交位置 | 恢复与保护 | 处理 |
| --- | --- | --- | --- |
| `request-recovery/<node>/data` | 普通列式请求的物化副本；完整数据节点把数据复制到 committed external | 失败或部分请求需要原副本；全部端口校验、资源通过、checkpoint 发布、attempt 和 node 成功后，确认正式 external 中每个替代文件存在 | 仅本次登记的成功数据副本自动释放；保留目录及 `partial-index.json` 诊断记录 |
| `work/<attempt>/outputs` 中已提交载荷 | Worker 结果原件；提交到 external 后由 typed 端口交接 | 必须确认 Worker 进程树退出；普通节点要求全部输出完成提交，分区节点还要求状态和最终节点提交完成 | 自动释放逐项登记的文件；不删除整个 `work`，回执、代码、诊断保留 |
| 分区 Worker 状态载荷 | 分区之间传递状态；状态另有正式 external 和分区 checkpoint | 当前节点仍运行或最终提交失败时保留；后续分区仍可能读取原状态文件 | 仅完整节点成功后随登记载荷释放 |
| 因果 Worker、活动会话、未能闭合替代文件的输出 | 特定执行协议的中间材料 | 尚未纳入普通 Worker 的交接登记；不能据文件名推断无消费者 | 保留 |
| `staging/<attempt>` | checkpoint 发布前准备 | 当前 Runtime owner、attempt 终态、COMMITTED 与 pin/lease/active 都参与保护 | 达到 TTL 且无保护时可隔离；缺少 owner 停止证据的旧 run 保留 |
| `external-artifacts/.staging/<token>` | external 检查和发布前暂存 | 新生产者在 `.staging-owners/<token>/` 自动记录归属及 `.active`；写入作用域结束、Runtime owner 停止、attempt 终结才允许考虑回收 | 满足证据、保护和诊断 TTL 后可隔离；缺少记录的历史暂存保留 |
| 根下 `.staging`、`.staging-*` | 其他生产者的暂存布局 | 没有接入当前 external owner 协议 | 只展示字节和保护原因，保留 |
| `cache/<entry>` | 原有缓存回收范围 | COMMITTED、pin/lease/active，以及存在的运行 owner 证据 | 沿用预览后隔离；不自动删除缓存 |
| `input-artifacts`、归档输入、原始行情 | 冻结计划的研究来源，可被多个运行共享 | 不因本次交接解除其他计划的引用 | 保留 |
| committed external、checkpoint、事件、invocation | 下游输入、inspect、resume、rerun 和跨运行复用 | 失败 run 内的成功节点仍可能有效 | 保留 |
| Result、VerificationResult、内嵌 Verifier | 自包含交付与独立可信消费 | Result 移出 run root 后仍应可验证 | 保留 |
| holdout 账本及研究系列锚点 | 限制留出集重复消费 | 失败消费和已打开区间不能重置 | 保留 |

## 成功释放顺序

先完成各输出及状态的正式提交和复验，再完成资源验收与节点 checkpoint 发布，等待 Worker 已退出，写入 attempt 与 node 的成功事件，最后确认旧载荷的全部文件均有正式替代位置。任何必要条件不满足都保留原件。

源路径必须属于当前节点的请求恢复目录或当前 attempt 的 Worker outputs；正式替代路径必须位于该 external 对象内并属于它的文件清单。清理不重新计算内容摘要，继续使用既有 ExternalArtifact 验证。源与替代文件尺寸无法对应时保留。`.pin`、`.lease`、`.active` 在源目录、子目录或 run 内祖先目录均可阻断释放。

只逐文件删除成功交接的载荷，按每次成功删除累计逻辑字节。删除失败会记录到回收报告，已提交研究状态保持成功。没有成功交接登记的历史目录不会由该机制补扫。中断后已有合法 checkpoint 继续走正式恢复，不为补清理而重新执行节点。

## 只读预览与隔离

```powershell
python -m research_pipeline gc --root I:/research/example-run --ttl-seconds 86400 --json
```

预览返回 `candidates`、`candidate_bytes`、`candidate_files` 与 `protected`。每个候选包含类别、相对路径、年龄、逻辑字节及文件数；保留对象提供 `reasons`。预览读取目录和现有状态，不打开数据库。

`--apply` 使用相同计划约束、根锁、路径边界和保护复查，把候选移入 `quarantine/`。计划生成后新增 pin/lease/active，或 Runtime owner 重新活动，均会阻断隔离。`--apply` 不删除隔离载荷，因此不代表空间已经释放。原有 `purge_artifact_quarantine` 库函数默认预览，只有显式 `apply=True` 才删除过期隔离目录；公共 CLI 没有新增 purge 入口。

## 已知范围

历史目录清理需要单独授权。旧 external 暂存缺少归属和停止证据时，即使超过 TTL 也保留；TTL 只代表诊断保留期结束。owner 登记失败会留下诊断和保守保护，不改变业务提交的成功或异常。小型 owner 记录不承担全局引用索引职责。

本机制不退休整个 run，也不回收共享输入、正式输出、holdout、项目候选源码或尚有消费者的状态。失败和部分完成的请求仍使用原恢复协议；因果执行输出暂未接入自动释放。目录及少量回执继续保留，逻辑释放字节不等于文件系统分配空间。
