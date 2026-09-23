# 运维

安装依赖、离线构建和最低 Python 验收见 [安装与发布构建](release.md)。

## inspect、doctor 与恢复

先用 `inspect` 看单个 run 的节点、attempt、checkpoint 和下一动作：

```powershell
python -m research_pipeline inspect --run-root <run目录> --json
python -m research_pipeline doctor --run-root <run目录> --json
```

`doctor` 只读检查当前 Runtime 和扩展注册，不修数据库、不改 Result。

运行项目扩展前需信任其源码和依赖。worker允许正常临时文件与替换操作，不限制任意本地副作用；仅正式提交路径和内容由框架控制。普通目录、受控junction和hardlink均按真实路径核验，越出正式根仍拒绝。独立worker、超时和进程树清理用于故障隔离，不代表数据库写入授权或恶意代码沙箱。

`inspect` 会返回最后错误、尝试次数和余额、正式 checkpoint 复验原因、Result finalize 状态以及唯一建议命令；该过程只读。恢复规则：同身份中断用 resume；错误码在 retry policy 内且仍有余额才用 retry-node；可重试故障耗尽或取消后的终态重算用 rerun-from；非重试错误和任何身份变化都要修正 package/算子合同、重新 admit 并新建 run。旧 run 缺 finalize 投影时显示 `unknown`，不会把 Runtime succeeded 当成完整研究成功。

run 内的工件验证复用不落盘。即使 checkpoint 可复用，新的 `resume` 进程也会重新验证它实际
打开的 ExternalArtifact、DatasetArtifact 和分钟分区；`rerun-from` child 同样建立自己的验证
会话。若新会话发现内容、manifest、marker、schema、分区或路径身份变化，应停止恢复并调查
工件来源，不能复制旧会话状态或写持久 hash 缓存绕过。

data 节点在第三个或更后 request 失败时，不要删除 run 内的 `request-recovery`。按既有错误码
执行 `retry-node` 或在中断后执行 `resume`，Runtime 会复验已完成 request 的
DatasetArtifactRef 并只重做失效项。partial index 不是正式输出：不能手工复制到其他 run、不能
交给下游，也不能通过编辑 plan hash 或引用来强制复用；无法解析的索引会被当作没有恢复记录。

## 垃圾回收

```powershell
python -m research_pipeline gc --root <工件根> --ttl-seconds 86400 --json
python -m research_pipeline gc --root <工件根> --ttl-seconds 86400 --apply --json
```

第一条只生成计划。`--apply` 才执行工件回收。

当前命令只扫描工件根下的 `staging/` 和 `cache/`。它不扫描、不移动 ResultStore、完成态 run-root 或历史项目轮次，因此也不会自动建立这些目录之间的引用关系。真实研究归档必须另行保留 ResultStore 中的 Result 本体和对应 VerificationResult；验收摘要不能替代它们。完成态 run-root 只有在 `report`、`export-result` 已证明不再依赖它，并经过独立清理规划和授权后才可处理。

仍被故障排查、人工记录或其他工具直接引用的 run-root 继续保留。历史 legacy junction 只作为独立清理计划中的明确对象记录；当前 `gc` 不处理它，本流程也不为它提供兼容或迁移入口。

## Workspace 与 Dashboard

Workspace 分配不可复用 execution 目录，并把运行委托给同一个 run 服务。Dashboard 清单先复验 VerificationResult 与 Result 身份，再只列 `status=pass` 的结果；混合清单跳过失败项，全失败时拒绝生成可信清单，并移除上一次导出的旧清单。单结果 Viewer 和手写 Workspace 清单也执行同一门禁，不能把 Runtime succeeded、文件存在或自然语言报告当成通过。

## 数据库

research_pipeline 的数据库访问只读。采集、修复、建表、因子重算和发布不属于运维命令；这些操作必须在对应子系统获得单独授权。

## 故障处理

- 保留失败 run、事件和结构化错误码，先找根因。
- 不编辑 checkpoint、Result、VerificationResult 或 plan 来“修通过”。
- 输出目录冲突时使用新目录，不覆盖已提交工件。
- 资源不足时提高显式额度或缩小研究范围，不静默改 seed、样本或算法。`verify` 的金融复核可用 `--verification-memory-bytes`、`--verification-temp-bytes` 和 `--verification-scratch-root` 单独配置；额度不足时整次失败，不会跳过 canonical/TCA 表或发布部分通过结果。
- Catalog/PIT 阻断时返回数据准入阶段，不能在 Runtime 绕过。

## 检查原则

只运行会改变后续动作的检查。代码改动跑贴身测试；跨层主链变更再跑全量测试、Ruff、compileall、能力 containment、clean wheel 和文档审计。
