# 贡献指南

QuantWitness 接受缺陷修复、文档改进、项目扩展和经审查的通用能力贡献。提交前请先确认改动属于框架机制还是某个研究项目。

## PR 提交到哪里

正式贡献统一提交到 `ljjtim/QuantWitness` 的 `main`。自己的fork用于推送来源分支；fork内部PR只用于临时验证，不作为正式贡献入口。

GitHub网页创建PR时，明确选择：

- base repository：`ljjtim/QuantWitness`
- base：`main`
- head repository：自己的fork，例如`ljjtim-research-ai/quantwitness`
- compare：包含本次修改的功能分支

在独立公开源码仓库执行，下面以当前集成分支为例；其他贡献替换账号和分支名：

```powershell
git fetch origin main
git push fork codex/qlib-real-portfolio-delivery
gh pr create --repo ljjtim/QuantWitness --base main --head ljjtim-research-ai:codex/qlib-real-portfolio-delivery --title "Qlib 与 RD-Agent 集成及组合报告" --body-file pr-description.md
```

示例假定`origin`指向上游、`fork`指向自己的fork。执行前用`git remote -v`核对；若命名不同，使用自己的remote名称。`pr-description.md`保存行为变化、金融/PIT影响、实际验证和限制，放在仓库外并传入真实路径，不随源码提交。

推送前处理与上游main的冲突，保留上游已有发布流程。创建后检查PR页面显示的目标仓库与分支，并以该PR当前提交的CI为准。PR创建、合并和版本发布分别处理；当前main的版本递增合并在CI成功后会触发既有发布流程，发起PR本身不会发布软件包。

## 开发环境

```powershell
python -m pip install ".[dev]"
python -m research_pipeline --help
```

数据库访问默认只读。测试不得依赖本机数据库、远端配额、cookie、私钥或不可再分发的数据。

## 框架与项目边界

- 项目算法、字段组合、资产规则和研究假设放在 ResearchPackage、recipe 或项目 extension。
- 不要因为一个项目需要某段逻辑，就把项目代号、固定资产池、固定因子或专用分支写入 `src/research_pipeline/`。
- 公共 Operator 只有在两个异构项目真实复用、具备独立 oracle 和攻击测试，并经过人工批准后才能晋级。
- 日频现金节点的有界人工准入仅适用于已批准的精确版本和定义，能力保持 `local_only`，见[准入记录](release/daily-cash-local-admission.md)。其他公共节点仍遵循上述通用晋级要求。
- 财报、成分、行业、状态、复权、标签和交易信号必须按当时可见信息对齐。

## 提交要求

- 增加或改变用户可见行为时，同步更新 README 或当前文档。
- 选择能发现本次具体失败的贴身测试；不要以刷新 baseline 代替修复失败。
- PR 说明应列出行为变化、PIT 影响、验证命令和已知限制。
- 不提交数据库、数据导出、缓存、日志、凭据、构建产物或本机绝对路径。

`main` 只通过受保护 PR 合入。自动化账号可以从自己的 fork 提交 PR，但不能批准或合并自己的改动。


## 扩展验收与兼容变更

从 `examples/` 中与研究问题最接近的完整项目复制到仓库外目录，保留项目自己的算法、输入快照和独立 Verifier；声明端口、依赖、资源预算以及数据可见时间。按[项目扩展](EXTENSIONS.md)构建 bundle，再完成 `package lint → package admit → run → verify → report`。构建成功只说明扩展格式有效，正式结果还须独立验证。

贡献包含一份可以手算的正确对照和一份与该算法相关的错误样本。错误应在项目计算或输入声明中构造，经正常构建与封存进入复核；封存后改文件只能证明完整性检查有效。记录 `VerificationResult.status`，CLI 命令执行成功不等于研究结论通过。提交的示例不依赖私有数据库或维护者机器路径。

改变 CLI 参数时同时更新帮助、文档和参数转发回归，并将公开回归纳入发布清单与 CI。改变扩展 ABI、工件 schema、PIT 口径或恢复语义时，说明旧计划、bundle、Result 的读取与重算影响；不能改写历史 Result。新增可选参数保持既有默认行为，破坏性变更在发布说明中明确指出受影响入口与迁移步骤。

## 性能与恢复证据

基准记录版本、操作系统、Python 和启动方式、依赖、CPU/内存、输入行数和日期范围，以及冷/热缓存条件。算法计时与 `admit/run/verify` 计时分别记录，预算与观测峰值分开。短任务至少五轮交替对照，报告中位数和范围；长任务单次样本明确标注，不以不同输入或不同缓存状态计算加速比。

恢复实验记录中断位置、已提交与未提交节点、恢复耗时和实际重算次数，逐表比较结果并重新独立验证。Worker 实验记录异常退出、超时、未提交输出和后代进程清理。只有观察到实际瓶颈，才增加缓存、合并读取或调整执行结构。

首次使用验收由不了解实现的使用者从公开源码开始，完成安装、第一份验证报告、一处参数变体和一次错误定位。记录总用时、人工编辑位置、维护者介入及卡住的步骤；自动化脚本通过与真人独立完成分别保存结论。跨平台支持由对应环境的 CI 或实际验收记录证明。

## RD-Agent 可选集成贡献

可选集成在 `integrations/rdagent` 独立安装。先完成[发行包安装验收](integrations/rdagent/docs/installation.md)，再选择其中的可认领任务。它用公开合成 Parquet 验证错误公式被拒绝、正确公式通过，不需要私有数据库或模型密钥。

只改请求与材料处理时，安装核心、可选集成及 pytest 后运行对应的 `integrations/rdagent/tests/test_request_builder.py`、`test_formula_spec.py` 或 `test_source_materials.py`。涉及 RD Loop 的测试按集成 README 安装固定上游源码和场景依赖。测试输出放在仓库外的新临时目录。

新增文件同步加入 `tools/release_allowlist.py`；可选包的示例和文档由其 `MANIFEST.in` 收入 sdist。提交前检查源码发行清单，用户可见行为变化同时更新集成文档。安装、正式验证和远端 CI 结果分别说明，不以 CLI 退出码代替 VerificationResult。


## 从可复现的小改动参与

先按[完整研究起点](docs/getting-started.md#从完整研究起点开始)运行一个公开合成项目，再选择与该项目相关的改动：

| 改动 | 推荐交付 |
| --- | --- |
| 公式边界，例如零成交量或窗口不足 | 一份手算输入、预期状态、算法与独立参考的对应检查 |
| 新的开发参数候选 | 明确参数含义、同一评价窗口、两份正式结果及比较依据 |
| Qlib 模型或处理器支持 | 训练范围、直接组件对照、无标签恢复预测和资源说明 |
| 组合或成本规则 | 目标到订单的时点关系、手算费用及独立金融复核 |
| 安装或文档问题 | 干净环境、实际命令、具体卡住步骤和修订后的运行结果 |

反馈研究问题时提供合成复现或可公开的最小输入。研究包、Result 与 VerificationResult 的引用可以用于定位；私有行情、密钥、完整数据库与本机环境不进入 PR。文档改动无需运行无关研究，计算改动则验证受影响的行为。

首次使用的自动化验收与真人体验分别记录。真人体验至少覆盖首次报告、一次参数修改和一次错误定位，记录耗时、手工编辑位置和需要维护者帮助的地方；自动化通过不能替代这份体验记录。
