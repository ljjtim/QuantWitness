# 六张图，看懂 QuantWitness

README 展示六张简图，点击任一图片直接进入对应的详细交互版。详细版保留完整模块、连接关系、职责说明和状态约定；可以切换浅色/深色主题与四种视觉风格，开关动态连线，搜索节点并追踪关系。点击节点可高亮，再次点击同一节点取消；空白处也可清除选择。

[![总体概览](overview.svg)](https://ljjtim.github.io/QuantWitness/overview.html)

**在线：** [交互图册](https://ljjtim.github.io/QuantWitness/)。**离线：** 下载源码后用浏览器打开本目录 `index.html`，无需安装 Node 或联网。GitHub 的 HTML 文件页只显示源码，不能代替图册浏览。

| 你想问的问题 | 交互图 | 静态预览 |
| --- | --- | --- |
| 一项研究从想法到交付要经过什么 | [总体概览](overview.html) | [SVG](overview.svg) |
| 14 个核心模块与外部组件怎样协作 | [总体架构](research-pipeline-architecture.html) | [SVG](research-pipeline-architecture.svg) |
| 一项结果能追溯到什么 | [数据追溯](dataflow-research-lineage.html) | [SVG](dataflow-research-lineage.svg) |
| 一次研究该按什么顺序做 | [研究流程](workflow-research-task.html) | [SVG](workflow-research-task.svg) |
| 一次 run 内谁先调用谁 | [调用时序](sequence-research-run.html) | [SVG](sequence-research-run.svg) |
| 中断、失败与成功如何区分 | [运行状态](lifecycle-research-run.html) | [SVG](lifecycle-research-run.svg) |

## 概览与总体架构的区别

总体概览按研究者的任务组织，不展开全部代码模块。总体架构保留 `catalog`、`packages`、`data_plane`、`application`、`runtime`、`research`、`domain`、`simulation`、`results`、`evidence`、`extensions`、`cli`、`operations`、`platform` 共 14 个模块，并标出外部输入、可选 AI 与算法组件。两张图对应不同阅读深度。

## RD-Agent 在哪

RD-Agent 位于研究提案层，接收人的问题与已确认约定，提出候选或生成代码。它经正式研究入口执行；拿回的是已通过验证的开发指标，而不是最终留出集的答案。

Qlib 位于计算层，提供已接入的模型、处理器、表达式等组件。QuantWitness 的原生仿真负责声明范围内的交易和账户处理，结果再进入独立验证。详细模块目录见[架构与代码边界](../architecture.md)。

## 图的含义与边界

六张图覆盖研究主链、数据入口、结果封存、验证和运行状态，源码位置见 [source-evidence.json](source-evidence.json)。总体架构详细图的箭头表示调用或依赖；总体概览表示工作交接，其他图按各自图例阅读。

- **总体概览：** 人可以直接发起研究，也可以使用可选 RD-Agent；AI 的开发反馈有独立验证前提。市场标识采用 `cn_stock`、`cn_etf`、`cn_future`。
- **总体架构：** 展示核心模块和外部组件。运行产物由 CLI 调用 `results` 封存；`operations` 与 `platform` 提供全链支持。
- **数据追溯：** 输入数据与准入计划分别进入取数步骤；报告同时读取 Result 和独立验证，不用检查单代替数值表。
- **研究流程：** Runtime 节点完成、CLI 封存结果、独立验证是不同阶段，不能合并为一次“成功”。
- **调用时序：** 画的是普通列式取数路径。分钟扫描、归档输入与已提交节点复用有各自入口。
- **运行状态：** 区分实际写入路径与状态表允许的转移。`cancelled` 是允许状态，但当前执行主控没有直接写入路径，不能据此推导出取消命令。

node、attempt、holdout 与 Result finalize 各有自己的状态约定。运行图突出使用者最需要判断的阶段，细节在可点击节点说明与[运行文档](../runtime.md)中。

## 函数与接口定位

图中的模块框是职责边界；时序箭头表达调用与返回顺序，不逐项展开参数。需要查看真实签名时，从这些入口定位：

| 图中步骤 | 实际入口 | 源码 |
| --- | --- | --- |
| Workspace 完整流程 | `execute_workspace` | `cli/commands/workspace_flow.py` |
| Catalog 编译 | `compile_catalog` | `catalog/compiler.py` |
| 查询准入 | `admit_query` → `AdmittedQueryPlan` | `data_plane/admission.py` |
| 普通数据节点 | `execute_operator_graph_data` | `application/grid_data.py` |
| 普通列式物化 | `materialize_dataset_plan` | `data_plane/service.py` |
| 执行与检查点 | `RuntimeExecutionService.execute` | `runtime/execution_service.py` |
| 结果封存 | `ResultAssembler.finalize`，内部使用 `ResultStore` | `results/assembler.py`、`results/store.py` |
| 独立验证 | `verify_result` → 包含 `VerificationResult` 的上下文 | `evidence/verification_result.py` |
| 报告与比较编排 | `_execute` | `cli/commands/evidence_lifecycle.py` |

表中路径相对 `src/research_pipeline/`；RD-Agent 位于独立的 `integrations/rdagent/`。完整定位记录见 `source-evidence.json` 的 `interfaces`。

## 维护图册

- `sources/*.json`：README 简图的作者文件。
- `sources/detailed/*.json`：详细交互图的作者文件，保留完整的节点、关系和说明卡片。
- `render-previews.mjs`：生成六张简图 SVG 和图册首页。
- `render.mjs`：生成简图，并通过 Archify 3.0.1 生成六张自包含详细 HTML。

浏览图册无需安装依赖或联网。重新生成时，需要 Node 与 Archify 3.0.1；从上游 `tt-a1i/archify` 的 v3.0.1 发布包解压后，在本目录运行（将路径改为自己的安装位置）：

```bash
node render.mjs /path/to/archify
```

只更新某一图使用 `node render.mjs /path/to/archify architecture`；类型还包括 `overview`、`dataflow`、`workflow`、`sequence` 和 `lifecycle`。只更新 README 简图使用 `node render-previews.mjs`。SVG 文字使用微软雅黑常规或粗体。详细 HTML 保留上游交互组件及字体许可，见根目录 `THIRD_PARTY_NOTICES.md`。

修改模块职责或状态约定时，同步更新简图、详细图源、源码依据与渲染结果；两个版本的阅读深度不同，事实口径相同。

## 在 GitHub 上启用在线图册

仓库 Settings → Pages 中选择 **GitHub Actions**，图册变更合入 main 后自动运行 **Architecture pages** 工作流，也可手动触发。工作流只上传六张交互图、SVG 预览及图册首页，不上传本地研究记录、数据或开发目录。

部署地址为 `https://ljjtim.github.io/QuantWitness/`。README 中的图片链接以此为目标；首次启用和成功部署是在线链接可用的前提。发布配置不等于已经完成远端部署。
