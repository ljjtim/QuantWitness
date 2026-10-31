# 六种视角，看懂个人AI研究

图册描述当前 ResearchPackage → 一次准备与准入 → 执行 → Result → 直接交付。页面可以展开节点说明；SVG用于静态预览。箭头表示主要工作交接，不是完整Python导入图。

| 问题 | 页面 | 预览 |
| --- | --- | --- |
| 从想法到交付经过什么 | [总体概览](overview.html) | [SVG](overview.svg) |
| 核心模块怎样分工 | [模块职责](research-pipeline-architecture.html) | [SVG](research-pipeline-architecture.svg) |
| 数据、时点与结果如何关联 | [数据追溯](dataflow-research-lineage.html) | [SVG](dataflow-research-lineage.svg) |
| 一项研究怎样推进 | [研究流程](workflow-research-task.html) | [SVG](workflow-research-task.svg) |
| 准备、准入和执行怎样交接 | [调用时序](sequence-research-run.html) | [SVG](sequence-research-run.svg) |
| 中断、恢复与结果发布如何区分 | [运行状态](lifecycle-research-run.html) | [SVG](lifecycle-research-run.svg) |

## 职责与边界

个人确认问题、公式和范围，可选RD-Agent生成候选。开发反馈来自成功Result的开发指标；最终test/holdout不进入循环。Qlib提供模型与处理器，日线与一分钟项目策略由LEAN执行。实际消费者处理PIT、标签、账户和交易约束。

Workspace execute一次解析和编译声明，准备对象直接交给准入；独立lint用于主动诊断。持久计划仍用于后续执行与恢复，跨进程读取实际输入。Runtime提交合格checkpoint，Result原子发布，ResultContext直接服务报告、比较、分析和导出。报告失败不重跑模型或金融节点。

## 源码入口

| 步骤 | 入口 | 路径，相对src/research_pipeline |
| --- | --- | --- |
| Workspace | execute_workspace | application/workspace_flow.py |
| 准备与准入 | prepare_package / admit_package | application/research_plan.py |
| 执行与检查点 | RuntimeExecutionService.execute | runtime/execution_service.py |
| 结果封存 | ResultAssembler | results/assembler.py |
| 直接消费 | load_result_context / ResultContext | results/delivery.py |

定位记录见[source-evidence.json](source-evidence.json)，模块约定见[架构](../architecture.md)。

## 查看与维护

离线直接打开[index.html](index.html)，无需联网或安装依赖。生成器固定读取 `sources/overview.json`、`architecture.json`、`dataflow.json`、`workflow.json`、`sequence.json` 和 `lifecycle.json` 六份图源，同一份图源生成SVG和HTML。旧详细图源 `sources/detailed/` 不属于当前图册，也不是生成依赖。生成命令在仓库根执行：

```powershell
node research_pipeline/docs/architecture/render.mjs
```

可追加overview、architecture、dataflow、workflow、sequence、lifecycle选择单图。生成器只依赖Node内置模块，SVG使用微软雅黑常规或粗体。修改入口、职责或状态时同步图源、source-evidence和页面。

Pages既有页面文件名保持；是否已发布以实际部署结果为准，本地图册不依赖远端。
