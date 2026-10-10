# QuantWitness 如何组成

一个研究从“我想试试”到“我相信这个结果”，至少要经过三件事：用对数据、按约定计算、检查算得是否正确。QuantWitness 将这三件事分开实现，再用同一份研究配置和可追溯结果把它们连起来。

[![总体架构：RD-Agent 提案，统一执行与独立验证](docs/architecture/research-pipeline-architecture.svg)](https://ljjtim.github.io/QuantWitness/research-pipeline-architecture.html)

点击图片进入在线交互图。离线可直接打开 `docs/architecture/index.html`。其他视角和源码依据见[六张图的说明](docs/architecture/README.md)。

## 三层职责

| 层 | 解决的问题 | 主要组件 |
| --- | --- | --- |
| 研究提出 | 要检验什么？公式是什么？下一轮试什么？ | 人、可选 RD-Agent、ResearchPackage |
| 研究执行 | 数据当时可见吗？计算如何排序？交易和账户如何演化？ | Catalog、数据平面、Runtime、Qlib 接入、原生仿真 |
| 结果交付 | 结果来自哪里？能否独立检查？能否继续或重现？ | Result、Verifier、VerificationResult、报告 |

**RD-Agent 在第一层，不取代第二层的数据检查或第三层的独立验证。** Qlib 是执行层可选的模型、特征与组合组件来源。两者可以一起使用，也可以都不启用。

## 一次完整研究

```text
研究问题 / 人工确认的公式 / AI 候选
  → 研究包检查 → 数据与时点准入
  → 按依赖执行计算与回测，保存完整检查点
  → 封存 Result
  → 独立验证，生成 VerificationResult
  → 报告 / 实验比较 / 已验证开发指标反馈
```

最终 test/holdout 不回到 AI 开发循环。数据采集、数据修复和实盘交易属于另外的系统边界。

## 看哪张图

- [总体架构](docs/architecture/research-pipeline-architecture.html)：从研究想法到结果，RD-Agent 在哪里。
- [数据追溯](docs/architecture/dataflow-research-lineage.html)：输入、计划、输出和验证如何关联。
- [研究流程](docs/architecture/workflow-research-task.html)：每一步做什么，失败后从哪里继续。
- [调用时序](docs/architecture/sequence-research-run.html)：开发者排查“谁调用谁、何时封存”。
- [运行状态](docs/architecture/lifecycle-research-run.html)：区分运行成功、封存成功和验证通过。

GitHub 文件页面会显示 HTML 源码；在线浏览使用图片指向的 Pages 图册，本地浏览使用下载后的 HTML。模块目录和依赖约定见[架构与代码边界](docs/architecture.md)。
