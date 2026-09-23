# 架构与边界

## 主链

```text
ResearchPackage
  → Catalog/PIT Admission
  → Typed Operator DAG
  → Runtime
  → self-contained Result
  → independent VerificationResult
```

项目 extension 和 core 算子只在实现来源处分叉；从 lint 起回到同一条主链。

## 模块职责

- `catalog/`：数据集、字段、可见性、修订、binding 和漂移合同。
- `packages/`：ResearchPackage schema、纯编译和 ResultSpec。
- `data_plane/`：唯一数据库读取边界，输出列式工件。
- `extensions/`：受控项目 bundle、算子合同和准入。
- `runtime/`：typed DAG、事件、checkpoint、资源租约和恢复。
- `domain/`、`simulation/`：金融时间、市场规则、订单、成交和账本。
- `research/`：研究算法、标签、统计、holdout 和负控制，不直接查库。
- `results/`：自包含 Result 的选择、原子发布和只读快照。
- `evidence/`：独立读取 Result，重算 validity 并生成 VerificationResult。
- `cli/`：参数解析和编排，不复制领域算法。

## 单一事实源

- 能力：`capabilities.json`
- 数据：Catalog Lock
- 研究：ResearchPackage
- 通用算子：唯一 OperatorDefinition manifest
- 项目实现：计划内已准入 bundle 闭包
- 运行：当前 invocation、事件链和 checkpoint
- 结果：ResultStore
- 可消费结论：VerificationResult

Markdown 只解释这些合同，不替代它们。

## 依赖方向

平台和领域层不反向依赖项目目录。core 实现不得读取 `project_extensions` 或某个研究包；项目 bundle 也不得反向导入框架内部、数据库兼容层、采集器或因子发布器。

Result 不执行研究算法，Verifier 不导入生产仿真实现。两者可共用纯 schema 合同，但金融守恒和 TCA 必须由独立实现重算。

## 数据安全

研究数据库只读。准入和运行都记录 size/mtime 前后不变；任何采集、修复、建表、因子重算或发布属于另一条需单独授权的流程。
