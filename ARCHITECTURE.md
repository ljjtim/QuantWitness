# QuantWitness 如何组成

QuantWitness 面向个人的 AI 协作研究：用声明固定问题、数据、时点和算法，按依赖执行，发布 Result，再生成报告或开发反馈。

## 三层职责

| 层 | 解决的问题 | 主要组件 |
| --- | --- | --- |
| 研究提出 | 检验什么，下一轮试什么 | 人、可选 RD-Agent、ResearchPackage |
| 研究执行 | 数据何时可见，计算如何排序，交易和账户如何演化 | Catalog、数据平面、Runtime、Qlib、日线与一分钟 LEAN |
| 结果交付 | 输出来自哪里，怎样分析、导出和继续 | Result、ResultContext、报告、比较与 Dashboard |

RD-Agent 使用成功 Result 的开发指标提出下一轮候选。最终 test/holdout 不进入开发反馈；金融规则、PIT和标签读取限制在实际数据与计算入口执行。

## 一次完整研究

```text
研究问题 / 人工确认的公式 / AI 候选
  → ResearchPackage
  → 一次准备与准入
  → 执行数据、模型或交易节点，保存 checkpoint
  → 原子发布 Result
  → 报告 / 比较 / 分析 / 导出 / 开发指标反馈
```

独立 `package lint` 用于主动诊断。Workspace `execute` 在同次流程内交接准备好的声明与编译对象，持久计划用于后续执行及恢复；新进程仍检查实际来源和输入。

恢复复用身份、输入和配置相符的成功节点。Result 组装失败可接续组装，发布成功后保持只读。holdout按持久账本消费，失败打开不重置资格。

## 模块与图册

- [模块职责与代码边界](docs/architecture.md)
- [六种视角的图册](docs/architecture/README.md)
- [运行与恢复](docs/runtime.md)
- [正式结果与消费](docs/evidence.md)

算法正确性通过开发期手算、确定性案例和必要差分测试验收。数据采集、数据库修复、因子发布和实盘交易按各自独立流程授权。
