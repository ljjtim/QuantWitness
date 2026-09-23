# QuantWitness 架构

QuantWitness 的正式主链是：

```text
ResearchPackage
→ package lint
→ package admit
→ Runtime typed DAG
→ Result
→ verify
→ VerificationResult
→ report
```

框架核心由 Catalog/PIT、ResearchPackage 编译、列式数据平面、Runtime、金融领域合同、模拟、研究原语、证据和 CLI 构成。数据源保持只读；运行工件写入显式工作目录。

项目特有逻辑不进入框架核心。项目使用声明、recipe 和受控 extension bundle 提供算法、字段组合、资产规则和研究假设。公共 Operator 晋级必须证明异构复用、稳定合同、独立 oracle 和攻击测试。

Result 是运行完成后封存的不可变研究结果。VerificationResult 由独立验证步骤生成，不能由 Runtime 自报成功代替。报告和比较只消费已封存的 Result 与 VerificationResult。

详细合同见 [文档索引](docs/index.md) 和 [项目扩展说明](EXTENSIONS.md)。
