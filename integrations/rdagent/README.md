# RD-Agent可选集成

本集成让AI参与定义提取、代码实现和开发研究。
可选包0.2.0配合QuantWitness 1.6.x使用。
QuantWitness执行研究包并发布Result。
研究循环只读取正式开发指标。
最终test/holdout与研究反馈隔离。

## 选择入口

| 已有材料 | 入口 | 指南 |
| --- | --- | --- |
| PDF和明确公式 | 提取、审阅、确认、实现 | [公式复现](docs/formula-reproduction.md) |
| 完整研究包和参数 | 有界参数研究 | [研究循环](docs/research-campaign.md) |
| 因子基线和候选空间 | 因子提案与反思 | [因子研究](docs/factor-research.md) |
| 固定特征和训练设置 | 前馈模型结构研究 | [模型研究](docs/model-research.md) |
| 因子与模型研究包 | 共享预算的联合研究 | [联合研究](docs/joint-research.md) |

安装步骤见[安装指南](docs/installation.md)。
首次体验使用[合成公式示例](examples/volume_concentration/README.md)。
示例包含教学PDF和虚构行情。
示例不需要数据库、密钥或付费模型。

## 公开合同

因子请求使用`rd-factor-research-v2`。
模型请求使用`rd-model-research-v2`。
联合请求使用`rd-joint-research-v2`。
研究知识使用`rd-research-knowledge-v3`。

因子请求和研究包设计共同声明`factor_space`。
项目提供字段、候选公式、方向、特征槽和资格列。
框架不按项目名称选择公式。
项目算子负责公式计算和数据可见性证明。
框架复用ResearchPackage、Workspace和Result。

旧请求、知识索引和checkpoint不用于当前会话。
历史Result及实际依赖继续保留。
新研究使用新目录并重新准入。

## 冻结请求

公式实现请求绑定确认材料和项目adapter。
`request-build`核对材料与请求模板。
模板声明归档输入、Catalog和算子源码边界。
候选代码只能修改已声明文件。
完整字段以`FrozenRequest`及命令帮助为准。

```bash
python -m quantwitness_rdagent --help
python -m quantwitness_rdagent request-build --help
```

研究请求封存模型身份、材料、预算和响应收据。
恢复复用已完成收据。
未知调用结果会停止会话，避免重复付费。
失败执行只返回诊断，不返回金融指标。

## 数据与输出

研究数据库保持只读。
准备命令只生成材料，不执行研究。
运行目录保存会话、研究包、模型和Result引用。
跨会话知识必须能复核原会话与正式Result。
清理前保留研究结果及实际依赖。
凭据只保存在仓库外的模型配置文件。
