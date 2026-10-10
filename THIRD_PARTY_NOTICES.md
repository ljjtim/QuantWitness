# 第三方代码、数据与许可

QuantWitness 自有代码采用 [Apache-2.0](LICENSE)。Qlib、RD-Agent 及其他依赖保留各自的许可证；安装或分发 QuantWitness 不会把这些上游代码重新授权为 Apache-2.0。

## Qlib

- 上游：[microsoft/qlib](https://github.com/microsoft/qlib)。当前接入版本为 `pyqlib==0.9.7`。
- 许可证：MIT。版权归 Microsoft Corporation。随本仓库保存的原始正文见 [Qlib MIT 许可证](licenses/QLIB-MIT.txt)，上游对应 [v0.9.7/LICENSE](https://github.com/microsoft/qlib/blob/v0.9.7/LICENSE)。
- 使用方式：通过依赖导入模型、数据集与处理器接口；项目示例调用 Qlib 表达式、Alpha158/Alpha360 特征定义，以及风险模型和组合优化组件。
- 主要接入位置：`src/research_pipeline/research/modeling/`、`examples/qlib_portfolio/extension/expressions.py`、`project_extensions/qlib_portfolio_risk/optimizer.py`。

QuantWitness 管理外层研究配置、数据时点、执行和验证；调用 Qlib 组件不表示本项目原创了这些算法。若再分发 Qlib 的源码副本、修改版或其中实质部分，应同时保留上游版权与 MIT 许可声明。分发包含依赖的环境或镜像时，也应保留依赖包自带的许可文件。

## RD-Agent

- 上游：[microsoft/RD-Agent](https://github.com/microsoft/RD-Agent)。当前集成固定提交为 `484776c211e4fbbeef03e0ec00d6bbee7362a4f4`。
- 许可证：MIT。版权归 Microsoft Corporation。原文见 [RD-Agent MIT 许可证](licenses/RD-AGENT-MIT.txt)，对应[上游提交的 LICENSE](https://github.com/microsoft/RD-Agent/blob/484776c211e4fbbeef03e0ec00d6bbee7362a4f4/LICENSE)。
- 使用方式：可选集成调用 LoopBase、CoSTEER 等研究调度与生成接口，上游源码按安装说明单独取得。QuantWitness 提供公式确认、请求适配、正式研究执行及开发反馈的连接代码。

核心安装不捆绑 RD-Agent 的完整源码。打包整个运行环境或复制上游代码时，继续保留其版权与许可声明。

## RQAlpha 与其他依赖

RQAlpha 是账户与执行职责设计的参考之一，当前原生仿真不调用其执行引擎。本项目不据此主张拥有 RQAlpha 的代码版权或商标，也不把其许可替换为 QuantWitness 的许可。未来引入源码片段或直接依赖时，应按实际引入的版本重新记录。

NumPy、pandas、PyArrow、DuckDB、PyTorch、XGBoost 等依赖适用各自发行版本的许可。本文列出与研究算法和 AI 接入直接相关的来源，不替代安装环境中全部依赖的许可清单。

## 数据、PDF 与示例

公开源码不附带真实市场数据库、供应商手册或未经授权的研报。使用者需自行获得行情与材料的使用权限；软件许可证不赋予行情、论文、研报或模型服务的使用和再分发权利。

公开教学示例中的虚构证券、确定性行情和自编公式 PDF 用于演示工程行为，应始终标注为合成或教学内容。它们不是真实市场观测，不构成策略收益证据。

Qlib、RD-Agent、RQAlpha 及 Microsoft 等名称仅用于说明来源和兼容关系，不表示上游对本项目的认可或背书。

## Archify 交互图册

六张详细交互图使用 Archify 3.0.1 渲染器与浏览器端组件，按 MIT 许可证分发，完整文本见 `licenses/ARCHIFY-MIT.txt`。上游项目为 `tt-a1i/archify`；版权归 tt-a1i (Archify) 与 Cocoon AI。HTML 内嵌字体的 SIL Open Font License 声明随文件保留。README 的简图使用项目自己的预览渲染器。
