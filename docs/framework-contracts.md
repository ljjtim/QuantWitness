# 框架约定：从研究输入到结果交付

QuantWitness 面向本机研究：正确取数、按当时可见信息计算、执行金融规则、保存可继续使用的 Result。研究声明、运行与报告共用一条主链。

## 主链与模块

研究声明 → 准备与准入 → 列式输入 → 可恢复 DAG → Result → 报告或 AI 开发反馈。

| 模块 | 负责的事实 |
| --- | --- |
| catalog | 数据集、字段、物理绑定、可见时间及持久 Lock |
| packages | 四份声明、静态诊断、算子图与正式准入计划 |
| application | 公开命令和 Workspace 共用的准备、准入、运行、恢复与结果交付 |
| data_plane | 独占只读数据访问，生成有界列式输入 |
| runtime | 调度、事件、资源、项目 Worker、checkpoint 和恢复 |
| domain / simulation | 历史规则、订单、成交、资金、持仓、费用与公司行动 |
| research | 特征、标签、模型、统计和留出消费 |
| results | 正式表、运行摘要、发布与直接消费 |
| evidence | Result 时间序列分析等仍有调用者的计算合同 |
| extensions | 项目代码、端口、依赖和资源声明及 bundle |
| cli | 显式参数、异常输出和服务委托 |
| operations / platform | 诊断、资源与纯基础合同 |

应用服务不反向导入 CLI；研究算法不直接查库；核心不反向依赖 Dashboard。包编译不执行交易或重算研究。

## 研究声明与一次准入

ResearchPackage 由 package.yaml、sources/sources.yaml、localization.yaml、spec/research.yaml 组成。中性草稿须填写来源、研究范围、时间、图、指标、结果表、费用、种子和资源。当前没有公共 Recipe。

独立 package lint 面向操作者，一次报告本地声明缺口，不打开研究数据库。普通 Workspace execute 合并准备与准入，同次解析、静态诊断和物理来源检查后交接类型对象；持久计划为跨进程执行和恢复保留。

准入检查当前 Catalog 物理 binding、实际 schema、请求边界、PIT 和输入版本。运行只接受已准入计划；实际来源、实现、参数或恢复环境变化后重新准入。源码和数据没有变化的同次对象不在每层重复扫描。

项目自定义指标使用 spec/research.yaml 的可选 metric_definitions 列表，每项采用 MetricDefinition.to_dict() 格式。指标绑定实际生产表、单位、样本和实现；一项正式指标必须命中唯一 producer。

## 数据与时间

- 数据库默认只读，输入与输出位置分离。采集、修复、建表和因子发布需要独立授权。
- QueryIR 明确字段、证券、日期、排序和预算，大表有界读取。
- 财报按公告或可见日期使用；历史成分、行业、ST、停牌、规则和复权使用相应历史快照或有效区间。
- 分钟只使用已完成 bar。同日开盘不能消费全天日线，业务日期和开盘日期按 Catalog 已批准规则对齐。
- Feature 的祖先不得包含 Label。每条 Feature/Label 保留其实际事件、可见与决策时间。
- AUDIT 输入仅供观察，不能流入特征、标签、信号、统计或交易执行。
- raw collected 分钟不要求证明供应商未保存的版本；修订数据和公司行动仍受真实版本可见性约束。

归档输入通过 input-snapshot-manifest 显式提供，与数据库来源互斥。准入冻结完整清单及当前绑定，恢复不得替换来源。时态数据保留版本流。

## 金融与模型

交易执行在拥有事实的转换处检查可用现金、可卖数量、T+1、冻结、费用、参与率、数量格点、保护价和成交顺序。公司行动按登记、除权、付款、股份到账和可卖时点执行。费用与金额使用声明精度。

项目分钟策略使用冻结的 LEAN 安装、插件和 Python bundle；后续完成分钟 Close 加声明滑点成交。未完成 LEAN 节点整体重跑，成功上游按 checkpoint 合法复用。支持范围见[LEAN](lean-execution.md)和[策略协议](strategy-protocol.md)。

日频模型按 split、fit、predict、metrics、selection、holdout 执行。处理器只拟合训练范围，候选仅按开发指标选择。序列窗口要求完整历史，与表格候选使用相同合格末端；正式评价使用 raw 标签。

最终评价绑定稳定研究系列。同系列窗口、候选、seed、期限、证券和数据版本变体共享已打开区间；读取前在系列锁内登记 opened，失败消费不重置。详情见[研究系列](holdout-series.md)。

## 运行、结果和恢复

默认主 DAG 串行，显式有界并行仍共用总预算。资源验收和正式交接完成后才发布 checkpoint；失败材料保留。输入、实现、clock/seed 或配置漂移不能当作缓存命中。

Result 原子发布，保存正式表、配置、实现与输入引用。直接消费接口见[Result](evidence.md)。报告按需读表，不扫描无关载荷，也不重训或重跑交易。声明的 claim_level 和 limitations 不因图表成功提高。

Workspace 成功返回 succeeded、Result 位置和 report_path。报告失败保留执行成功；接续沿用原 execution。所有节点成功但 Result 未发布时，原 resume 只重试组装；已发布 Result 不重新打开。

迁移和归档同时保留持久输入及 holdout 锚点。export-result 复制 Result 本体；报告摘要不能替代正式表。gc 默认只读预览，apply 仅隔离，历史清理另行批准。

## 开发验收

算法正确性使用手算案例、确定性用例和必要的差分对照。针对公告当日、未来 bar、Label 祖先、T+1、最低佣金、部分成交、跨日到账和 holdout 重复打开检查实际生产拒绝或转换。

公开能力仍按实际范围声明。合成用例证明相应工程行为，真实数据验收对应其版本、来源和窗口；命令成功不自动成为认证或盈利证明。历史结果和性能记录保持原样，新改动的收益按同输入完整流程实测。
