# 日频现金节点本地准入

维护者于2026-10-03批准 finance.simulation.daily-cash@1.0.0 作为既有现金交易引擎的薄接口进入公共主链，能力上限为 local_only，研究结论上限为 research_observation。

批准对象包含精确的算子版本、实现模块、定义摘要，以及 data.daily-market.v1、research.portfolio-targets.v1、research.daily-simulation.v1 三个工件身份。摘要由 runtime/operator_promotion.py 固定；定义变动需重新评审。该记录不属于通用晋级，也不改变其他算子的异构复用要求。

输入为显式ETF分类、规则、成本、日行情、目标和决策价格基准。撮合、现金、结算、公司行动、规范六表和TCA使用已有引擎。分数转目标规则放在研究项目扩展；预测必须在决策时点可用，下个交易日开盘价只用于执行。最终holdout不参与组合规则选择。

费用和金融口径依据 tests/test_daily_cash_artifact.py 的两次100股交易、总佣金0.6元、收益 -0.6/2100 手算与独立ETF金融oracle。未来决策基准和目标身份不符必须拒绝。

## 真实日频报价合同

日频报价保留三位小数，决策基准显式声明`price_scale=3`；金额、现金、费用、持仓市值和净值以分记账。先用完整报价乘总数量，再四舍五入到分，初始现金超过分精度时拒绝。3.947元成交100份的金额为39470分，成交7份为2763分；独立金融与TCA复核使用相同明示单位各自计算。

维护者批准真实模型组合验收范围内的精度修正已完成实现审阅，能力仍为`local_only`。新执行身份不与旧checkpoint混用；历史Result保持原样。日频金额合同为分，旧公开API自行构建的非两位报价结果须按其原始合同解释，不承诺直接重新运行时保持旧整数单位。

## P2 等价结构迁移复核

2026-10-07，按维护者“完成 P2”的实施授权复核现货规则、费用和候选成交的职责抽取。384 组现货成交／拒单与改前实现精确一致，9 个分钟场景的六表、语义与结果身份一致；独立静态审阅确认金融口径、拒单无副作用和事件顺序保持。实施记录见仓库任务 `rp-native-simulation-refactor` 的 `p2-implementation.md`。

`market_rules` 负责现货政策与交易限制，`matching` 只读试算，`costs` 负责金额与费用，`cash_market` 应用获准候选的事件。精确源码依赖和定义摘要由 `runtime/operator_promotion.py` 同步登记；身份不符仍拒绝，新执行身份重新准入，旧 checkpoint 不跨身份复用，历史 Result 保持只读。

算子和三类工件身份不变，能力仍为 `local_only`，研究结论仍为 `research_observation`，异构真实项目复用尚未完成。

## 2026-10-07 P3 公共执行与生命周期复核

维护者批准 P3 的公共执行、目标与订单管理、结果封存和独立验证变更。日频现货通过 ExecutionEngine 按盘前结算／公司行动、目标调和、收盘估值和订单到期推进；Broker 只持有订单状态，资金和持仓仍归唯一账本。

新结果使用 `research-simulation-result-semantics-v2`、`research-order-lifecycle-v1`，ETF 日频上下文为 `research-daily-etf-financial-context-v3`，独立金融验证为 `result-bundle-financial-oracle-v6`。生命周期分区随 Result 封存，六表保留原成交摘要。新增支持 schema 是本次已批准证据合同的一部分，不增加交易能力。

精确源码依赖包括 engine、broker、execution_clock、target_execution、orders 和 order_lifecycle。定义摘要按最终源码由 `runtime/operator_promotion.py` 固定；实现和验收证据见任务 `p3-implementation.md`。旧 checkpoint 不跨身份复用，历史 Result 保持只读。能力上限仍为 local_only，研究结论仍为 research_observation，异构复用条件保持不变。

## 2026-10-08 P3 边界与 P4 公共账本复核

按维护者继续修改并进入 P4 的批准，分钟目标生成、执行与结果投影分别归属 target_execution、minute_orders 和 result_collector。延迟目标保留可见决策证据，迟到旧行情不覆盖较新估值；日频期货迁入公共 Engine 与唯一会话账本。日频现金继续使用原现货账本与政策，新增期货支持 schema 不增加现金节点的交易能力。

金融 oracle 更新为 `result-bundle-financial-oracle-v7`，精确源码身份登记在 `runtime/operator_promotion.py`，验证与变更范围见任务 `p4-implementation.md`。定义变化要求重新准入，能力仍为 `local_only`、研究结论仍为 `research_observation`；异构复用和通用晋级要求保持。

## 2026-10-08 P6／P6A 历史规则及现货账户

按维护者完成 P6／P6A 的批准，`finance.simulation.daily-cash@1.0.0` 新增可选 `market_rules`、`corporate_action_records` 和 `account`。旧 ETF profile 保留；逐证券历史规则支持股票或 ETF，明示来源、可见时间、有效期、数量格点、交易状态和费用。普通 CNY 现货账户新增期初市值、取得批次、税权、核定与扣收、换股和非交易退市权益，现金继续由唯一 Ledger 记账。

新增 `research-daily-cash-financial-context-v1`、`research-spot-account-context-v1` 和账户估值模型 `cash_plus_positions_and_account_rights_v1`；完整输入及衍生事实随 Result 封存，独立账户 oracle 重建金额、数量和净资产调整。精确定义摘要仍登记于 `runtime/operator_promotion.py`，节点必须重新准入，旧 checkpoint 不跨身份恢复。

本次批准扩展该节点的有来源研究输入，能力上限继续为 `local_only`，结论上限为 `research_observation`。真实来源与 ResearchPackage 验收按声明范围认定；阶段证据见任务 `p6-implementation.md`。

普通结算产生的 T+1 释放及退市款到账事件与账本一同进入账户上下文；独立 v8 金融复核据此校验应收转现金和可卖数量。当前正式股票、ETF 合成研究包均通过 run、verify、report 和失败运行账户 checkpoint 复用；精确身份由 `runtime/operator_promotion.py` 登记，真实账户来源范围不因工程验收扩大。

## 融资信用账户范围

2026-10-09用户批准P7B实施，日频节点增加`account_model="financing_credit"`及`credit_account`，与现货期初和显式订单共同冻结。变更覆盖授信预占、融资买入、自然日利息、还款、批准展期、信用风险、负债估值、独立金融oracle v11和报告。原`local_only`、`research_observation`准入边界保持；真实行情与假设协议的融资研究不提升为真实客户合同复原，也不构成通用能力晋级。

## 已知限制

尚未完成两个异构真实项目复用，通用晋级要求仍保留。该节点用于有显式规则和来源的日频股票／ETF研究，不提供tick、排队位置、真实成交概率或实盘能力证据。TCA为 analysis_only，附加冲击假设为零。

## P9 无行情现金清算

2026-10-10用户授权完成已具来源的剩余现金清算，缺数据场景暂缓。日频节点新增可选`non_trading_sessions`，声明事前可见的会话日期及来源；仅用于空行情、无交易指令的普通现金账户，首会话按有来源的v2现金清算退出旧仓，再推进应收到账。初始估值、会话覆盖、清算数量／成本及现金由独立oracle复核，期末非零证券缺价仍拒绝。

本次同时按退市现金清算的实际生效批次投影账户权益；分红和送转保留登记日批次规则。规则、声明及空行情事实随Result封存；新定义仍为`local_only`、`research_observation`，不跨旧定义恢复，不改变历史Result。当前精确摘要由`runtime/operator_promotion.py`登记。
