# 策略事件协议

项目分钟策略由LEAN执行，沿用ResearchPackage、Catalog、PIT、Result主链。安装、转换、运行和资源配置见[LEAN执行](lean-execution.md)及[安装入口](../integrations/lean/README.md)。

## 声明与支持范围

策略声明使用`research-strategy-protocol-v3`，JSON状态使用`research-strategy-json-state-v1`。StrategyDeclaration保存策略及bundle身份、state_schema_id、subscriptions、input_bindings和limits。策略业务代码在已准入bundle内，通过`entry(context, event, state)`处理事件。

| 维度 | LEAN执行合同 |
|---|---|
| 资产、频率、币种 | 人民币A股、完成的一分钟行情 |
| 行情绑定 | 一个close字段、lookback_bars=1、columnar交付 |
| 信号绑定 | 至多一个正式causal Feature绑定、lookback_bars=1 |
| 委托 | 金额买入、数量买卖、撤单；market、DAY |
| 生命周期 | initialize、before_trading、bar、execution_feedback、after_trading |
| 可提交交易的阶段 | bar、execution_feedback |
| 状态 | 有界JSON对象，绑定项目state_schema_id |
| 恢复 | 成功节点checkpoint复用；未完成LEAN节点从冻结输入重跑 |

协议schema中保留的其他意图和调度字段用于既有结果解释。当前项目执行支持范围由上表与LEAN准入共同限定。

## 正式节点

项目节点显式声明`execution_backend=lean`、`execution_model=lean_next_completed_close_v1`和冻结的`lean_runtime`。参数保存证券集合、策略声明、初始资金、滑点、参与率、完整分钟范围及资源预算。声明中的bundle_id必须对应已准入bundle。

输入为`bars: data.minute-bars.v1`、`snapshot: data.adjustment-factor-snapshot.v1`及可选`signals: research.feature-set.v1`。输出为`simulation: research.lean-simulation.v1`。历史规则和日历通过包内minute_scope冻结，规则版本按实际生效日和当时可见时间选择。

行情只使用原始未复权、已完成、质量合格的一分钟记录，可见时间等于bar结束时间。AUDIT数据不能进入策略。公司行动保留完整来源、公告时间、登记日、除权日及现金或股份到账时点。

项目候选由正式causal Feature提供，每行保留核心可见时间；同一回调只交付截至决策时点最新的完整信号批次。实际回调保存在trace，Feature源表由ResultSpec选择上游producer，输入工件与计划保持持久引用。daily_open和auction来源需要明确可见规则，全天日线不能作为开盘已知数据。

## 事件、账户与行情

event包含连续event_seq、trading_date、decision_time、kind、信号inputs、missing_instruments和只读account。初始化、盘前和盘后回调只更新state。订单反馈顺序交付，每次回调记录此前已处理的order_event_count，保持事件与账户的执行顺序。

account包含positions、active_orders、available_cash与corporate_entitlements。positions分别给出持仓和可卖数量，available_cash扣除活动金额买单尚未消费的预占。corporate_entitlements以CNY记录应收红利、待到账股份价值、锁定股份价值和附加权益，并记录待到账数量。

通过`context["market"].quote(binding_id, instrument_id)`读取当时可见close，通过`history(binding_id, instrument_id, 1)`读取同一窗口。证券必须属于冻结集合。缺失报价返回空值。除权参考价只用于权益估值，不作为真实完成分钟行情交付。

context同时提供冻结的strategy和parameters。每次回调使用独立上下文，字节额度包含事件、账户、参数和可见报价。

## 应答与委托

应答包含protocol_version、event_seq、state_version、state_schema_id、state和intents。state必须符合JSON类型及字节限制。意图身份按“event_seq:批内从零序号”组成，并保持提交顺序。

| kind | 内容 |
|---|---|
| quantity | side及正整数quantity |
| amount | buy及正Money amount，含费用现金上限 |
| cancel | 已有order_id |

非撤单意图需要证券、market、DAY、资金政策和本次可见price_input_id。数量格点、可卖数量、价格保护、停牌、涨跌停、费用和购买力由LEAN及冻结中国规则处理。

新委托只能在之后开始的完整分钟成交，价格使用该分钟Close及声明滑点。金额买入按可用现金、参考价和费用裁剪数量，累计实际成交及费用不超过金额上限。数量买卖按原生购买力、T+1和历史数量规则处理。

现金红利在付款前计入应收权益，付款后成为现金。整数送转股份在到账前计入待到账权益，到账后进入持仓，可卖日前保持锁定。登记后出售原股不撤销已登记权益。

## 资源、结果与开发验收

节点监管完整进程树的内存、进程槽、临时空间、累计时长及单次回调时长。引擎正常完成、全部冻结交易日收盘完成且输出齐全后才提交工件。成功回调不是节点checkpoint。

Result保存订单事件、交易意图、日终账户、必要回调记录、策略状态和金融指标，保留安装、配置与输入版本引用。行情和历史规则使用冻结的持久输入，报告直接读取Result。

simulation端口的正式目录为`lean/orders`、`lean/intents`、`lean/daily`、`lean/trace`和`lean/metrics`。账户终态、策略state、运行锁、策略输入及引擎完成摘要随结果保存。行情、规则、公司行动和信号来源保留为输入工件与计划引用，清理时不能删除仍被正式结果依赖的来源。

生产执行检查来源可见性、输入交付、成交与费用、现金与持仓、公司行动权益和账户转换。开发验收使用手算及确定性案例覆盖部分成交、撤单、资金预占、T+1、到账顺序和指标单位；不会在每次研究完成后重放整套金融计算。

日终收益包含首日相对期初资金的变化；回撤以期初资金和此前日终权益的运行峰值计算，年化口径在指标定义中保存。历史Result保持只读，已有计划和checkpoint不能跨实现身份变化恢复。

## 已知限制

- 当前项目策略不支持定时回调、多期行情窗口、目标权重、限价或IOC。
- 执行价格模型为后续完成分钟Close加滑点，保真度为bar_level_research_only。
- 公司行动支持毛额现金分红和整数送转；个人红利税、非整数股份分配及其他行动类型需要独立合同。
- LEAN内部状态不提供跨进程分钟级接续，未完成节点重新执行。
- 能力保持local_only范围，真实项目复用和公共能力晋级分别验收。


## 股票分红送股来源

`finance.STK_XR_XD`使用供应商当前保存的历史事件。实施公告后的首个真实交易日09:15可见，公告当日不进入策略；`addTime/modTime`仅保留为维护信息，不要求未提供的历史版本。新运行使用Catalog v5数据集和绑定，旧计划与Result保留原身份。

来源请求包含`note`。明确记载“本次转增不向原股东分配”的事件不生成原持仓转增权益；现金、红股独立判断。非零实际权益仍要求真实到账日和可卖日，不以除权日补空值。该口径使用供应商当前事件值，不声称还原历史源记录的每次修订。ETF维持原版本可见性合同。
