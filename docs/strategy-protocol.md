# 策略事件协议

编写通过事件接收行情并提交委托的策略。

## 开始前

- 输入：已准入策略 bundle、行情绑定、规则和账户声明。
- 输出：有序事件、策略状态、意图和正式执行表。
- 命令中的尖括号内容是待替换参数。

## 使用顺序

1. 声明协议、订阅、绑定和额度。
2. 实现 `entry(context, event, state)`。
3. 只从当前上下文读取行情和账户。
4. 按本页结构返回状态与意图。
5. 检查正式回调轨迹及订单事件。

项目日线与一分钟策略由LEAN执行，沿用ResearchPackage、Catalog、PIT、Result主链。安装、转换、运行和资源配置见[LEAN执行](lean-execution.md)及[安装入口](../integrations/lean/README.md)。

## 声明与支持范围

策略声明使用`research-strategy-protocol-v4`，JSON状态使用`research-strategy-json-state-v1`。StrategyDeclaration保存策略及bundle身份、state_schema_id、subscriptions、input_bindings和limits。策略业务代码在已准入bundle内，通过`entry(context, event, state)`处理事件。

| 维度 | LEAN执行合同 |
|---|---|
| 资产、频率、币种 | 人民币股票、ETF与真实期货合约，日线1d或一分钟1m |
| 行情绑定 | 一个OHLCV字段子集、lookback_bars>=1、columnar交付 |
| 信号绑定 | 至多一个正式causal Feature绑定、lookback_bars=1 |
| 委托 | 现货金额买入、数量买卖、撤单，market、DAY。期货显式手数开平与撤单，market/limit、DAY/IOC |
| 生命周期 | initialize、before_trading、bar、execution_feedback、after_trading |
| 可提交交易的阶段 | bar、execution_feedback |
| 状态 | 有界JSON对象，绑定项目state_schema_id |
| 恢复 | 成功节点checkpoint复用。未完成LEAN节点从冻结输入重跑 |

策略声明须同时满足协议schema、上表范围与LEAN准入。

## 正式节点

项目节点显式声明`execution_backend=lean`和冻结的`lean_runtime`。一分钟使用`execution_model=lean_next_completed_close_v1`，日线使用`lean_next_session_open_v1`。参数保存证券集合、策略声明、初始资金、滑点、参与率、完整分钟范围及资源预算。声明中的bundle_id必须对应已准入bundle。

一分钟输入为`bars: data.minute-bars.v1`。日线bars绑定同一`data.columnar-bundle.v1`内的`bars_request_id`完整OHLCV请求和`open_request_id`独立开盘价请求。现货两种频率均输入`snapshot: data.adjustment-factor-snapshot.v1`。

期货不使用现货snapshot，独立结算来源按[期货执行合同](lean-futures.md)绑定。两类资产可输入`signals: research.feature-set.v1`，现货组合另可接入正式targets端口。输出为`simulation: research.lean-simulation.v2`。

历史规则和日历通过包内minute_scope冻结，规则版本按实际生效日和当时可见时间选择。

行情只使用原始未复权、已完成、质量合格的记录。一分钟可见时间等于bar结束时间。完整日线按Catalog的next_session_open在下一交易日开盘可见。

独立开盘价按session_open取得，不交付当天尚未知的high、low、close和volume。AUDIT数据不能进入策略。公司行动保留完整来源、公告时间、登记日、除权日及现金或股份到账时点。

项目候选由正式causal Feature提供，每行保留核心可见时间。同一回调只交付截至决策时点最新的完整信号批次。实际回调保存在trace，Feature源表由ResultSpec选择上游producer，输入工件与计划保持持久引用。daily_open和auction来源需要明确可见规则，全天日线不能作为开盘已知数据。

现货组合的targets由运行内顺序读取器按执行日交付。读取器先只读时间列，完整检查决策早于执行、09:30执行时点及跨文件排序和重复。目标载荷随后只顺序读取一次，保留有界批次及下一条目标。未到执行日的权重不进入当日信号，零权重清仓事实继续完整交付。

## 事件、账户与行情

event包含连续event_seq、trading_date、decision_time、kind、信号inputs、missing_instruments和只读account。初始化、盘前和盘后回调只更新state。订单反馈顺序交付，每次回调记录此前已处理的order_event_count，保持事件与账户的执行顺序。

account包含positions、active_orders、available_cash与corporate_entitlements。positions分别给出持仓和可卖数量，available_cash扣除活动金额买单尚未消费的预占。corporate_entitlements以CNY记录应收红利、待到账股份价值、锁定股份价值和附加权益，并记录待到账数量。

股票确认退市生效后，剩余股份按零价值计入研究账户。保留数量、估值来源及生效时点，不产生卖出成交或现金收入。已登记现金权益继续按事件到账。普通停牌或行情缺失不触发退市归零。

期货账户按long/short与today/yesterday分别给出持仓，并提供settled_cash、unrealized_pnl、equity、margin、frozen_funds、available_funds及risk_active。金额使用Money的人民币分，quantity使用手数。正负浮盈进入已声明购买力，多空保证金不相抵。

通过`context["market"].quote(binding_id, instrument_id)`读取当时可见报价，values只含绑定声明的OHLCV字段。`history(binding_id, instrument_id, count)`读取不超过声明lookback_bars的历史窗口，所有成员须在当前模拟时钟前可见。证券必须属于冻结集合。

缺失报价返回空值。除权参考价只用于权益估值，不作为真实完成分钟行情交付。

context同时提供冻结的strategy和parameters。每次回调使用独立上下文，字节额度包含事件、账户、参数和可见报价。

## 应答与委托

应答包含protocol_version、event_seq、state_version、state_schema_id、state和intents。state必须符合JSON类型及字节限制。意图身份按“event_seq:批内从零序号”组成，并保持提交顺序。

| kind | 内容 |
|---|---|
| quantity | side及正整数quantity |
| amount | buy及正Money amount，含费用现金上限 |
| cancel | 已有order_id |

非撤单意图需要证券、market、DAY、资金政策和本次可见price_input_id。数量格点、可卖数量、价格保护、停牌、涨跌停、费用和购买力由LEAN及冻结中国规则处理。

期货quantity意图另须显式`position_effect=open/close/close_today/close_yesterday`，使用market或limit以及DAY或IOC。limit需要按合约尺度声明限价，amount和现货资金政策不适用。普通close按冻结桶顺序拆腿，禁止超平。

反向open保留双向仓。风险阶段暂停策略交易，换月与主动到期退出由冻结计划控制。

一分钟新委托只能在之后开始的完整分钟成交，价格使用该分钟Close及声明滑点。日线信号在下一交易日已可见开盘价上执行。金额买入按可用现金、参考价和费用裁剪数量，累计实际成交及费用不超过金额上限。数量买卖按原生购买力、T+1和历史数量规则处理。

现金红利在付款前计入应收权益，付款后成为现金。整数送转股份在到账前计入待到账权益，到账后进入持仓，可卖日前保持锁定。登记后出售原股不撤销已登记权益。

## 资源、结果与开发验收

节点监管完整进程树的内存、进程槽、临时空间、累计时长及单次回调时长。引擎正常完成、全部冻结执行窗口完成且输出齐全后才提交工件。完整会话完成收盘动作。

盘中窗口按实际结束时刻保存账户，不提前撤销DAY订单或登记收盘权益。成功回调不是节点checkpoint。

Result保存订单事件、交易意图、日终账户、必要回调记录、策略状态和金融指标。保留安装、配置与输入版本引用。行情和历史规则使用冻结的持久输入，报告直接读取Result。

simulation端口的正式目录为`lean/orders`、`lean/intents`、`lean/daily`、`lean/trace`、`lean/fills`、`lean/positions`、`lean/account_events`、`lean/lifecycle`和`lean/metrics`。现货 fills 和 positions 专用表为空。现货 lifecycle 和 account_events 可保存退市及终态观察事实，期货保存实际成交腿、方向桶、资金和生命周期事实。

账户终态、策略state、运行锁、策略输入及引擎完成摘要随结果保存。行情、规则、公司行动和信号来源保留为输入工件与计划引用。清理时不能删除仍被正式结果依赖的来源。

生产执行检查来源可见性、输入交付、成交与费用、现金与持仓、公司行动权益和账户转换。开发验收使用手算及确定性案例覆盖部分成交、撤单、资金预占、T+1、到账顺序和指标单位。不会在每次研究完成后重放整套金融计算。

权益收益包含首个观察值相对期初资金的变化。回撤以期初资金和此前权益观察值的运行峰值计算，年化口径在指标定义中保存。分钟指标使用`daily_execution_window`，完整日与盘中终态都保留实际`valuation_at`。

年化和日频风险指标按252个交易日期换算，包含不完整首尾日期。这类样本不宜与完整交易日样本直接比较。历史Result保持只读，已有计划和checkpoint不能跨实现身份变化恢复。

## 已知限制

- 当前项目策略不支持定时回调。现货限于market/DAY，期货另支持limit/IOC。行情窗口须显式声明。Qlib组合通过正式targets端口交付冻结权重。
- 分钟按后续完成分钟Close加滑点成交，日线按下一交易日开盘成交，保真度为bar_level_research_only。
- 公司行动支持毛额现金分红和整数送转。个人红利税、非整数股份分配及其他行动类型需要独立合同。
- LEAN内部状态不提供跨进程分钟级接续，未完成节点重新执行。
- 能力保持local_only范围，真实项目复用和公共能力晋级分别验收。


## 股票分红送股来源

`finance.STK_XR_XD`使用供应商当前保存的历史事件。实施公告后的首个真实交易日09:15可见，公告当日不进入策略。`addTime/modTime`仅保留为维护信息，不要求未提供的历史版本。新运行使用Catalog v5数据集和绑定，旧计划与Result保留原身份。

来源请求包含`note`。明确记载“本次转增不向原股东分配”的事件不生成原持仓转增权益。现金、红股独立判断。

非零实际权益仍要求真实到账日和可卖日，不以除权日补空值。该口径使用供应商当前事件值，不声称还原历史源记录的每次修订。ETF维持原版本可见性合同。


## 常用名称

| 名称 | 含义 |
| --- | --- |
| ResearchPackage | 数据、算法、样本、指标和预算的完整声明 |
| Catalog Lock | 已确认的字段、来源和可见性说明 |
| PIT（按时点可见） | 过去某时刻只使用当时已知的信息 |
| bar（周期行情） | 一个日线或分钟区间的行情记录 |
| OHLCV（开高低收量） | 开盘价、最高价、最低价、收盘价和成交量 |
| Feature（特征） | 决策时已知的研究输入 |
| Label（标签） | 用于评价预测的未来目标 |
| bundle（封存包） | 完整代码、依赖或数据声明及其内容身份 |
| checkpoint（检查点） | 通过验收的节点输出和恢复依据 |
| Result（正式结果包） | 正式表、运行摘要及配置、实现和输入引用 |
| minute_scope（分钟范围引用） | 指向同一冻结日历和历史规则的小引用 |
| Supervisor | 监督运行和提交结果的主进程 |
| Worker | 执行节点计算的独立进程 |
