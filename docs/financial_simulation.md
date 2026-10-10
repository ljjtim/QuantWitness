# 回测如何计算交易与账户

策略给出目标或订单后，回测还要回答：能否成交、成交多少、花了多少费用，以及现金和持仓怎样变化。QuantWitness 用统一的执行和账户组件处理已支持路径，再从封存事实独立复算关键数值。

当前这里讨论日线与已完成分钟线上的研究仿真，不模拟交易所逐笔订单簿。

## 参数与行为说明

订单、成交和费用归因统一进入现有 Bar/ledger 后端。日频与分钟 Bar TCA 的参数、跨 Bar 余量、
四层结果和独立 oracle 见 [Bar 级交易成本分析](bar_tca.md)；该能力不表示支持 Tick、盘口或 LOB。

完成 bar 驱动的分钟订单、容量、资产规则与前视边界见[分钟事件仿真](minute_simulation.md)。分钟路径复用本页的现货／期货账本，输出只用于 bar 级研究，不能解释为 tick 撮合或实盘成交质量。

金融语义由 `research_pipeline.domain` 与 `research_pipeline.simulation` 实现。金额、价格和费用使用定点整数；订单、成交、结算和公司行动通过事件重放；现货现金账本与期货保证金账本隔离。A 股和 ETF 日频路径由 `daily_event` 编排，`cash_market` 负责确认成交事件、结算、公司行动、目标手数、调仓和现货估值；交易规则、候选成交和费用分别由下述共用模块负责。

## 规则、候选成交与记账

`execution_market` 保存现货／分钟行情观察及分钟参与策略，供执行、规则、撮合、结算和结果共用；这些类型不反向依赖执行模块。`market_rules` 集中 A 股／ETF 执行政策、零股与交易限制，以及分钟期货生命周期约束。`matching` 根据当前可见行情、规则和只读账本，试算现货与分钟期货的成交价量、费用、保证金及拒绝原因；同一 Bar 的剩余容量沿用累计已成交数量，不预先扣减。

获准候选返回执行入口后，才依原顺序生成预占、成交和保证金事件并更新 Ledger。试算本身不改变现金、持仓或事件记录；拒单不收费。现货缺钱策略分别保持整单拒绝和按整手缩量，零股余额不能拆分，最低佣金仍按原订单边界计取。公司行动继续复用既有编译和到账服务。

`costs` 负责现金金额／费用、期货按手费用及日频／分钟费率计算，`margin` 负责保证金试算。日频期货仍按 Decimal 半升舍入到分，分钟期货仍按整数向上取整并保留盈亏余数；日频保证金检查和强平仍在原会话阶段执行。

旧公共导入位置保留直接导入，同一规则或公式只有一份实现。候选接口服务已接线的执行路径；持续挂单使用显式订单合同。日频融资沿同一 Engine／Ledger，须声明信用账户与协议，详见[账户与融资](spot_account.md#融资信用账户)。跨账户共享资金仍须单独合同。

## 交易中间合同

目标模式路径为 `InstrumentKey → PortfolioTarget → OrderIntent → IntentToOrderPort → Order`；显式模式由 `ExplicitOrderCommand` 经 Broker 创建持续订单。`PortfolioTarget` 支持 weight、quantity 和 notional；全现金组合用空 entries 加 `cash_weight=1` 表达，做空和杠杆必须分别由 `short_allowed`、`leverage_limit` 显式打开。股票/ETF 的 `position_effect` 只能为 `auto`；期货必须在 `open/close/close_today/close_yesterday` 中显式选择。`Order` 才保存 market/limit、TIF、限价和生命周期。

当前能力矩阵如下；ResearchPackage 在 plan/admit 阶段必须声明并通过矩阵，不能等到成交时再猜：

| backend | target | asset | short / leverage | position_effect |
| --- | --- | --- | --- | --- |
| `cash-daily-v1` | weight | A 股、ETF | 不支持 short，最高 1 倍 | `auto` |
| `cn-futures-daily-v1` | quantity | 中国商品期货 | 支持 short，最高 20 倍 | `open/close/close_today/close_yesterday` |

这里的“支持”是 backend 的正式接线范围，不是说 `PortfolioTarget` 能表达的所有形状都已经可运行。全现金、负权重和大于 1 倍的目标在合同层可以被明确表示，但“任意正式 DAG 的全现金能力”、现货做空和现货杠杆目前都不是 current 能力：现货做空与杠杆会在 plan/admit 阶段拒绝，全现金只保留已验收参考包中的明确行为，不能外推为通用能力。正式能力状态以 `capabilities` 输出与当前 OperatorDefinition 的能力声明为准。

当前账户本位币只支持 CNY；没有注册 FX model/backend 时，非 CNY 目标、订单或金额相加全部失败。股票/ETF 意图必须绑定当时可见的公司行动/除权快照；期货意图必须绑定乘数、费用、保证金和结算规则快照，任一缺失或 `available_at` 晚于应用时点都失败关闭。

## 日频现货事件仿真

正式 `finance.simulation.daily-cash@1.0.0` 支持旧 ETF profile，或按证券提供 `market_rules` 历史快照列表；两者互斥。历史规则明确板块／类别、上市阶段、数量最小值和步长、零股、交易状态、价格限制、结算及费率，按开盘可见时间和有效区间选择，未知终止上市日期用空值表达。无涨跌幅限制使用显式 `unbounded` 及空上下界。

期初持仓、持有期红利税和有来源换股通过可选 `account` 参数进入同一执行、封存及独立验证；`external_cashflows` 声明普通现货日频入出金，按生效时点切分收益，输入和事件口径见[现货账户与股东权益](spot_account.md)。P6 公司行动 v2 按登记日实际数量确认权益，分别表达除权、现金到账、股份到账及可卖；退市以有来源的非交易清算事件注销持仓。股票现金派息、红股到账和转增到账分别映射 `a_bonus_date`、`dividend_arrival_date`、`a_transfer_arrival_date`，采用新版 Catalog 合同重新准入，历史锁及 Result 保持只读。


`run_daily_cash_event_simulation` 消费显式行情、交易意图计划、公司行动和市场规则快照。股票与 ETF 使用同一入口；同一规范化执行日只能有一条计划，重复立即失败。订单、费用、停牌/涨跌停门禁和账本复用 `Order`、`execute_cash_order` 与 `reduce_spot`。

正式 `result-contract/` 包含 `orders`、`fills`、`positions`、`cash`、`costs`、`valuations` 六张规范表及独立 manifest/`COMMITTED`。ETF 日频和期货日频使用同一组主外键与校验器；Runtime 的恢复以其正式节点工件和检查点合同为准。

### ETF 零股与公司行动独立复核

沪深百股（份）现货规则允许将全部零股余额单独卖出，或与整手一并卖出；零股余额不得拆分。买入仍按整手。显式历史规则优先，默认规则覆盖 2006 年 7 月 1 日起的沪深市场；其他交易单位必须由规则快照声明。容量不足时只成交能够容纳的整手或完整零股余额，不把部分零股作为成交数量。

未声明历史规则或账户的日频 ETF 使用 `research-daily-etf-financial-context-v3`（历史 v2 只读兼容）；声明历史规则或账户时使用 `research-daily-cash-financial-context-v1`，显式订单模式使用 v2；声明外部资金流时使用 v3，封存逐笔前后估值与时间加权收益序列。`simulation/daily-context.json` 必填 `corporate_actions`，内容为本次仿真使用的完整 `CorporateAction.to_dict()` 列表。`DailyCashSimulationResult.corporate_actions` 保留这些原始事实，`build_daily_etf_financial_context` 与受控规则、成本、仿真及六表身份一起发布上下文。

`write_simulation_result_contract` 的 `daily_etf_context` 参数接收构造上下文的参数映射（不含 `simulation_result_hash`）；写入器使用本次六表身份补齐该字段，核对来源仿真，并在 `result-contract/` 的同级目录写入 `daily-context.json`。Result assembler 将其封存为金融支持工件。

独立 verifier 从成交、拆分比例、生效日、可见时间和到账日还原持仓，不调用生产账本或公司行动编译器。v2 公司行动分别核对股份到账与可卖日；股份在声明可卖时点前保留于未结算桶。旧即时／延期公司行动按其封存合同复核；普通 T+1 买入仍在下一会话结算。逐日核对总持仓、可卖、未结算、冻结和非交易变化，外部表按有界批次读取。缺少公司行动事实、提前使用未到账权益、拆分零股或伪造持仓桶均拒绝。v1 上下文缺少完成该复核所需的事实，当前 verifier 明确拒绝；历史 Result 原文件不变。

规则依据：上交所《上海证券交易所交易规则（2006 年版）》及《关于进一步明确竞价交易申报数量及前端控制有关事项的通知》（2014 年 6 月 27 日），深交所《关于发布〈深圳证券交易所交易规则〉的通知》（2006 年 5 月 15 日，7 月 1 日施行）及《关于严格执行证券竞价交易卖出申报数量有关规定的通知》。

## 分钟现货基础权益

分钟股票与 ETF 使用同一现货账本处理基础现金分红、送股、转增、拆股、合股和有来源的退市现金清算。输入为完整 `CorporateAction` v2，沿 `financial_corporate_actions` 从复权快照、调整后行情和研究节点传到目标，再由分钟仿真按当时可见的修订执行。登记持仓取登记日上海时间 15:00 已可见的账户状态；生效、现金到账、股份到账与可卖时点分别处理，退市通过非交易事件清算并注销持仓。

分钟股票 `price_scale=2`，价格与现金单位为 0.01 元；分钟 ETF `price_scale=3`，单位为 0.001 元。初始资金、成交、费用、应收和估值使用对应精度。公共权益编译的分单位金额须无损换算后进入分钟账本，不能将 ETF 分钟金额直接按日频分单位解读。

分钟基础账户从空仓开始。P6A 的期初持仓、取得批次、持有期红利税核定与扣收，以及换股成本、可卖数量和税权承接仅由日频现金仿真消费。完整分钟输入与事件时钟见[分钟基础权益](minute_simulation.md#分钟基础权益与金额单位)。

`research-minute-financial-context-v6` 保存目标中的完整行动、实际登记持仓和权益事件，使用 `research-minute-corporate-actions-v1` 子合同。独立 `result-bundle-financial-oracle-v9` 从封存 Result 重建权益和六表变化，核对目标与行动一致、可见修订、登记数量、到账／可卖、退市清算和金额精度。分钟 `minute-simulation-facts-v3` 再将六表结果、四表账本及规则身份绑定到实际 Bar TCA 期望，正式 `verifier.minute-financial.v4` 缺少 Result oracle 时拒绝通过。

## SimulationResult 公共事实合同

公共合同不是新的撮合器或统一账户平台，只是三个现有引擎的无损事实投影：`orders` 记录订单成交结果摘要，`fills` 逐笔绑定订单、价格、数量、乘数、费用和已实现盈亏，`positions/cash/valuations` 共用快照主键，`costs` 必须逐笔回指 fill。适配器只允许换名、定点单位换算和暴露原状态，不得重新决定成交、强平或费用。

`SimulationResultSemantics` 显式记录资产类别、频率、决策/执行/估值口径、价格口径、费用版本、交易日历、结算、缺失数据策略和负现金权限，并绑定逐会话 `SimulationSemanticsV1` 摘要。六表使用唯一 Arrow 物理 schema，零成交时也保留 date、带时区 timestamp、int64 和 string 类型；schema 字段、类型与 nullable 属性进入 manifest 和逐表摘要。合同版本、语义摘要和六张表摘要共同进入 `result_hash`；任何 schema 或语义变化都会改变身份并使旧缓存失效。

统一校验器在下游读取前复核：订单成交量等于 fills 汇总，fill 的订单/标的/方向外键一致，成交额等于定点价格×数量×合约乘数，费用与 costs 逐笔且同币种一致，现金变化与成交及非交易事件守恒，持仓变化与成交及公司行动守恒，现货可用现金与应收不超过总现金，期货可用权益加保证金等于总权益，现货现金加持仓市值等于 NAV。期货非交易现金变化直接取原结算行的逐日盯市盈亏，并校验权益变化等于已实现盈亏减费用加逐日盯市；不得用权益残差替代源事实。同一交易会话的成交不得晚于估值时点。缺表、重复 fill、禁止的负现金、未来可见基准、物理 schema 漂移或任何摘要篡改都失败关闭。持仓表采用稀疏快照：保留非零持仓、当日有变化的持仓和归零终止行，不为每个历史标的逐日复制零仓。

独立金融复核 v7 不导入上述生产账本状态。它从 ResultStore 的已验证 Parquet 以稳定批次重算逐表身份，并用受 memory/temp 配额约束的只读 DuckDB 完成主键、连接、现金、持仓、估值和 TCA 关系检查；日频 ETF 的规则与结算聚合也不会保存全量 Python 行。Result 大于旧 512 MiB 来源阈值不再自动失败，只有真实资源不足或金融不变量不成立才会拒绝生成 VerificationResult。

## 防前视规则

- A 股开盘成交只能使用开盘时已知的停牌、涨跌停、可卖状态和此前可见容量。
- 日线全天成交量和收盘价在开盘时尚不可见，不能决定当日开盘订单或成交；未建模容量时必须显式声明 `assumed_unbounded`，零可见容量则拒绝成交。
- ETF 的品类由 package 显式分类；交易单位和 T+0/T+1 必须来自受控规则 profile 生成的
  PIT 规则快照，不能按名称猜测或由 package 自报。
- 期货连续合约只生成信号，交易和结算落到当时可见的真实合约。
- 分钟 bar 完成并到达可见时间后才能触发决策；tick 序号冲突失败关闭。
- 公司行动按生效日、登记日和当时持仓处理，不能事后回填未来公告。

## A 股费用是研究假设

A 股日频仿真不再把一组固定佣金、最低佣金、卖出税、过户费和每股滑点写成从 1990 年起一直准确的
历史规则。使用 A 股仿真的 ResearchPackage 必须通过 `research_cost_assumption` 显式声明
费用及滑点数值、币种、单位和适用起止日期；该窗口必须覆盖完整研究窗口，否则 package 准入和
独立验证都会拒绝。Result、VerificationResult 和报告保存的是本次研究实际采用的假设，报告
会明确标注“不是历史真实费率”。

其中佣金、卖出税和过户费使用成交额 ppm，最低佣金使用分，每股滑点使用分/股；滑点与其他
成本放在同一份 `research_cost_assumption` 中，不再由 simulation 顶层参数另声明一遍。

T+1、交易手数、停牌和涨跌停等市场约束不属于费用假设，仍由各自的规则证据约束。本次调整
也不建立历史费率服务；如果研究目标要求复原某一历史时期的真实费率，应先补齐对应的历史
来源和可见时间，再另行扩展规则合同。

## C2 ETF 日频参考切片

C2 使用四只真实 ETF 的冻结小窗口验证
`Signal → PortfolioTarget → OrderIntent → Simulation`。信号在 T 日收盘后完成，订单只在
下一交易日开盘提交；成交使用未复权 open，动量才使用 `close * post_factor`。股票型
ETF 按 T+1、债券 ETF 按 T+0，停牌、涨跌停、手数、万三佣金和最低 5 元均显式进入
规则或成本身份。窗口末日买入 T+1 ETF 时允许持仓保持未结算并计入 NAV，不伪造窗口外
结算事件。

当前 C2 只接受 `cn_etf.daily.curated.v1`。该本地受控 profile 提供 100 份交易单位、债券
ETF T+0、股票 ETF T+1、零卖出税和零过户费，并用有效区间和事前可见时间覆盖研究窗口；
它是本地整理的规则引用，不是交易所发布字节归档。万三佣金与最低 5 元是独立的用户研究
假设，不属于 profile 的市场规则来源。Runtime 将 profile、逐标的规则、成本假设和正式
SimulationResult/ledger 身份写入 `simulation/daily-context.json`，ResultAssembler 必须封存
该文件，独立 verifier 再从 Result 六表重算交易单位、费用和 T+0/T+1。该链只支持
`research_observation/local_only`，不能作为实盘规则或可交易性证明。

窗口内的 510300 现金分红已按公告日、修订日、除息日和到账日编译。正式策略在除息日
没有持有 510300，因此真实 ResultBundle 的 entitlement 为零；文档和报告不得把它描述
成已收到分红。非零持仓的应收与到账折叠由公司行动贴身测试覆盖。

## C4 商品期货日频参考切片

C4 使用冻结的 AG2406 真实窗口验证
`连续信号 → 实际合约数量目标 → OrderIntent(position_effect) → 逐日盯市`。连续合约
`AG9999.XSGE` 只产生信号；所有 fill、保证金和结算都绑定 `AG2406.XSGE`。生命周期按
上市日和到期日含端点检查，乘数按成交日在只读 `data_rq.duckdb` 生效区间唯一命中 15；
禁止硬编码或默认乘数。

订单使用下单前已知的保证金和手续费规则。春节保证金变化通过事前公告生效区间表达，
当日源记录不能回填到更早订单。结算价只在当日收盘后用于盯市，缺失或重复时不以收盘价
替代。非法 `close_today/close_yesterday`、反向平仓、连续合约成交和规则快照漂移都在
账本变化前拒绝。版本化 session resolver 已验证 AG 夜盘归属下一交易日；本次正式日频
结果没有声称复原逐笔夜盘成交，也没有发生移仓。

正式 v5 结果包含 19 笔成交和 57 次结算。独立 oracle 逐事件零差异核对，最终权益为
1,007,670.63 元；篡改 ResultBundle 引用的 Parquet 后，公开 `verify` 明确拒绝。该结果
只支持 bar-level 历史 `research_observation`，不支持组合保证金、跨币种、期货公司
附加费、容量、tick 撮合或实盘结论。

## SimulationSemantics v1

正式 A 股现金日线仿真由已准入的项目 Worker 生成订单意图，再进入公共现金账本、日频事件仿真和 `SimulationResultSemantics` 合同。每个目标执行日都显式记录输入 `available_at`、信号、决策、订单提交、成交、估值，以及收益窗口 `[return_start_at, return_end_at)`；全部时间固定为 `Asia/Shanghai` 并绑定 `calendar_id`。收益窗口既支持成交后开始的持有期收益，也支持上一交易日收盘到当前交易日收盘的账户日收益；后者必须满足收益起点不早于决策、收益终点不早于当日估值，不能用未来信息改写信号或成交。项目的目标组合、参数网格和调度方式不属于框架内建能力。任何决策输入晚于决策、容量输入晚于成交，或未来成分/规则在使用时尚不可见，都会在仿真或独立 Verifier 重算时失败。

容量不是一个布尔开关：

| `capacity_mode` | 执行含义 | 最高结论 |
| --- | --- | --- |
| `modeled` | 使用有来源摘要和可见时间的历史容量模型 | `liquidity_modeled`，可映射到 tradable simulation |
| `assumed_unbounded` | 明示以订单量作为无限容量假设 | `research_only`，最高为 portfolio simulation candidate |
| `unknown` | 容量未知，不把填充结果解释为流动性充分 | `no_liquidity_claim`，只允许 research observation |

当前正式 ResearchPackage 只注册 `assumed_unbounded`；这保持既有成交数量，但不再暗示容量已建模。容量、成本的 `model_id/version` 与每个 session 的语义摘要进入公共结果合同、恢复比较、上层 simulation summary、validity facts 和 Verifier v2 算法。

分钟事件仿真已经通过同一 `IntentToOrderPort` 生成现货和期货执行订单；`SessionCalendarResolver` 在冻结参考范围内按 instrument classification、policy revision、来源摘要和显式 trading_date 生成 `TradingSession`。股票、ETF、指数午休分段，AG2406 夜盘归属下一交易日。AG2406 盘中使用事前可见费率、保证金和前结算推导限价，非零持仓只在真实收盘 bar 完成后按当日结算价逐日盯市。规则缺失、冲突、休市时点或范围外日期都失败关闭；分钟路径不得从事件自然日或固定 240 分钟模板重新推断。

运行时通过封闭 implementation ID 调用组合标准化和仿真，不传 SQL、数据库连接或函数。仿真输出是研究证据的一部分，不自动升级为可交易结论。


## 期货剩余持仓成本

同向增仓按合约数量加权成本。部分平仓按原成本确认已实现盈亏，剩仓成本保持不变；反转先平原方向，再以新成交价建立新方向成本。收盘结算只对剩仓盯市，随后将成本重置为当日结算价。

成本以整数分子、分母精确保存。单笔已实现金额向零取整至金额最小单位，余数留在合约持仓中，后续平仓或结算继续结转。成本、余数和该合约保证金均进入账户状态身份；累计损益不因拆分平仓而丢失。保证金按各合约实际占用额释放并重新计提，不用加权成本推测旧保证金。

乘数为 15、费用为零时，100 买入 1 手、110 再买 1 手、120 结算的总收益为 450；100 买入 2 手、110 平掉 1 手、120 结算剩仓的总收益同为 450。正式成交与结算上下文共用生产成本状态；独立 oracle 则累计逐笔持仓资金和相反方向现金流，单独分配已释放成本、复算舍入余数与费用，逐项核对成交损益、结算事件和权益。

## 现货零股余额

买入按政策中的整手单位提交。卖出可以提交整手，或一次带走全部零股余额；110 股持仓既可一次卖出 110 股，也可卖出 10 股后保留 100 股，不能拆为 5 股和 105 股。成交容量不足时只允许整手成交或全部零股成交，不能把零股余额拆开。

显式历史规则字段 `sell_remainder_allowed` 优先。没有该字段时，沪深市场自 2006-07-01 起的百股（份）A 股和 ETF 适用上述余额规则；其他市场和整手政策需自行提供历史规则。正式执行仍受可卖数量、结算、限价和费用约束。

零股规则来源为沪深交易所 2006 年版《交易规则》，两所均自 2006-07-01 起施行；深交所 2015 年《关于严格执行证券竞价交易卖出申报数量有关规定的通知》进一步明确可单独一次卖出零股，或与整手部分一并卖出。

- 深交所 2006 年规则，第 3.3.8 条：`https://www.szse.cn/disclosure/notice/general/t20060515_499577.html`
- 上交所 2006 年规则，第 3.4.7 条：`https://big5.sse.com.cn/site/cht/www.sse.com.cn/lawandrules/sselawsrules2025/repeal/rules/c/c_20120917_10785168.shtml`
- 深交所卖出申报数量通知：`https://www.szse.cn/lawrules/rule/trade/current/t20150914_565054.html`

## Sharpe 选择偏差诊断

DSR 诊断的最大 Sharpe 门槛采用 Bailey、López de Prado《The Deflated Sharpe Ratio》2014-07-31 版式（1）：

```text
SR₀ = μ + σ [(1 − γ) Φ⁻¹(1 − 1/N_eff) + γ Φ⁻¹(1 − 1/(e N_eff))]
```

其中 Euler 常数 `γ=0.5772156649015329`。本实现的 `N_eff=(tr(C))²/tr(C²)`，C 为候选收益的相关矩阵；结果同时保留候选总数、有效试验数及其计算方法。N_eff 为 1 或候选 Sharpe 标准差为零时，门槛取均值。有效试验数接近 1 时，极值近似的溢价可能为负，实际门槛以单候选均值为下限。N_eff=10、均值为零、标准差为 0.1 时，门槛约为 0.157459830134575。该极值公式是近似；有效试验数依赖已提交的完整候选集合及其样本相关结构。

DSR 保持统计诊断函数边界，不新增正式 DAG 算子或扩大金融能力等级。


## 日频价格精度与现金单位

日频股票、ETF 使用未复权价格，`Price` 及规范成交表通过 `execution_price_units`、`price_scale` 显式表示价格。日频引擎保留三位小数，例如 3.947 元表示为 3947、`price_scale=3`，不先舍入为 3.95 元。超出声明精度的输入拒绝运行。

日频 `notional_units`、现金、费用、持仓市值与净值均以人民币分记录。初始现金必须是正数且精确到分，0.005 元等超出分精度的输入拒绝运行，账本与收益率共用同一初始金额。价格乘总数量后再四舍五入到分：3.947 元买入 100 份为 39470 分，7 份为 2763 分；不是逐份舍入再相乘。费用以该成交额按显式费率计算，最低佣金仍为分。目标数量按完整价格计算，收盘估值对每个标的总持仓按相同规则换算。

规范结果已有的 `semantics.frequency` 决定金额单位：`daily` 按分换算；`minute` 沿用对应价格精度下的整数金额合同。独立金融 oracle 按该语义和每条成交的 `price_scale` 重算成交额，不混用两条路径。日频事件仿真使用 `research-daily-event-simulation-v3`，TCA 公式身份使用 `formal-fill-attribution-with-explicit-cash-scale-v3`。历史 Result 保持其原始内容。

日频组合的 `benchmarks` 必须包含整数 `price_units` 与 `price_scale=3`。TCA policy 显式声明基准价格精度；正式成交只允许无损换算到该精度。日频实际价差金额四舍五入到分后与正式费用相加，模型价差成本换算到分后向上取整；分钟仍使用原整数单位。这样三位报价、低费率、零股和两位报价均遵守同一现金口径。


## 公共执行与订单生命周期

分钟数量目标、日频现货与日频期货经 `ExecutionEngine` 执行，`TargetExecution` 管理分钟目标流，`daily_target_deltas` 调和日频目标并保持先卖后买。`Broker` 使用 `transition_order` 推进真实订单状态；资金和持仓仍只在原账本中维护。

六表 `orders.status` 是成交结果摘要：`filled`、`partially_filled` 或 `rejected`。订单生命周期单独保存在 `simulation/context-tables/order-lifecycle/session=YYYY-MM-DD/data.parquet`。分钟 IOC 部分成交的剩余数量取消，原目标下一合格 Bar 可以生成新订单；日频 DAY 余量在收盘到期。空订单结果保留相同物理 schema。

`research-simulation-result-semantics-v2` 要求 `research-order-lifecycle-v1` 支持事实；日频 ETF 上下文使用 v3，分钟上下文使用 v6，独立金融验证使用 `result-bundle-financial-oracle-v9`。该合同要求生命周期支持事实，Result 封存逐会话分区。独立验证检查订单外键、转换顺序、事件时点、剩余数量、累计成交与 fills 一致，以及 IOC 结束和 DAY 到期，不调用生产状态转换函数。源码依赖变化会改变执行身份，恢复必须匹配当前准入。历史封存结果保持只读。


## 日频期货会话账本与独立验证

日频期货使用 `FuturesDailySessionLedger` 持有当前会话唯一工作状态，依次应用显式订单、逐笔费用、逐日盯市、保证金需求和必要强平；会话结束才发布严格校验的 `FuturesLedgerState`。日频采用 `settlement_margin_check`，允许结算计算中出现保证金需求高于权益，再按结算价全量平仓假设完成批次。最终权益为负或保证金仍高于权益时拒绝结果。

普通 `close` 按先昨后今减少仓位；显式平今、平昨只能消费对应桶。强平按实际昨仓和今仓分别生成订单与成交，各自计算费用。昨仓2手、今仓1手，普通平仓每手1元、平今每手10元时，总费用为12元。结算后权益100元、需求120元、强平费3元时，终态为权益97元、仓位0、保证金0。

价格保留原始小数精度，加权基价用有理数保存。日频盈亏与费用逐笔半升舍入到分；分钟继续使用既有余数结转策略。日频期货合同为 `research-futures-daily-simulation-v2`，动态换月为 v3，多品种组合为 v2。多品种仍按独立品种账本执行后聚合，组合保证金合计不能超过权益；共享购买力属于后续账户能力。

新结果将订单生命周期、原始行情／结算输入、规则、成交日龄、精确基价和强平前需求封存。小控制文件为 `simulation/futures-context.json`。金融支持事实在 intents 模式使用 `research-futures-daily-context-v2`，显式命令模式使用 v3 存储合同，在 `simulation/futures-context-tables/<role>/part-00000.parquet` 按表、每文件最多 65,536 行保存；各表按交易日稳定排序，允许一个会话跨文件，行内时间和金融事实完整保留。逻辑表 schema 与 `research.futures-daily-context.v1` 工件类型保持不变；历史 v1 逐会话分区仍可只读验证。订单生命周期继续逐会话封存。独立金融验证只读封存 Result，按会话重建账户并与规范六表核对；删除运行目录后仍可验证。

### 已知限制

旧 intents 路径沿用显式订单时点对应日开盘价的研究假设；显式命令路径要求提交早于日开盘执行。两者均保留17:00结算及结算价强平的研究假设。输入具备来源时间时保留实际事件与可见时间；缺失时在 `timing_policy` 和来源字段标明研究时间假设。该政策不代表真实盘中撮合。旧分钟净仓入口按原准入限制拒绝差异平今；新版共享期货日频／分钟入口已支持独立今昨桶及差异费率，使用独立九表合同。P4历史证据不扩大真实品种、日期或共享账户覆盖。

## 零成交与账户权益

日频和分钟的零成交窗口保留有类型的 canonical 与 TCA 空表；TCA research 汇总仍有一行，成交量及交易费用为零，流动性分项为 `not_computable`，不以零值冒充已计算。日频无订单窗口也允许生成该结果；成交引用不存在的订单仍拒绝。

账户可以只有期初持仓和权益结算而没有市场成交。分红、红利税、换股及退市款仍进入账户净资产，独立金融验证分别重算账户与TCA并核对来源身份。股票零成交合成正式包已通过 lint/admit/run/verify/report 和失败恢复；真实来源范围保持独立声明。

## 显式订单与持续执行

股票、ETF 与当前期货账户可在各自已有执行范围内声明显式订单。`market/limit × IOC/DAY`、撤单、部分成交、按订单预占及累计最低佣金的合同见[显式订单与预占](explicit_orders.md)。日频期货仍采用日频费用舍入、逐日盯市与已声明的结算价强平假设，逐品种组合资金分配保持独立。

规范六表保存实际成交摘要，订单生命周期保存接受、部分成交、撤销、拒绝和会话到期；TCA 与报告只消费这些正式事实。独立复核从封存命令、规则与行情重建订单限制及资金变化。

共享期货账户采用独立方向桶、共享权益九表和封存上下文，合同与金额口径见[共享期货账户](shared_futures.md)。

## 代表性真实验证

真实行情、历史规则与账户假设分别封存。已有ETF融资以及现金分红／期初持仓／外部资金流的正式Result和独立VerificationResult；覆盖窗口、独立金额与尚未完成项见[真实覆盖与验收](native_backtest_acceptance.md)。低层算例不扩大成正式历史覆盖。

日频权重目标先按买入申报增量（step）向下取整为目标持仓，再计算买卖差量；最低申报量只约束订单，不作为持仓取整单位。差量继续受方向格点、零股可卖量及资金／容量限制。普通100股增量下，持仓26000股、未取整目标13080股对应卖出13000股；200股最低量／1股增量的规则保留整数股目标，差量不足最低申报量时不下单。
