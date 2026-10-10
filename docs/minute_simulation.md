# 分钟回测：只在信息已经出现后决策

分钟回测更容易把未来信息放进当前决策：例如用尚未收完的一根分钟线决定该分钟内的交易。这里按已完成 Bar 驱动决策，并明确规则、容量和剩余量怎样参与成交。

Bar 就是一段时间的汇总行情。它不包含完整的盘口排队信息，因此这里的结果不能解释成逐笔实盘成交质量。

## 参数与行为说明

`finance.simulation.intraday@3.0.0` 消费显式数量目标，根据正式持仓、未完成目标和可卖数量调和订单，并把 participation cap 未成交余量顺延到下一完成 Bar。正式 fills 直接进入公共六表和 [Bar TCA](bar_tca.md)，不再二次撮合。股票和 ETF 分钟输入缺少 PIT 复权快照证明时只能 `analysis_only`；这不会因合同测试或 fixture 自动升级。

分钟仿真只在完成 bar 驱动的研究边界内成立，不等价于 tick 撮合、订单簿排队、真实成交质量分析或实盘执行。

## 显式订单

同一算子可声明 `execution_mode=explicit_orders` 和完整 `order_commands`。该模式消费有类型的空目标工件，保留上游 PIT 与权益元数据；同一订单由 Broker 持续管理，预占归属于 Ledger 中的订单编号。限价、DAY、撤单、资金不足处理与累计费用见[显式订单合同](explicit_orders.md)。目标模式继续逐 Bar 调和 IOC。

显式分钟上下文使用 `research-minute-financial-context-v7`，封存命令、执行观察、规则、预占事件及完整 SessionPolicyBundle；目标模式保留 v6。交易会话到期按冻结的日夜时段确定，不能以最后一条输入 Bar 代替。

## 内部职责

分钟 Runtime 适配器继续调用原有仿真入口。`execution_market.py` 保存共用的不可变行情与分钟参与策略，原公开导入方式保持。`engine.py` 通过明确的会话接口推进完成 Bar，`minute_execution.py` 持有当前会话和唯一账户；`target_execution.py` 负责目标替换、进度和订单意图，`minute_orders.py` 消费意图并确认成交及记账；`minute_rules.py` 集中执行和结算共用的 PIT 规则解析，`market_rules.py` 负责交易限制和生命周期；`matching.py` 只读试算现货／期货成交数量与金融约束，复用 `costs.py` 的费用和 `margin.py` 的保证金计算；`settlement.py` 负责期货会话结算及收盘事件约束；`valuation.py` 计算持仓市值和账户净值；`result_collector.py` 将成交与已确认的账户状态投影为规范六表。账本仍是金融状态的唯一来源。

分钟仿真算子的实现身份绑定执行、权益、规则和结果模块。源码或输入合同变化后须重新准入，旧 checkpoint 不跨身份复用；已封存历史 Result 保持只读。

## 显式历史规则输入

`finance.simulation.intraday@3.0.0` 可在 JSON 参数 `parameters.rule_bundle` 中声明完整的 `MinuteRuleSnapshotBundle`，并用 `rule_bundle_hash` 固定同一份内容。未提供时使用随包发布的参考规则。研究包编译时校验完整规则、来源引用、当前平台能力绑定与内容身份；Runtime 再次核验冻结载荷，订单仅消费有效期覆盖且在决策时可见的规则。

显式规则仍只适用于当前平台已经准入的标的和日期，不扩大分钟行情覆盖。股票必须声明历史板块、上市阶段、ST、交易状态、数量限制、结算、费用与价格限制来源；股票交易规则的复权身份必须和目标实际使用的 PIT 快照及公司行动快照一致。费用等研究假设须在来源及参数中明确声明，不能当作真实券商或历史交易资格证据。

完整规则随金融上下文封存进入 Result，独立验证从该 Result 重算；不回读项目规则文件或数据库。修改规则内容需要重新准入，旧 checkpoint 不跨输入或实现身份复用。随包的 unsupported 规则保持拒绝，只有显式提供了完整事实的研究包才进入对应执行路径。

## 盘中临停的完整分钟边界

股票和ETF撮合检查整个执行分钟及实际执行观察时刻。分钟覆盖任何当时已可见的暂停状态，即使分钟结束前已经复牌，整根Bar的撮合容量仍为零；复牌后的首根完整可交易分钟才恢复参与率容量。原始价格和成交量保持不变。延迟到达的Bar还要求成交记录时刻未暂停。

停复牌沿显式规则的可见时间和有效日期更新，每个时点选择最高可见修订；未来事件不影响过去，晚到低版本不覆盖已知高版本。DAY订单在暂停或价格未满足时继续等待，撤单仍按声明时点处理。原订单提交时间不变，撮合规则以实际执行时刻核验可见性；执行之后才可见的规则不能参与当前成交。独立金融验证从封存行情和规则重新计算完整分钟状态，拒绝跨暂停成交及伪造的非零撮合容量。

分钟级边界不复原秒内可成交份额、复牌集合竞价或逐笔队列。采用交易所状态生效时点作为事件可观察时点的研究，应明确这项假设，不将其表述为供应商网络接收记录。

## 上市首日的复权原点

上市首日没有前一已完成日因子时，`data.minute.adjustment_snapshot` 可声明可选JSON参数 `initial_listing_evidence`，精确包含 `instrument_id`、`listed_date`、`available_at` 和 `source_reference`。仅支持股票，上市日须等于适用起点的上海日期，证据须在适用起点及当日09:30前可见。初始因子固定为1，日期为上市日；该值是归一化原点，不从当日日线收盘价或因子读取。

已有前日因子时不能同时声明首日原点；缺少前日因子又未提供上市证据时仍拒绝。上市前或当日已有生效公司行动的场景不走此简化原点。证据与输入引用共同绑定快照身份并封存，独立结果审计重新核对四字段、可见时间及因子原点。普通历史路径保持原有因子来源口径。

## 分钟基础权益与金额单位

股票和 ETF 的金融公司行动必须使用完整 `CorporateAction` v2，保留行动与修订身份、来源、公告可见时间、登记日、生效日、现金到账日、股份到账日及可卖日。复权快照保存研究价格所需事实，`financial_corporate_actions` 保存实际记账所需的完整行动列表；该列表沿调整后行情、Feature、Signal、Target 传递，Runtime 从目标工件读取并交给唯一现货账本。

基础权益包括现金分红、送股、转增、拆股、合股和有来源的退市现金清算。登记日按上海时间 15:00 已可见的账户持仓确认权益；除权、现金到账、股份到账与可卖分别按声明时点处理。送转股份到账后在可卖日前留在未结算桶；退市使用非交易清算事件注销持仓并终止该标的后续交易。每次应用只选择当时已可见的修订，生效时尚不可见的公告、缺失的生效或股份到账会话、对已入账行动回填其他修订均拒绝。

| 分钟资产 | `price_scale` | 价格与现金整数单位 |
| --- | --- | --- |
| 股票 | 2 | 0.01 元 |
| ETF | 3 | 0.001 元 |

初始资金、成交额、费用、现金、应收和估值遵守各自的分钟金额精度。公共权益编译产生的分单位金额先按规则精度无损换算，再写入分钟账本；不能无损表示时拒绝。日频现金账本仍统一以分记账，不能将日频的金额换算规则直接套到 ETF 分钟账本。

分钟基础账户从空仓开始，窗口前的登记只确认零持仓。P6A 的期初持仓、取得批次、持有期红利税核定与扣收、换股成本及税权承接仅进入日频现金路径，见[现货账户](spot_account.md)。

## 事件时钟

首版唯一模型是 `next_bar_participation_v1`：bar N 完成且可见后产生决策和订单，订单最早在下一根 eligible bar 完成并可见时执行。容量使用该执行 bar 此时已经完整可见的成交量，成交时间也记为其 `available_time`。框架不提供 `next_bar_open`，从而避免用下一根完整成交量支持开盘瞬时成交。

```text
bar completed/available → decision → submitted → next eligible bar available
→ fill/partial/reject → valuation
```

结果逐项保存 decision、submitted、eligible、fill、valuation、bar hash、规则身份、policy hash 和 claim ceiling。输入 bar 必须使用上海时区、完成、质量通过，并携带 source snapshot hash。

## 资产边界

- A 股和场内基金：只有生命周期、手数、涨跌停、费用和结算规则在决策时均可见时，才会复用现有 `execute_cash_order` 与 `SpotLedgerState`。ETF 不预测下一分钟停牌；只有下一根实际存在、已完成且质量通过的 eligible bar 才能成交。ETF 子类规则缺失时拒绝，不套用股票默认值。
- 指数：只能作为 benchmark，订单在下一 eligible event 明确拒绝，永不进入现金或持仓账本。
- 期货：只允许当时已映射的实际合约；需要生命周期、到期、乘数、最小变动价位、价格限制、费用、保证金、结算和 session 规则闭合。容量同时受已完成 bar 成交量参与率和 open interest 约束，成交与保证金事件复用 `FuturesLedgerState`。

随包参考规则包含 `510300.XSHG` 在 2024-01-02—05 的产品、T+1、前收限价和研究费用资料，但股票与 ETF 均缺少当前完整合同要求的历史交易状态。当前重新运行需显式提供完整 `rule_bundle`；缺少来源的默认交易路径拒绝执行。历史已封存 Result 保持可读，旧成交证据不替代当前规则准入。`AG2406.XSGE` 在 2024-01-03—04 冻结窗口内已闭合实际合约、生命周期、乘数、tick、逐日限价、成交额费率手续费、投机保证金和收盘结算；盘中订单只读取此前已可见规则，结算价只在 completed/pass 的 session 收盘 bar 后消费。平今费率与普通平仓费率不同时首版明确拒绝，不猜持仓日龄。

AG2406 正式参考结果产生 32 笔开平成交并通过独立验证。固定信号在两日收盘前都回到空仓，所以该真实 Result 的 `settlement_events` 是空集合；非零持仓的逐日盯市、基价重置和保证金聚合由独立最小 fixture/oracle 验收。文档不能把空集合写成真实发生过结算事件，也不能用成交价或经纪商默认费用伪造成交。

## 容量与结论限制

参与率是明确的 ppm 参数，只在执行 bar 完成后使用。所有输出的 claim ceiling 固定为 `bar_level_research_only`。任何规则缺失、未来可见、连续合约、过期合约、未完成 bar、质量失败、价格 tick 不一致或资金/保证金不足均失败关闭。

该能力已登记为 `finance.simulation.intraday@3.0.0`。ETF 与 AG2406 冻结参考包均完成 Result、VerificationResult、report 和 export-result 后，`minute_line.complete` 提升为有界本地 `local_only`；它不代表全历史、全品种、Tick/LOB、供应商历史版本精确重放或实盘可交易。


## 收益标签的经济时间

标签的 `label_start/label_end` 分别记录起价和终价实际对应的价格事件：开盘价用 `bar_start`，收盘价用 `bar_end`。这两个时点进入统计 observation 的 entry/exit，用于计算重叠窗口、HAC 下限、embargo 下限和拆分边界。午休、隔夜和跨 session 的间隔保留实际经过时间，不用 bar 数替代。

当标签以决策时已知的收盘价为起点，经济起点可等于或早于 decision_time；终价事件必须晚于决策及起点。核心未来观测事实另行保存 first_actual_observation_time 和 last_actual_observation_time：已知起价不计入未来观测，首个实际未来价格事件仍须严格晚于决策。标签 available_time 保留源数据实际可见时间。

open-close、close-close、open-open 均沿用这一合同。标签可以以未来开盘价定义研究收益；仿真成交仍遵守完成 bar 的执行时钟。


## 目标与生命周期证据

分钟目标由 `TargetExecution` 保存和替换；它只生成差量与 OrderIntent，分钟规则绑定和执行由 `minute_orders` 完成，结果支持事实由 `result_collector` 收集。连续延迟目标分别绑定原始决策 Bar，当前会话索引在切换来源交易日时清空，跨会话按需读取已封存的支持分区。`execution_bars` 保存全部已观察行情，不因当时尚未读到该标的目标而遗漏证据。`ExecutionClock` 检查 available_time 的稳定顺序，执行仍要求 completed、质量通过且决策输入当时可见。夜盘保留来源的 trading_date，不按自然日改写会话。

每个合格 Bar 的目标尝试生成 IOC 订单，经公共 `ExecutionEngine` 与 `Broker` 执行。部分成交的六表摘要为 partially_filled，生命周期记录取消剩余量，原订单不会在下个 Bar 再次成交；未达到的目标会在下个合格 Bar 重新调和。支持事实逐会话写入 `simulation/context-tables/order-lifecycle`，空结果保留稳定 schema，ResultAssembler 封存后由独立金融验证消费。显式限价、持续挂单和撤单能力仍按对应准入范围声明。

## 执行机会与估值时点

目标只在同一标的、完成且质量通过、开始时间不早于决策时点的后续 Bar 上激活。其他标的行情或不合格 Bar 不消耗待执行目标；结束时仍缺少合格机会则拒绝结果。存在合格机会但容量不足或目标已经达到时，允许零成交。

现货日终估值从当前会话、估值时点已可见的合格 Bar 中选择 `bar_end` 最新者；同一事件时点按稳定来源序号确定。迟到的旧行情保留在支持事实中，不覆盖已可见的新价格。独立金融验证从封存行情按相同明示口径自行选价并复算市值，不调用生产估值函数。分钟金融上下文为 `research-minute-financial-context-v6`。

独立金融验证使用 `result-bundle-financial-oracle-v9`，分别核验数量格点、无涨跌幅限制、生命周期、停牌和当时可见的规则修订。股票在目标生成及实际订单时点均须绑定同一份目标 PIT 事实；未消费的未来修订不影响过去验证。

## 金融上下文与 validity facts

金融上下文 v6 将完整规则、目标工件、订单生命周期、行情与执行观察、六表结果及账本身份一起封存。`corporate_action_context` 使用 `research-minute-corporate-actions-v1`，保存完整 v2 行动、实际登记持仓和权益账本事件。独立 `result-bundle-financial-oracle-v9` 从封存 Result 核对行动与目标输入一致，独立复算可见修订、登记数量、到账与可卖、退市清算、现金及持仓变化，并与六表和 Bar TCA 对账。

`research.validity.minute` 显式接入 `minute_1m` 与 `decision_minute_1m`；分别封存执行行情和决策行情的输入范围、来源及能力上限。同源入边去重，未准入或相互冲突的引用拒绝。

当前分钟 validity 消费 `status=simulation_succeeded` 的六表 manifest 与 TCA metadata，生成 `minute-simulation-facts-v3`。facts 保存结果身份所需字段，订单审计绑定 orders 表身份，账本身份绑定结果及 costs、cash、positions、valuations 四表。`verifier.minute-financial.v4` 独立重算这些身份，并与实际 Result 金融 oracle 返回的 `bar_tca_expectations` 对齐；正式验证缺少 oracle 时拒绝通过。实际六表行数、schema、生命周期和金融上下文由 Result oracle 复核。已封存 `minute-simulation-result-v2` 继续按原合同读取。

标签与统计独立验证分别使用 `verifier.minute-label-split.v3`、`verifier.minute-statistics.v3`。经济起价时点允许已知；起价及决策均须严格早于终价。PIT 快照验证 `verifier.adjustment-snapshot.v2` 支持完整金融行动列，拒绝非 v2 行动或超出研究时钟的公告。

## 验收与来源范围

股票和 ETF 的合成分钟工程包已完成公共 lint/admit/run/verify/report，非零登记分红、成交及 T+1 拒单均通过独立验证；P6／P6A 整体验收以阶段汇总为准。已有真实参考窗口的验收记录只对应其冻结合同。P9 的历史盘前状态、完整规则修订来源、代表性真实权益事件，以及客户取得批次、税务身份和完整换股来源覆盖继续逐项认定，缺失项保持未完成。合成研究或合同检查不能替代真实来源覆盖。

共享期货分钟路径通过 `finance.simulation.shared-futures.intraday@1.0.0` 消费已准入 raw 完成 Bar 和明确的费用、保证金、会话及退出来源。差异平今与多合约共享账户使用新版方向桶结果，见[共享期货账户](shared_futures.md)。

P9逐市场及频率的真实来源和正式结果覆盖见[真实覆盖与验收](native_backtest_acceptance.md)。日频账户结果不能代替分钟成交证据。

利安科技300784在2024-06-07的上市首日两次临停已完成真实240分钟正式链、金融节点恢复及独立副本复核，具体数值与范围见[真实覆盖与验收](native_backtest_acceptance.md)。
