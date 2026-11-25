# LEAN 日线与一分钟策略执行

把研究策略接入本地 LEAN 执行。

节点结果的 `preparation.stage_seconds` 记录安装检查（`runtime_verify`）、组装执行配置（`prepare`）、准备原生数据绑定（`native_data_prepare`）、写运行环境锁（`runtime_lock_write`）和执行（`execute`）各阶段的秒数。`preparation.daily_input_timings` 给出逐日输入耗时。公告可见日期按冻结交易日历映射，每行仍取公告与修订时钟中的最晚可见时点。

## Python 策略直读日级研究数据

将直接 DuckDB 来源声明为 `kind="pit"`，绑定研究端口、请求、业务列和已准入的日期规则。运行端从实际输入引用在 `strategy-input.json` 写入 `pit_sources` 来源描述。策略初始化时读取该描述并调用 `ReadOnlyPIT(self, sources)`。同一查询器使用一个只读数据库连接，`read(binding_id, start=..., end=...)` 返回 Arrow 表。日期参数可省略，或用于缩小声明的区间。查询器以算法时钟和冻结日历限制可读日期，保留声明的证券、过滤条件、顺序和数值类型。

直读适用于单日期可见的原始事实。开盘状态在当日开盘可见，收盘行情在下一交易日开盘可见，公告在公告日后首个交易日开盘可见，业务生效日期按当地零时处理。复杂修订和区间来源使用原有事件接口。策略在结束回调及异常清理时调用 `close()`。

## 开始前

- 输入：完整安装、研究包、输入、规则和声明预算。
- 输出：正式执行表、账户终态、指标及报告。
- 命令中的尖括号内容是待替换参数。

## 使用顺序

1. 按安装文档准备独立 LEAN 环境。
2. 填写策略、来源与执行模型。
3. 准入真实输入和规则。
4. 执行研究并等待正式 Result。
5. 直接从 Result 生成报告。

项目策略声明 `execution_backend=lean`，在正式 ResearchPackage 主链中使用本地 LEAN。频率为 `1d` 或 `1m`。数据经过同一Catalog、PIT与交易范围准入；选股可在原生算法内部执行，也可消费正式上游研究输出。

项目策略直接实现Python或C#的QCAlgorithm，LEAN负责账户、订单、指标与算法生命周期。安装后的统一数据服务为订阅、History、SetWarmUp和Universe执行相同PIT规则；中国模型直接检查原生订单。原生完整接入仍在实施验收中，尚不能以编译、组包或内存测试代替完整引擎交付。

安装与完整包转换命令见[LEAN 接入](../integrations/lean/README.md)。安装脚本位于 `integrations/lean/`，外部插件和宿主位于 `integrations/lean/engine/`，使用固定的本地 LEAN 安装。运行不依赖 Docker。

Windows x64 和 PowerShell 7 的首次安装可从[合成回测示例](../examples/lean_quickstart/README.md)开始，先生成策略包与 Parquet 输入，再安装并执行。使用完整公开源码或解压的 sdist。wheel 只提供 Python 框架。

`run.ps1` 的 `DataDb` 与 `InputSnapshotManifest` 必须二选一，归档分支不连接数据库。当前公开 LEAN 安装流程仅支持 Windows x64 与 PowerShell 7。Linux 核心或模型验收不代表 LEAN 安装可用。

## 声明与输入

准入只检查和冻结输入合同，候选与信号在正式执行时计算，可位于原生算法内部或独立研究节点。覆盖证券、候选、持仓和权益的关系见[准入与执行边界](admission-execution.md)。

股票确认退市生效后，剩余股份采用零估值研究假设，保留数量和归零时点，不产生卖出现金。普通停牌或缺价不触发归零。无涨跌幅阶段沿用现有分钟撮合模型，历史市价许可、保护价、数量、资金和T+1约束继续生效。

现货策略参数`lifecycle`按证券代码交接独立生命周期记录，每条包含`delisted_date`（最后存续日）、带时区的`available_at`及`source_ref`。零估值从最后存续日的次日且信息已经可见时生效。窗口末日、会话缺失和停牌不能用作退市日期。

退市生效后保留股份账簿，停止该证券交易。已登记现金权益继续按事件到账，`lifecycle`执行表与账户估值来源保存`delisted_zero`口径。

全部证券在请求终点前退市时，终点增加一次账户观察。消费已登记权益并保存零估值终态，不新增交易日或策略回调。研究收益使用该终态，保留实际`valuation_at`。原始逐日账户仍按原交易日保存。

策略节点同时声明 `execution_backend=lean`、`claim_ceiling=bar_level_research_only` 和完整 `lean_runtime`。一分钟使用 `execution_model=lean_next_completed_close_v1`，日线使用 `lean_next_session_open_v1`，声明频率须与 bars 端口一致。策略声明的 `bundle_id` 必须对应已准入 bundle，输出端口为 `simulation: research.lean-simulation.v4`。

人民币股票使用 `asset_class=cn_stock`、`contract_kind=stock`、`price_scale=2`。ETF 使用 `asset_class=cn_etf`、`contract_kind=etf`、`price_scale=3`。现金金额均按人民币分记账。

ETF 的数量、结算和费用来自冻结的 `cn_fund` 规则。日线与独立开盘来源须在所用 Catalog 中声明并准入。

人民币期货使用`asset_class=cn_future`、真实合约与逐合约价格尺度，按完整历史会话处理夜盘。一个账户内多合约共享资金，同合约保留多空与今昨四桶。期货不使用现货snapshot。独立结算、规则、换月和风险合同见[中国期货执行](lean-futures.md)。

输入包括正式 `bars` 和可选 `signals`。现货公司行动可以来自逐证券 `snapshot`。原始股票一分钟策略还可声明 `stock_corporate_actions`，从正式列式输入直接交付账户权益。行情必须原始未复权、已完成且质量合格。一分钟在分钟结束时可见。

日线按 Catalog 的 `next_session_open` 规则在下一交易日开盘可见。日线直接绑定列式数据请求，以 `bars_request_id` 指定 OHLCV 来源，以 `open_request_id` 指定同证券、同日期的 `session_open` 开盘价来源。两者由同一 data bundle 交接。

历史规则来自包内冻结的 `minute_scope`。每条信号保留核心可见时间，同一输入只交付当时最新的完整批次。

原生分钟订阅、History 与交易共用 LEAN 进程内的 ParquetSharp 读取器。清单绑定实际文件、行组、研究证券和可见区间。研究域可以大于交易域，账户按交易域和实际会话消费行情。每次运行的 `lean/parquet-read.json` 记录各月物理读取次数、读取行数、逐日交易域行数和发给订阅的行数。

现货一分钟在 LEAN 内按交易日读取冻结计划的 `rules.parquet` 和 `parameters.parquet`，按规则实际可见时点处理日内修订，并保留最近已可见规则用于跨日估费。分钟行情覆盖与规则消费共同校验。`parquet-read.json` 同时记录规则读取次数和耗时。公司行动仍以除权日 09:30 已可见的前收参考价估值，策略通过已有声明明确请求所需规则类型。

日线、事件和 Universe 在引擎进程内按已准入的数据引用读取。订阅保留发布和修订过程，History 按消费时点选择当时的版本。组合目标在第一次订阅时读取并按证券索引，后续订阅共享索引。算法可使用 `ReadOnlyPIT` 在当前引擎时钟直接读取已声明的 DuckDB 字段。

`ReadOnlyPIT` 在同一只读连接上共享声明的证券范围。对可按单一可见日期裁剪的来源，连续短查询复用有界 Arrow 日期块，策略只能取得在当前引擎时钟和冻结范围内已可见的记录。宽区间或无法安全分块的查询按原范围读取。引擎日志的 `PIT read statistics` 按来源记录实际查询次数与耗时。

日期升序的 DuckDB 日频证券池订阅可在一次只读查询里取多个完整截面。对已限制证券范围、且主键能确定每个证券每日最多一条的来源，按证券数、估算行宽及已准入的行数、字节和内存额度选择窗口，每次最多 128 个日期。数据继续分 Arrow 批读取，按原来的行序、交易日历和可见时点交付。逐证券 History 沿用自己的读取范围。

日线绑定的daily_time保存经Catalog检查的规则、时区和完整交易日历。日期公告在严格晚于公告日的首个交易日开盘可见，周末和长假沿冻结日历处理。ResearchData保留原始观测时间、BindingId和全部公开Fields，多来源订阅名称为binding_id:完整代码。

原生算法直接使用OnData、OnOrderEvent、Scheduled Events、指标和Universe。正式Qlib targets按证券交付weight_ppb和batch_id，批次包含全部声明证券，空组合全部权重为零；算法收齐同一完整批次后下单。具体声明见[原生算法](strategy-protocol.md)。

## 执行与恢复

Workspace execute 合并准备与准入后运行并报告。独立 package lint 保留为主动诊断入口。

项目策略只接受显式声明的 `execution_backend=lean`。历史 RP 策略 Result 保持只读。当前研究使用已准入 LEAN 会话。

独立命令流程使用 `package admit → run → report`。安装和源码同步成功后，安装根的 `rp-integration/runtime-audit.json` 保存引擎源码记录和完整文件 SHA-256。正式计划及 Result 的运行环境锁记录该审计身份、行为版本及启动路径。缓存采用上游 LEAN 提交、适配器行为版本、.NET 和 Python 依赖版本，以及配置和市场元数据的有效内容身份。

运行和恢复默认核对安装审计身份和关键文件内容，并检查 Launcher 入口目录新增的执行文件。关键文件包括引擎和插件、基础运行库、Python 包版本元数据、配置模板和主要市场元数据。独立运行及恢复会重新核验。同一次运行内，Windows 目录通知确认安装未变化时可以复用检查。关键文件或安装合同发生变化后，按当前安装重新准入。

研究使用安装目录内的自定义市场元数据时，沿用已发布的引擎和依赖身份，并单独计算项目元数据文件的 SHA-256。缓存按自定义市场元数据的有效内容区分，严格审计覆盖引擎、依赖及本次实际采用的元数据。

需要检查全部依赖的实际内容时，用 `doctor --lean-home <安装目录>` 复验安装凭证，或在运行命令上加 `--strict-audit` 复验冻结计划的安装锁。严格审计还会枚举全部依赖，逐文件验证 SHA-256，并检查完整依赖集合。

原生算法使用已安装的中国券商、成交和费用模型。这些模型在证券初始化或加入证券集合时检查并冻结，模型修改会在原位被拒绝。现货完整交易规则按实际消费的证券构建，行情仍检查当日历史规则覆盖。

冻结自定义交易时段和证券属性时，安装凭证同时绑定文件内容与解析后的业务身份。普通运行核对文件内容、关键二进制及该凭证；严格审计重新解析元数据并检查完整安装。跨进程加载计划时先核对冻结文件，只在严格模式重新计算完整查询和算子图的语义摘要。

Supervisor 直接启动 LEAN。Windows 在引擎开始执行前绑定 CPU 和 Job Object，宿主退出时回收全部后代。资源监督按秒采样。整个进程树受声明的内存、临时空间、CPU、累计时间、初始化initialize_seconds及单时间步time_step_seconds额度约束。

Windows Job 在宿主退出时清理后代。引擎退出码、正常完成状态和正式输出均满足后，节点才提交工件。

成功节点沿用现有 checkpoint 恢复合同。中断或失败的 LEAN 节点从该节点重新执行，先前成功的独立数据或研究节点可按原身份复用。算法内选股随该LEAN节点重新执行。修改引擎安装或桥接实现后须重新准入，不能继续使用旧成功检查点。

运行故障先查看run目录的节点诊断和LEAN日志。安装、实现或输入变化后重新准入到新运行目录，保留原Result及其依赖。

## 结果与金融计算

正式Result使用research.lean-simulation.v4，保存真实订单事件、成交、持仓、账户、生命周期及金融指标；原生算法无策略意图时intents为空。trace保留真实提交、更新、撤销请求及响应。输入行情与来源保留持久位置。

迁移时一并携带引用依赖。报告直接读取正式表。

日线按最新已可见价格估值：当日 Open 可用时使用 Open。缺 Open 时使用已可见历史价格，除权参考价沿用公司行动规则。每日账户及最终账户保存 `equity_semantics`、`valuation_at` 和逐证券 `valuation_marks`，记录价格来源和可见时间。

该序列不是当日收盘净值。日线指标定义使用 `daily_latest_visible_mark`，报告注明相同口径。一分钟使用执行窗口结束时的已完成行情估值，完整收盘为`session_close_mark`，盘中终态为`execution_window_mark`。

指标频率为`daily_execution_window`。年化仍按观察的交易日期计数，盘中首尾日期的比较限制进入Result和报告。

LEAN的simulation端口交付八张执行表和一张指标表：

| 表目录 | 内容 |
| --- | --- |
| `lean/orders` | 订单事件、成交、费用及对应账户变化 |
| `lean/intents` | 显式策略意图；原生算法无意图时为空 |
| `lean/daily` | 逐交易日日终账户与持仓 |
| `lean/trace` | 原生请求及响应、订单事件和算法生命周期 |
| `lean/fills` | 真实现货成交；期货成交腿、开平、方向桶、费用与实现盈亏 |
| `lean/positions` | 现货持仓、可卖量与锁定股份；期货方向与今昨桶快照 |
| `lean/account_events` | 现货现金和权益；期货浮盈亏、保证金、冻结及可用资金 |
| `lean/lifecycle` | 公司行动及退市；期货预占释放、结算、风险、换月与退出 |
| `lean/metrics` | 金融指标、定义、单位及样本范围 |

支持文件包括`lean/account.json`、`lean/algorithm-state.json`、`lean/runtime-lock.json`、`lean/strategy-input.json`，以及引擎终态与Launcher摘要。行情、历史规则、公司行动和信号输入通过持久工件与计划引用保留。迁移、归档或清理时同时保留这些依赖。实际原生输出与Chart封存在lean/native-output，ObjectStore封存在lean/storage，算法源码封存在lean/strategy-source。native-support.json列出入口。algorithm-state只保存算法身份和终态，不构造逐事件JSON策略状态。详见[原生结果](lean-native-results.md)。

输入准备检查正式 PIT 快照并保存小型来源清单、策略输入、公司行动及市场元数据。必要金融事实按执行需要投影，来源身份保留在原准入输入及持久工件中。

研究DAG由实际数据、模型或策略计算节点组成，成功后发布Result。模型最终评价继续记录持久holdout账本，实际读取前登记消费。报告直接使用正式指标和执行表。

LEAN 与冻结的中国市场规则在下单、成交及账本转换处执行金融检查。检查包括信号和行情可见性、后续分钟成交、滑点、费用和参与率。数量格点、保护价、T+1、可用现金和日终权益也参与检查。原生账户视图区分持仓、可卖数量、活动订单与扣除金额买单预占后的现金。

现金红利按登记日持仓确认权益，付款后进入现金。整数送转分别按股份到账和可卖时点处理。应收红利、待到账和锁定股份使用当时可见价格估值。开发验收用手算案例检查登记后卖出、跨日到账、费用和购买力边界。

普通送转允许真实到账日在登记日、早于除权日。原始到账日期保留，经济交付日取除权日与实际到账日较晚者，除权前不增加股份估值。登记日按收盘持仓确认权益；日频桥接把登记日到账映射到15:00，其余到账日映射到09:30，这是日频执行边界假设。新增股份按真实上市日开放卖出，不再作为当天买入重复锁定一天。

公司行动快照算子 `data.minute.adjustment_snapshot` 通过 `trading_calendar_request_id` 读取已准入的 `cn_market.trading_sessions` 请求。请求包含 `fld_cn_trade_day`，覆盖最早相关公告至研究窗口结束，保留完整交易日。版本选择、复权因子和金融权益都使用该日历，以实施公告后的首个交易日 09:15 为公告时钟。日历工件引用计入快照身份。未绑定日历的声明只接受能够由日线日期直接确认的情况，即公告次日就是交易日。

原始股票一分钟策略可将 `corporate_actions` 端口直接绑定到 `data.columnar.materialize`，并在算子参数 `stock_corporate_actions` 中声明 `action_request_id` 和 `calendar_request_id`。两个请求来自同一列式工件。行动使用覆盖完整冻结股票及执行窗口的 `cn_corporate_action.events`，日历使用覆盖相关公告至执行窗口末的 `cn_market.trading_sessions` 并投影 `fld_cn_trade_day`。两个请求均需按日期限定范围并保留完整事实，禁止过滤和限行。需要在期末保留窗口内已登记、窗口后除权的权益时，公司行动请求须包含这些除权日。该入口与 `snapshot` 端口互斥。

`stock_corporate_actions` 可声明同一权益合同的 `financial_missing_date_policy` 和 `financial_entitlement_exclusions`，实际假设进入正式结果。LEAN 从冻结逐日规则读取除权日 09:30 已可见的最高修订版前收，逐日表缺少对应记录时查询冻结通用规则。账户按正式公司行动推进登记、应收、到账及可卖，初始空仓的历史登记数量为零。复权研究可独立声明其输入。

先运行独立金融包生成市场规则的项目，可在策略包中使用 `data.minute.adjustment_snapshot_import@1.0.0`。导入节点声明 `published_snapshot` 来源凭证和其中的 `request_ids`，交付具有原工件身份的 `snapshot`。导入依据正式快照表追溯计算节点、合并节点和实际输入。源运行须成功发布快照表，并保留原计划、运行记录、实际依赖工件和已发布快照。目标计划须保留对应准入请求、研究时钟、种子和主只读数据库版本。无关研究请求继续走自己的只读数据路径，无需交给金融计算节点。策略仍按逐证券适用范围和市场规则核对复权及公司行动身份。项目组包可用 `research_pipeline.application.published_adjustment_snapshot.load_published_adjustment_snapshot` 生成来源凭证。

股票分钟研究可在该节点声明 `stock_open_state_request_id`，绑定完整交易日历及已准入的 `cn_equity.session_open_state` v2。开盘请求选择 `fld_equity_open_state_date`、`fld_equity_open_state_code`、`fld_equity_open_state_factor`、`fld_equity_open_state_pre_close`，覆盖各证券的股票分钟窗口。原日线请求继续提供窗口前的已完成日种子与历史收盘价。开盘原始因子从上海时间 09:30 生效，缺少开盘因子、窗口中间缺交易日或重复行时拒绝。共同尾部无行情时，快照覆盖截止到下一交易日开盘，后续时点无法消费该因子。上市首日使用当日原始因子，允许前收为空，适用起点最早为 09:30。停牌日使用当日开盘观测。

该股票快照的 `factor_source` 为 `stock_session_open`。复权段序列保留供应商因子精度，因子变更与纳入的公司行动分开记录。段中的除权参考价取原始开盘前收。成交使用原始行情，现金与股份权益按实际公司行动处理，申报及成交保护价使用已绑定的当日交易规则。开盘请求使用供应商当前历史版本，结果限研究观察。省略该参数时，股票和 ETF 仍可使用前日因子及当时可见公司行动构造快照。该参数仅适用于股票。

股票快照算子`data.minute.adjustment_snapshot`的`financial_missing_date_policy`默认为`error`，非零权益缺日期时拒绝。研究包可显式声明`omit_event`：派息日为空时不生成现金分红事件。红股或转增的到账日、股份上市日任一为空时，不生成对应送转权益事件。

现金、红股、转增分别处理，其他日期完整的权益继续执行。空金额和比例仍按零处理，非空但无效的日期与缺失来源继续拒绝。该参数不适用于ETF。

`omit_event`只改变账户金融权益，除权和PIT复权仍使用原始非零分配信息。参数随计划冻结并改变节点配置身份，正式Result的`limitations`记录忽略空日期权益的假设。收益和持仓不包含被忽略的分配，历史Result与检查点保持原身份。

也可显式声明`financial_missing_date_policy=sellable_date_receivable`：普通送转的实际到账日为空、登记日与除权日及真实上市日完整时，保留空源日期，除权后计入应收股份，上市日从应收一次结转为持仓。应收按当时可见价格估值，不提供现金购买力或可卖量；结转同时扣除应收，不重复增加净值。原股卖清不取消已登记权益。事件的`share_settlement_policy`记录该模型，数量事件的`pay_date`表示经济交付日，不是补填的源到账日；Result的`limitations`保存该假设。

登记股数为零的事件保留登记证据，零股份权益直接按零估值，无需订阅该证券。已有正股份应收仍按当时可见价格计价。

股票调整快照可显式声明 `financial_entitlement_exclusions`，按证据处理个别账户权益分量。列表每项记录字符串 `action_id`、`instrument_id`、`announcement_date`、`record_date`、`ex_date`、原每十股分配比例字符串 `source_ratio_per_ten`、原记录备注 `source_note`（可为空）、证据引用 `evidence_ref` 和决策 `disposition`（`omit_unresolved` 或 `verified_zero`）。`component` 可取 `cash`、`stock` 或 `transfer`。原日期对象 `source_dates` 在现金分量含 `cash_arrival_date`，在红股分量含 `stock_arrival_date` 和 `listing_date`，在转增分量含 `transfer_arrival_date` 和 `listing_date`。日期值可为空，且须与原记录对应日期一致。

决策随节点参数冻结，并按原事件身份、比例和源字段核对。来源补齐日期、修改比例或备注时，已有决策须重新核对。决策只影响指定的账户权益分量，同一记录中其他分量与原始除权复权继续执行各自规则。Result 限制列出事件、分量、决策及证据引用。

该模型不补齐缺失上市日、现金到账日或重整中原股东实际分配比例。未结转权益遇到下一次权益登记时，缺少受益资格依据则拒绝。普通送转出现零碎股份且没有真实分配依据时拒绝，不能下取整。策略通过`account.share_receivables`读取逐证券权益，字段见[策略协议](strategy-protocol.md)。

金融指标包含收益、回撤、波动、夏普、换手、费用及交易数量。收益含首日相对期初资金的变化，回撤使用含期初资金的运行峰值。年化使用252个交易日，夏普采用零无风险利率。定义、单位和样本随 Result 保存，算法更改使用确定性案例验收。

HTML报告展示净值、回撤、成交和费用。将正式Result ID与表ID写入请求文件：

```json
{
  "contract_version": "lean-research-report-v2",
  "result_id": "<正式Result ID>",
  "tables": {"daily": "lean_daily", "orders": "lean_orders", "intents": "lean_intents", "trace": "lean_trace", "fills": "lean_fills", "positions": "lean_positions", "account_events": "lean_account_events", "lifecycle": "lean_lifecycle", "metrics": "lean_metrics"},
  "budget": {"max_rows": 100000, "memory_bytes": 268435456}
}
```

```powershell
python -m research_pipeline report --result '<Result目录>' `
  --result-store '<Result存储根>' --format html --request '<报告请求JSON>' `
  --output '<报告HTML>' --summary
```

Markdown和JSON报告沿用同一正式指标入口，不需要HTML请求文件。

## 已知限制

- 一分钟按后续完成分钟 Close 加声明滑点成交。日线按下一交易日已可见开盘价成交。日线账户估值记录价格来源和时间，不等同于当日收盘净值。
- 当前金融模型支持已准入人民币股票、ETF或真实期货合约的日线和一分钟；原生Scheduled Events沿同一历史时钟执行。现货分钟支持Market、Limit、MarketOnOpen，日线支持Market开盘执行；期货支持Market/Limit、DAY/IOC和显式开平。Stop、TrailingStop及全部TIF尚无完整支持合同，不能由LEAN API存在推断支持。
- 不保存可跨进程接续的LEAN内部状态。节点失败时重新执行整个节点；旧entry(context,event,state)回调不保留兼容入口，历史Result只读。
- 公司行动支持毛额现金分红和整数送转。个人红利税、非整数股份分配及其他行动类型需要独立合同。
- 日线与期货在 Launcher 内按交易日读取冻结来源。日志和构建缓存按生命周期规则处理。正式 Result、模型、holdout 账本、冻结声明与恢复工件须保留。直接来源运行依赖原数据库和行情文件。


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
