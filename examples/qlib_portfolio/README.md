# ETF的Qlib研究

本例使用10只虚构ETF；完整模型研究使用100个价格会话，开发研究只封存最终holdout边界前的79个价格会话。价格由`synthetic.py`确定性生成，输入为经过Catalog准入的封存Parquet，整个流程不需要数据库、联网或付费模型接口。

`development`用于研究迭代：只训练模型并评价validation，保留时间切分和真实拟合范围，不生成test预测，不读取holdout索引或打开最终holdout。`model`执行相同模型代码，继续逐时点选择、开发test评价与一次性最终holdout。三种模式共享数据生成器、特征公式、模型声明和Verifier。`portfolio`在完整模型研究之外，把开发test预测按冻结的最高三只等权规则接入日频现金引擎。

安装框架的`ml`依赖后，在公开源码根执行：

```powershell
python examples/qlib_portfolio/run.py --mode development --output I:/quantwitness/qlib-development
python examples/qlib_portfolio/run.py --mode model --output I:/quantwitness/qlib-model
python examples/qlib_portfolio/run.py --mode portfolio --output I:/quantwitness/qlib-portfolio
```

安装`ml-sequence`依赖后，下列命令默认比较GRU与Ridge；LSTM与Transformer通过后文的候选准备接口声明：

```powershell
python examples/qlib_portfolio/run.py --mode development --sequence-step-len 3 --output I:/quantwitness/gru-development
python examples/qlib_portfolio/run.py --mode model --sequence-step-len 3 --output I:/quantwitness/gru-model
```

窗口长度至少为2，必须同时满足日历、完整特征和可见时间要求。两个候选共用合格末端；原始Feature、Label及context、targets、members、exclusions随Result封存并独立核对。示例GRU、LSTM和Transformer使用CPU、单样本批次、两轮训练和原始收益标签，适合验证流程；模型参数可在准备阶段生成的研究包中按完整合同声明。序列选项当前仅支持`development`和`model`，不支持`portfolio`。通用序列模型合同见[序列模型说明](../../docs/walk_forward_model.md#序列模型与完整窗口)。

输出目录必须未使用且位于源码仓库外。准备、准入和运行结果分别保存在同一目录的`package/`、`plan/`、`run/`与`results/`；正式独立验证写入`verification.json`，报告写入`report.md`。只查看命令退出码不足以判断研究完成，必须检查Result finalized和VerificationResult的`status=pass`。

可用`--stage prepare|lint|admit|run|resume|verify|report`分阶段执行。恢复使用原目录与原冻结声明。`run`阶段也支持`--reuse-failed-run-root`与重复的`--require-reused-node`，在修改下游声明后要求已成功节点及上游严格复用；涉及最终holdout时必须显式要求复用`model_holdout`，不能把已消费的最终评价重新执行。同一研究须继续使用原输出目录：holdout账本按研究工件目录保存，框架不能跨任意新目录阻止重复访问。

## 量价与选定Alpha因子

使用同一单期限入口选择固定因子集合：

```powershell
python examples/qlib_portfolio/run.py --mode model --factor-suite volume_price_v1 --output I:/quantwitness/volume-price-model
python examples/qlib_portfolio/run.py --mode model --factor-suite alpha158_selected_v1 --output I:/quantwitness/alpha158-selected
python examples/qlib_portfolio/run.py --mode development --factor-suite alpha360_selected_v1 --output I:/quantwitness/alpha360-selected-development
```

三个集合分别包含8项量价、22项Alpha158子集和18项Alpha360子集。量价默认完整窗口，Alpha子集保留Qlib0.9.7原生窗口政策；独立手算范围就是这48项固定定义。完整Alpha158/360定义读取不等于全部独立验收。合成归档按需添加high/low/vwap/volume，成交量单位为份；特征在前一会话收盘观察、下一会话09:30可见并决定。字段、公式、预热窗口、原始日线与独立oracle随正式研究封存。

固定因子的一步命令在独立进程完成prepare，再进入lint、admit和run；verify与report分别在独立进程读取同一Result。原生定义准备和模型训练的依赖占用不会带入后续节点或复核资源预算。`--factor-suite`在prepare阶段冻结；恢复沿用原目录和原声明，不重新选择集合。单期限development/model与序列选项共用相同特征列合同。多期限组当前使用原冻结特征。自有行情启用集合时，input-config的columns必须补齐该集合所需的high/low/vwap/volume映射，对应字段须已在同一Catalog及归档准入。完整说明见[量价与选定Alpha口径](../../docs/qlib-factor-baselines.md)。

研究输出通常放在仓库外。需要把合成Result保存到仓库内工作目录时，另用`--resource-state-dir I:/quantwitness/resources`显式提供仓库外的资源调度状态目录；该目录是普通锁与状态文件，恢复使用首次冻结的调用参数。

## 滚动与多期限研究

默认窗口为 expanding，训练、validation、test、步长及 embargo 分别为 30、10、10、10、1 个会话。下列滚动例把名义训练窗改为20个会话，随步长前移；合成输入可形成至少三个开发 fold：

```powershell
python examples/qlib_portfolio/run.py --mode development --window-mode rolling --train-sessions 20 --horizon-sessions 5 --sequence-step-len 3 --output I:/quantwitness/rolling-h5-development
python examples/qlib_portfolio/run.py --mode model --window-mode rolling --train-sessions 20 --horizons 1 5 --sequence-step-len 3 --output I:/quantwitness/rolling-horizons
```

窗口还可用 `--validation-sessions`、`--test-sessions`、`--step-sessions`、`--embargo-sessions` 声明。`--horizon-sessions` 表示单一期限；`--horizons` 表示一组期限，两者不能同时指定。期限按交易会话计，标签为 `close[t+h] / close[t] - 1`，t+h收盘结束、下一会话09:30成熟。日期尾部按期限收缩，窗口不够完整 fold、训练或validation经过purge后为空的声明会被拒绝。

组入口先准备并冻结全部成员，再通过独立进程逐个委托原有 lint、admit、run、verify、report。成员阶段结束后释放模型内存，下一期限按自己的节点预算运行。每个期限有独立目录、模型、Result、VerificationResult和holdout账本；组清单只引用这些结果。不同期限的MSE不合并，也不用holdout表现选择期限。组内已完成成员不重跑；恢复继续原声明、原运行和原留出账本：

```powershell
python examples/qlib_portfolio/run.py --output I:/quantwitness/rolling-horizons --stage resume
python examples/qlib_portfolio/run.py --output I:/quantwitness/rolling-horizons --stage verify
python examples/qlib_portfolio/run.py --output I:/quantwitness/rolling-horizons --stage report
```

多期限组不接受单研究的 `--reuse-failed-run-root` 和 `--require-reused-node`。需要调整候选、期限、输入或窗口时，应在新的研究目录准备声明，不能更换已冻结组的配置。`portfolio`沿用默认单日期限与窗口。

滚动窗口只控制开发 fold。每个fold分别训练处理器和模型，按当时可见的validation选模；序列 warmup可使用窗前已可见的Feature。最终holdout阶段仍使用全部合格开发历史重新拟合，末尾validation用于早停，标签时间继续参与purge。

## 声明模型候选

Python准备接口接受`model_candidates`，使用同一Qlib候选合同，默认候选保持不变。可在本例目录通过`prepare`创建研究，再按`run.py --stage lint|admit|run|verify|report`依次执行：

```python
from prepare import prepare, gru_candidate, lstm_candidate

small = gru_candidate(3)
small["candidate_id"] = "gru_small"
large = lstm_candidate(3)
large["candidate_id"] = "lstm_large"
large["model"]["kwargs"]["hidden_size"] = 8
prepare("I:/quantwitness/gru-candidates", mode="model",
        sequence_step_len=3, model_candidates=[small, large])
```

候选全集进入冻结研究设计、fit和holdout声明。候选变化须在准入前完成；已准入研究继续使用原声明。该例可比较GRU与LSTM结构，赢家仍由原有validation规则选择，最终holdout只评价冻结赢家。

Transformer候选可用同一准备接口：

```python
from prepare import prepare, lstm_candidate, transformer_candidate

prepare("I:/quantwitness/transformer-candidates", mode="model",
        sequence_step_len=3,
        model_candidates=[lstm_candidate(3), transformer_candidate(3)])
```

Transformer使用`d_model`和`nhead`控制注意力维度及头数，窗口不能超过1000个会话。示例默认`d_model=4`、`nhead=2`，其余资源与留出规则同上。

## 声明表达式变体

Python 准备接口 `prepare(root, mode="development", feature_expressions={"historical_return": "$close / Mean($close, 3) - 1"})` 可替换收益变化特征。两个既有窗口槽使用同一声明式，波动特征保持原定义；窗口槽仍保留原样本时间约束。正式独立复核支持以下收盘价公式，表达式值取决策前一完整会话，所有窗口只读取当时已可见的收盘价：

| 因子 | 表达式 | 窗口 |
|---|---|---|
| 收盘动量 | `$close / Ref($close, N) - 1` | N为1至5 |
| 均价偏离 | `$close / Mean($close, N) - 1` | N为1至5 |
| 价格离散度 | `Std($close, N) / Mean($close, N)` | N为2至5 |
| 区间位置 | `($close - Min($close, N)) / (Max($close, N) - Min($close, N))` | N为2至5 |

价格离散度是收盘价的样本标准差（ddof=1）除以均价，不是收益率波动率；区间位置在最高价等于最低价时记为缺失。任一所需价格缺失、非有限或非正数，因子同样记为缺失。

候选定义随包冻结，变化后必须创建新的研究变体。更多 Qlib 算子和 Alpha158/360 定义见[表达式说明](../../docs/qlib-expressions.md)；带提案与反思的运行入口见[因子研究循环](../../integrations/rdagent/docs/factor-research.md)。

## 金融口径

特征只使用决定时点之前已公开的收盘价，5/10会话收益变化与波动各一项。标签为当前会话收盘到下一会话收盘的价格变化，并在再下一会话开盘成熟。处理器只在实际合格训练样本拟合。两个Ridge候选仅在正则化强度上不同，validation属于开发区。

合成样本验证的是研究执行、时间合同、模型封存与独立复核，不表达真实市场收益或投资效果。组合模式使用`finance.simulation.daily-cash@1.0.0`，能力为`local_only`。正式结果分别保留预测误差和组合收益、回撤、成交金额比、费用；预测误差不代表交易收益。

组合规则在运行前冻结：每个开发test会话09:31按当时可见的预测选最高三只，各占30%，保留10%现金；下一交易会话09:30按开盘价执行。决策基准为前一会话收盘价，按输入Catalog的次会话09:30可见政策对齐。目标节点只读取test预测所需列和收盘价格，不读取actual、最终选择评分、holdout预测或下一会话开盘价；成交行情通过独立端口供仿真使用。最终holdout仍只用于独立模型诊断，不参与组合规则选择。

组合Result包含目标、决策基准、执行行情、规范订单/成交/持仓/现金/成本/估值六表和TCA。项目Verifier复核预测排名、固定权重、基准和执行会话，并从正式成交与净值重算绩效；框架金融oracle独立复核成交、费用与结算。交易分类为合成权益ETF，佣金300 ppm，最低佣金0.05元，初始现金10万元，无公司行动。

## 文件

- `synthetic.py`与`inputs.py`生成合成行情、Catalog和封存输入。
- `prepare.py`生成声明、算子bundle和独立Verifier；`portfolio_plan.py`声明组合图和绩效指标。
- `extension/`保存项目计算，`verifier/`保存独立公式复核。
- `run.py`委托框架正式CLI主链，不另建运行器。

运行日志、MLflow文件存储、临时排序与Worker输出都保留在指定输出目录。`tmp/`为临时工件；`results/`和`verification.json`属于正式结果。

## 接入自己的封存行情

先用prepare生成完整、可直接读取的配置模板。这一步只封存合成输入和研究声明，不训练模型、不打开holdout：

```powershell
python examples/qlib_portfolio/run.py --mode portfolio --stage prepare --output I:/research/etf-template
Copy-Item I:/research/etf-template/input-config.json I:/research/own-etf-input.json
```

生成的`input-config.json`包含实际证券、日历、完整来源和本地化条目、金融配置及已生成Catalog/归档的绝对路径。保持内容不变时，可以直接作为合成研究输入；迁移自己的数据时，在副本中替换研究身份、已批准的持久Catalog和归档清单、证券、实际交易日历、逻辑字段映射、来源和金融事实。配置说明来源，不授予新数据语义批准。自己的归档仍需先按[Catalog与PIT准入](../../docs/catalog.md)完成准备和批准；无需手写四份ResearchPackage YAML。

使用同一入口传入填写后的配置，执行相同的ResearchPackage主链：

```powershell
python examples/qlib_portfolio/run.py --mode portfolio --input-config I:/research/own-etf-input.json --output I:/research/etf-portfolio
```

`--input-config`只在prepare阶段读取，配置正文与规范化归档引用随后封存到输出目录。后续lint、admit、run、resume、verify、report使用同一目录。配置不能包含SQL或任意runner，也不会生成新的数据语义批准。Catalog Lock和归档必须提前准备，归档物理binding需已批准；可见性、修订和PIT仍由正式admit检查。

配置字段：

| 字段 | 内容 |
| --- | --- |
| `contract_version` | `qlib-own-input-v1` |
| `research_id`、`display_name` | 自己的研究身份与显示名称 |
| `catalog_lock`、`input_snapshot_manifest` | 持久Catalog根和封存输入清单路径；相对路径以配置文件目录为基准 |
| `calendar_sessions`、`calendar_id`、`calendar_source` | 严格递增的真实交易日、日历身份与来源；至少102个会话 |
| `entities` | 排序且不重复的固定证券池，至少三只 |
| `columns` | `date/code/close/open/high_limit/low_limit/paused`到Catalog逻辑字段ID的映射 |
| `sources`、`localization` | ResearchPackage原生来源条目和本地化决策列表 |
| `snapshot_scope` | 数据范围、固定证券池选择与结论边界 |
| `fixed_clock` | 覆盖标签成熟日期的带时区冻结时钟 |
| `finance` | 下述完整金融配置 |

金融配置必须提供`market_rule_profile_id`、互斥且完整覆盖证券池的`bond_etf_codes/equity_etf_codes`、`commission_ppm`、以分为单位的`min_commission_units`、`initial_cash_cny`、`corporate_actions`、`corporate_action_evidence`、`classification_evidence`、`policy_available_at`和固定`price_scale=3`。报价保留0.001元，现金结算为分；输入超出报价精度会拒绝，不先舍入行情。空公司行动列表也要给出覆盖该证券与区间的证据，不能从价格存在推断。规则可见时间不能晚于研究起点，品类及历史上市依据必须如实声明。

清单必须恰好包含`daily_feature`和`daily_label`，用途分别为Feature和Label。两份归档均使用不复权日线，证券、字段与日期范围必须和配置一致；可见政策为`next_session_open`。日历末两个会话只用于标签成熟，完整模式的行情覆盖第一会话至倒数第三会话。研究在第12个会话开始，倒数第23个会话作为holdout起点；开发模式必须提供截止holdout前一会话的专用归档，不能引用完整holdout价格归档。

特征默认沿用本例价格公式；--factor-suite按冻结集合生成声明特征。标签沿用本例固定期限公式。next-session-open目标和执行保持不变；模型价格变化误差与交易组合收益分别报告。自有输入不会自动解决历史分类修订、复权和分红缺口；这些事实必须由来源及金融配置闭合。公开示例不包含私人行情、个人Catalog或凭据。
