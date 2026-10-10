# 期货账户：多个合约共用资金

多个期货合约会共同占用保证金，某个合约亏损也可能影响整个账户的可用资金。共享账户把多空、今昨仓、费用和盯市放在一起核算，并按声明的规则处理风险退出。

它不代表已实现跨品种组合保证金优惠。现货与期货账户的现金也不会自动互相挪用。

## 参数与行为说明

共享期货账户以人民币分记账，真实合约的多头、空头、今仓、昨仓分别保存。日频与完成 Bar 分钟使用相同账户口径；现货账户与期货账户各自保有现金，不相互借用资金。

## 金额与交易顺序

- 已结算现金 C 加未实现损益 U 得到账户权益 E。
- 持仓保证金 M 按合约、方向分别累计，不按净仓抵销。
- 活动订单冻结 F 从权益中扣除，可用资金 A = E − M − F。
- 真实结算价可见后，未实现损益转入现金并重置持仓基价；估值本身不重复结算。

同一时点先处理已可见结算，再处理取消、会话到期释放、平仓及风险减仓，最后处理开仓。每笔确认成交更新唯一账本，后续订单读取更新后的资金和剩余容量。

今昨仓按显式交易会话收尾滚动。夜盘跨自然日但交易会话未结束，仍属于今仓。普通平仓按声明顺序拆成今仓和昨仓成交腿，分别计算按手或按成交额费用。

## 风险、换月与到期

出现保证金缺口时取消活动策略订单，释放预占并暂停策略。风险减仓按每手可释放保证金降序、合约、方向和昨仓优先排序，在后续合格行情事件实际成交；无量或停板不能虚构成交。部分减仓后重新核算费用和缺口。

过程可用资金可以为负，结果保留这段风险事实。期末权益和可用资金必须非负，所有订单终结、预占释放；未解除风险拒绝发布正式结果。

换月使用在当时已经可见的真实合约映射，按1:1手数先平旧合约，再按已平数量扣除已开及新腿预占开放新合约。未成交的旧腿不产生新腿额度。旧合约退出开始后禁止重新增仓。达到声明退出截止时间仍有持仓会停止，不自动生成结算价成交或实物交割。

## 输入与结果

输入版本为 `research-shared-futures-input-v1`。合约声明报价乘数、价格尺度、tick、规格可见时间及退出截止时间；规则声明有效交易日、可见时间、保证金比例、开仓/平昨/平今费率与来源。国债每百元面值报价使用相应人民币报价乘数，不能将合约面值直接作为损益乘数。

共享账户结果使用独立版本的九张表：orders、fills、positions、cash、costs、valuations、reservations、risks、rolls。positions 的唯一键包含账户、合约、方向、仓位桶和快照时点；cash 保存 C/U/E/M/F/A，valuations 的净值取 E。完整输入与市场事件随上下文封存，独立金融复核从这些事实重算交易和账户，不调用生产执行器。

## 范围

这是 Bar 级研究模型。商品、股指和国债期货的规则与报价换算必须逐类提供；实际历史覆盖以来源和正式结果为准。共享账户暂不接收滑点参数；只支持同类别同乘数的1:1换月。保证金优惠、融券、期权、到期交割不在本账户合同中。

临时文件、测试库和运行工件优先放在 I 盘；运行前显式设置 `TEMP`、`TMP`，不要依赖尚未重启的程序继承新环境变量。

共享账户费用、成交腿损益和结算金额按半升舍入到分；持仓加权成本保留精确有理数。保证金按同合约同方向总量计算，再分配到今昨仓，桶拆分不改变账户总保证金。

## ResearchPackage 入口

- 日频：`finance.simulation.shared-futures.daily@1.0.0`。
- 分钟：`finance.simulation.shared-futures.intraday@1.0.0`。
- 参数：`spec`、`market_request_id`、`event_field_bindings`、`max_event_rows`。
- `market` 输入连接 `data.columnar-bundle.v1`，事件只能来自已准入的 feature 请求；分钟额外连接 `data.minute-bars.v1` 的 raw 完成 Bar。
- 节点默认内存预算1 GiB，输入超过行数或内存范围时拒绝；完整 DAG 的预算另行明确声明。

通过 `platform.shared_futures_contracts.shared_futures_result_tables(node_id)` 构建完整 ResultSpec。已有主指标表时传 `primary_table=None`，共享金融表作为诊断事实参与强制独立验证。缺少九表或 context 均不能准入。算子保持本地准入范围，定义与依赖身份改变后重新准入。

`sessions` 冻结真实交易日、各日盘/夜盘区间、完整会话终点和可见来源。`rule.effective_at` 区分公告可见与收盘结算后生效；`exit_deadline` 受 `last_trading_at` 和 `unsupported_from` 的较早时点约束。国债 `face_value_cny / quote_basis` 必须等于 `contract_multiplier`，并与TS或TF/T/TL产品面值一致。

合约规格必须声明带时区的半开有效区间 `valid_from ≤ t < valid_until`；命令、换月决策及市场事件必须在该版本内，分钟 Bar 的起止均须有效。静态规格版本之间不能带仓拼接，也不能用当前 tick 覆盖历史调整；跨版本连续回测需要另行实现版本切换，P9 选择有来源的单版本窗口。


独立资金分配的旧期货入口保留原结果版本和结算价全平模型。共享期货入口复用公共订单确认；旧独立分配、旧分钟及新共享账本共同调用 `ledger.FuturesAccountCore` 进行成交、结算与可用资金核算，日频／分钟共享入口使用同一个新账户状态；旧结果不能直接映射成方向桶九表，旧checkpoint不能跨算子身份恢复。

最后完成Bar、结算和会话收尾时间相同时，先执行最后Bar，再对该合约结算，最后释放DAY余量并滚动今昨仓；输入记录排列不改变这一顺序。


## 工程示例与复验

`tests/test_p8_formal.py` 构建日频合成ResearchPackage；`tests/test_p8_formal_minute.py` 构建含raw scan/resample的分钟ResearchPackage。两者均执行lint、admit、运行、故障恢复、独立verify、report及导出副本重验，输入明确标记为合成工程来源，不使用真实数据库。

在仓库根目录复验，临时路径显式指向I盘：

```powershell
$env:TEMP = 'I:\Temp'
$env:TMP = 'I:\Temp'
$env:PYTHONDONTWRITEBYTECODE = '1'
$env:RP_P8_FORMAL = '1'
$env:PYTHONPATH = "$PWD\research_pipeline\src;$PWD\research_pipeline;$PWD\research_pipeline\tests"
$runRoot = Join-Path 'I:\Temp' ('rp-p8-formal-' + (Get-Date -Format 'yyyyMMdd-HHmmss'))
& '.trellis/.runtime/rp-p5-validation/venv/Scripts/python.exe' -B -m pytest research_pipeline/tests/test_p8_formal.py research_pipeline/tests/test_p8_formal_minute.py -q "--basetemp=$runRoot"
```

正式日频和分钟的节点默认内存均为1GiB；上述完整DAG验收声明8GiB、4个CPU槽，独立复核在Windows虚拟环境声明3个进程槽。
