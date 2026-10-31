# LEAN合成买卖入门

本例从公开源码生成完整ResearchPackage、策略bundle、自编译Catalog、Parquet输入归档及完整minute scope，再使用Windows x64安装入口构建LEAN。无需既有LEAN安装、数据库、Qlib、供应商账号或私有Catalog。证券599999.XSHG为虚构ETF，合成规则按T+1结算，数据与代码按Apache-2.0发布。

要求Windows x64、PowerShell 7及Python 3.10+。在QuantWitness公开源码根目录创建准备环境，再使用其Python生成输入。输出目录必须不存在：

```powershell
python -m venv .venv
.venv/Scripts/python.exe -m pip install . pyarrow==21.0.0
.venv/Scripts/python.exe examples/lean_quickstart/build.py --frequency 1d --output "$env:TEMP/quantwitness-lean-daily"
.venv/Scripts/python.exe examples/lean_quickstart/build.py --frequency 1m --output "$env:TEMP/quantwitness-lean-minute"
```

生成器只写指定目录的Parquet、JSON、YAML及策略bundle，不创建DuckDB。`generated.json`给出全部入口路径：`package`用于安装与转换；`bundles`保存源码闭包；`catalog_lock`为本例独立Catalog；`input_snapshot_manifest`用于正式归档准入；一分钟另提供`minute_root`。

两个频率使用相同证券和2024-01-05、08、09、10、11日历，可共用一次安装。01-05仅作为前收与初始复权因子会话；执行覆盖01-08至01-11。一分钟每天生成240个完成bar，上午09:31至11:30，下午13:01至15:00。日线完整OHLCV按下一会话开盘可见，独立开盘来源按本会话09:30可见。公司行动来源为空，是合成市场的明确定义；规则、前收、涨跌停及初始复权因子均实际生成。

从源包安装，然后用安装环境转换与执行：

```powershell
pwsh -NoProfile -File integrations/lean/install.ps1 -LeanHome "$env:LOCALAPPDATA/QuantWitness/lean" -FromPackage "$env:TEMP/quantwitness-lean-daily/package"
pwsh -NoProfile -File integrations/lean/run.ps1 -LeanHome "$env:LOCALAPPDATA/QuantWitness/lean" -FromPackage "$env:TEMP/quantwitness-lean-daily/package" -CatalogLock "$env:TEMP/quantwitness-lean-daily/catalog" -InputSnapshotManifest "$env:TEMP/quantwitness-lean-daily/inputs.json" -Output "$env:TEMP/quantwitness-lean-daily-run"
pwsh -NoProfile -File integrations/lean/run.ps1 -LeanHome "$env:LOCALAPPDATA/QuantWitness/lean" -FromPackage "$env:TEMP/quantwitness-lean-minute/package" -CatalogLock "$env:TEMP/quantwitness-lean-minute/catalog" -InputSnapshotManifest "$env:TEMP/quantwitness-lean-minute/inputs.json" -MinuteRoot "$env:TEMP/quantwitness-lean-minute/minute" -Output "$env:TEMP/quantwitness-lean-minute-run"
```

安装会下载公开依赖和源码、创建独立Python环境并编译引擎。具体可选参数以安装脚本说明为准。源包不声明`execution_backend`或空runtime；`integrations/lean/prepare.py`在安装完成后冻结实际LEAN runtime、重建配套bundle并转换为正式LEAN声明。源包不直接执行。正式入口继续经过Catalog、归档、PIT、规则覆盖及策略准入。

策略订阅bar，只在首次已可见报价时提交100份买单；下一会话取得100份可卖数量后提交卖单。所有订单为市价、DAY、零滑点。撮合遵守后续bar与T+1，策略只读取当前事件账户和已可见报价。初始现金10000元，合成费率为成交额的万分之三、每单最低0.05元，无印花税及过户费。策略算子声明12GiB、4CPU、6个进程槽、32GiB临时空间与120秒，沿用验收入口预算；策略上下文上限8MiB，包含冻结安装声明、当前事件和可见报价；声明上限不是实际占用。

正式运行目录保存转换声明、plan、run、artifacts、results、Markdown和HTML报告，以及验收计时。报告读取正式Result。需要迁移时同时保留源输入、Catalog、包、bundle、安装与计划引用。

## 已知限制

合成样本用于验证安装、时间合同、订单和现金记账，不表达真实市场表现。当前示例仅覆盖单证券、无公司行动、整手完整成交。日频以当时最新可见价格估值，不能把该估值当成本日收盘价。运行成功以正式Result发布及运行回执为准，生成包成功不代表引擎已执行。

## 临时文件

生成目录中的`strategy-source`是构建bundle使用的源码副本。安装downloads、cache、temp和编译bin/obj为临时材料；运行临时文件位于运行目录。输入归档、minute scope、bundle、计划、Result及其依赖须保留，不自动清理。

## 手算核对

`expected.yaml`独立保存固定成交、费用和每日现金预期。使用准备环境只读核对正式Result的orders、intents、daily、trace四表；脚本从各表Parquet读取`payload_json`，不重跑策略或撮合：

```powershell
.venv/Scripts/python.exe examples/lean_quickstart/verify.py --frequency 1d --result <日频正式Result目录>
.venv/Scripts/python.exe examples/lean_quickstart/verify.py --frequency 1m --result <一分钟正式Result目录>
```

一分钟在01-08的09:31决定买入，09:32以10元成交；01-09的09:31决定卖出，09:32以10.5元成交。买卖佣金分别为0.30、0.32元，现金为10000－1000－0.30＋1050－0.32＝10049.38元。

日频首个完整日线在01-09开盘可见，以10.5元买入100份；01-10开盘可卖，以11元卖出100份。佣金向上取整到分，分别为0.32、0.33元，期末现金为10000－1050－0.32＋1100－0.33＝10049.35元。日频按下一会话开盘执行，分钟按后续完成bar执行。核对还要求两笔意图、卖出当时可卖量足够、四个日终会话完整、期末空仓及无未完成订单。
