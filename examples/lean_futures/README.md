# LEAN双向期货合成示例

生成并执行双合约期货合成研究。

## 开始前

- 输入：Windows x64、PowerShell 7、框架和完整源码。
- 输出：合成输入、独立安装、正式交易结果与报告。
- 命令从包含 `pyproject.toml` 的公开源码根执行。
- 尖括号内容是待替换参数，输出目录必须尚不存在。

## 使用顺序

1. 生成日线或一分钟源包。
2. 按本页命令安装 LEAN。
3. 通过归档入口执行研究。
4. 保存实际 Result 目录。
5. 只读运行手算核对脚本。

本例生成完整ResearchPackage、两个合约的合成行情、项目会话与规则、策略bundle、自编译Catalog和Parquet归档。支持日线和一分钟，不需要个人数据库或供应商账号。IF2401.CCFX与IF2403.CCFX用于表达两个独立合约。价格、乘数10、tick 1、保证金10%、每手单边费用1元均为合成参数，不代表真实IF历史规则。

从QuantWitness完整源码根目录运行，输出目录必须不存在：

```powershell
python examples/lean_futures/build.py --frequency 1d --output "$env:TEMP/quantwitness-futures-daily"
python examples/lean_futures/build.py --frequency 1m --output "$env:TEMP/quantwitness-futures-minute"
```

`generated.json`提供package、bundles、catalog_lock、input_snapshot_manifest及minute_root路径。生成器仅写JSON、YAML、Parquet和源码bundle，不创建数据库。会话覆盖2024-01-02至05日，交易覆盖01-03至04日。01-05仅提供末日结算的观察时点。

Windows x64、PowerShell 7安装和正式运行入口：

```powershell
pwsh -NoProfile -File integrations/lean/install.ps1 -LeanHome "$env:LOCALAPPDATA/QuantWitness/futures-lean" -FromPackage "$env:TEMP/quantwitness-futures-daily/package"
pwsh -NoProfile -File integrations/lean/run.ps1 -LeanHome "$env:LOCALAPPDATA/QuantWitness/futures-lean" -FromPackage "$env:TEMP/quantwitness-futures-daily/package" -CatalogLock "$env:TEMP/quantwitness-futures-daily/catalog" -InputSnapshotManifest "$env:TEMP/quantwitness-futures-daily/inputs.json" -Output "$env:TEMP/quantwitness-futures-daily-run"
pwsh -NoProfile -File integrations/lean/run.ps1 -LeanHome "$env:LOCALAPPDATA/QuantWitness/futures-lean" -FromPackage "$env:TEMP/quantwitness-futures-minute/package" -CatalogLock "$env:TEMP/quantwitness-futures-minute/catalog" -InputSnapshotManifest "$env:TEMP/quantwitness-futures-minute/inputs.json" -MinuteRoot "$env:TEMP/quantwitness-futures-minute/minute" -Output "$env:TEMP/quantwitness-futures-minute-run"
```

源包在安装前生成。prepare.py冻结实际安装和实现，将策略转换为正式LEAN执行声明。运行按当前Catalog/PIT、规则、会话和资金约束准入，最终发布Result并生成报告。

策略在首次可见bar中为近月提交一手买开、一手卖开，为远月提交一手买开，共享10万元初始资金。分钟订单在后续完成bar成交。下一交易日提交平昨仓。

日线完整bar在下一会话开盘可见。因此两日窗口在第二交易日才开仓，期末继续持仓并处理独立结算。结算价分别比当日合成分钟价格高8点，能够区分结算与Close。

多空同时存在时，即使近月净仓为零，双向保证金仍保留。

正式结果保存orders、intents、daily、trace、fills、positions、account_events、lifecycle八张执行表及metrics。交易数量以手计，成交名义金额乘合约乘数。研究收益使用权益，末日结算归属与实际可见时刻分别保留。

开发验收可只读核对正式Result，不重跑策略或撮合：

```powershell
python examples/lean_futures/verify.py --frequency 1d --result <日频正式Result目录>
python examples/lean_futures/verify.py --frequency 1m --result <分钟正式Result目录>
```

分钟三手开仓、三手平昨仓共收6元费用。近月双向盈亏相抵，远月上涨10点贡献100元，最终权益100094元。日线三手开仓共收3元费用，远月从210点开仓到218点结算贡献80元，最终权益100077元。近月双向和远月毛保证金合计454元。

## 已知限制

本例验证公开安装、归档输入、双向持仓、共享资金及结算合同。它不代表历史规则复原或策略收益表现。日线开盘使用已声明的无即时容量约束假设。

一分钟容量受当根完成bar成交量约束。换月、风险减仓和节假日夜盘的专项案例见开发验收，不能由本例常规两日窗口推断覆盖。

## 文件保留

生成目录的strategy-source是bundle构建副本。安装cache、temp、bin和obj是临时构建材料。归档输入、Catalog、会话规则、bundle、计划、Result和实际安装引用属于可恢复研究依赖，应共同保留。

独立 `package lint` 用于主动诊断。普通正式流程从完整声明直接准入。
