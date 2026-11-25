# 原生合约标识验收

`NativeSymbolContracts` 使用真实 LEAN `Symbol` 和订阅配置，核对供应商代码与原生期货别名的映射。日线、分钟、Feed 和 History 均按冻结的市场、品种及到期日匹配。

自定义数据同时覆盖字符串及隐式 `Symbol` 两条默认订阅路径，核对数据与会话时区均为上海，使用原生 `SubscriptionData.Create` 验证上海 09:30 在 UTC 01:30 发射。

合约测试核对上海时区、自定义证券绑定、原生期货到期月匹配以及研究字段的克隆和整数精度。实际 Universe 和 History 回调由 Launcher 场景覆盖。

分钟合约测试用两个小型 Parquet 文件覆盖跨自然月夜盘、午夜完成分钟、零成交量、消费时点截断和交易报价单位。订阅、History 和交易共用同一月的全量校验与证券明细缓存。

三个自然月的独立样本覆盖缓存淘汰、历史回载、完成分钟边界、入选行去重统计，以及独立读取器复用已校验索引。侧车损坏、执行报价精度变更或冻结文件变更时分别覆盖重新扫描或拒绝。盘中样本覆盖未订阅证券的规则检查、指定证券跨行组读取以及同月超预算时回载证券明细。

现货规则样本使用多行组的冻结规则与参数 Parquet，覆盖日内版本切换、窗口结束边界、空规则日、跨日估费、精确整数、重叠区间、不可用版本及参数冲突。

使用已安装 SDK 执行，构建产物放到安装外的临时目录。`LeanBin` 指向 Launcher 编译目录，最后一个参数指向已有元数据目录。

```powershell
$env:MSBUILDDISABLENODEREUSE = '1'
& "$Install/sdk/dotnet.exe" run --project research_pipeline/integrations/lean/engine/parquet-direct/tests/NativeSymbolContracts.csproj -c Release -p:UseSharedCompilation=false "-p:LeanBin=$LeanBin" "-p:BuildRoot=$BuildRoot" -- "$Install/rp-integration/metadata"
```

测试不安装插件、不启动回测、不访问数据库；临时清单在测试结束时删除。
