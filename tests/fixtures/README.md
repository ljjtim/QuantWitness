# 公开测试输入

`daily_observation`、`minute_stock_session` 和 `minute_futures_night_session` 保存可复制的完整研究声明。对应测试在临时目录修改窗口、证券和规则，并生成合成行情。

`minute_catalog.json` 只声明上述分钟测试消费的股票日线、股票公司行动、股票一分钟和期货一分钟四个数据集，包含49个字段、7项时点及漂移政策和4项物理绑定。分钟时区、完成bar、raw行情与PIT使用规则沿用当前接口，来源证据使用fixture引用。这个Catalog仅用于合成数据准入，不代表实际数据源审批。

测试通过 `CompiledCatalog.from_payload` 验证该声明，不读取安装环境或仓库内部的默认Catalog。
