# 日频模型资源与共享编码复核

授权日期：2026-10-12。范围为日频模型逐批完整校验后的列投影、必需字段复制、split／locked holdout 内存声明及保持原字节身份的标量来源列表编码。

复核原批准公共定义集合 `3a910e3abe9707db1cc899308c9e7d039678f29ba2b91f68ae79e31f1f735f4f` 与当前源码。26 项定义的变化仅含下述两类：

- split 内存由 4 GiB 升至 16 GiB，locked holdout 由 8 GiB 升至 16 GiB。其他资源声明保持。
- 六项模型的实现及派生 spec 身份随加载源码更新；26 项 Runtime adapter 身份随共享等价编码依赖更新。

输入输出、参数、依赖模块集合、分区键、PIT 能力、随机种子、缓存合同、金融口径及能力上限均保持一致。日频现金与共享期货仅更新共享编码依赖的身份，能力仍为 local_only；不增加金融执行能力，不改变原本地准入范围。

新内建定义集合为 `6c38155e6360abbd2e632ad4e12faf9f8014cbc374b42d2ee96bfccf0cf511e6`，精确本地定义登记于 runtime/operator_promotion.py。清单门禁继续拒绝未审阅的定义变化、删除和新增。新研究重新准入；旧 Result、VerificationResult 和已消费 holdout 不被修改，也不跨新身份恢复 checkpoint。

资源工作集、测量结果和编码边界见[资源预算](../docs/project_resource_budgets.md#日频模型长表加载)。本次定向验收使用合成 Parquet、Qlib 模型文件和 JSON 账本，不连接数据库。真实周频 Result 与独立 VerificationResult 由使用该研究包的项目记录。
