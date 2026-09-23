# Catalog 与 PIT 准入

Catalog Lock 说明当前允许研究的数据集、字段、物理 binding、可见性、修订和漂移政策。它不是数据副本，也不把未来才可见的值变成历史可用。

## 从发现结果交给准入

```powershell
python -m research_pipeline catalog dataset search <关键词> --catalog-lock <持久Catalog-Lock目录> --format json
```

QuantWitness 不附带任何供应商或个人数据库的默认 Catalog。使用者先通过
`catalog validate/compile --definition ... --approvals ...` 生成自己的持久 Catalog Lock；搜索、
lint 和 admit 都复用这一个显式目录。框架不会猜本机数据库或私有仓库位置。

`catalog discover/compile/docs` 的输出位置由调用方明确指定，不依赖源码所在的父仓库。
`discover` 不允许覆盖输入数据源或写入源目录；`compile` 的 release 不能与声明、审批
输入重叠；`docs` 不能写进不可变的 Catalog Lock。其他工作目录名称不影响输出权限。

机器结果的 `catalog_context.lock_reference` 是本次实际加载、可直接传给
`package lint/admit --catalog-lock` 的稳定目录。dataset 命中项的 `bindings` 逐项来自这份
Catalog Lock，包含当前 dataset/binding 版本、`source_profile`、`environment` 和
`object_name`；`catalog_context.source_profiles` 与 `next_commands` 说明每个 profile 应填入
哪一个显式只读路径，但不会猜测本机数据库位置。field 搜索返回相同 Catalog 上下文，不在每个
field 上复制 binding。blocked dataset 的 `bindings` 为空，也不会生成 admit 建议命令。

## package admit 自动完成什么

1. 从 ResearchPackage 的 QueryIR 找到唯一 dataset/version。
2. 在显式只读数据源中解析唯一 source profile、environment 和 binding。
3. 观察当前物理 schema，生成 drift attestation。
4. 核对字段用途、as-of、可见性、修订和来源身份。
5. 编译 typed DAG、ResultSpec 和指标可达性。
6. 完成准入校验后原子发布不可变计划。
7. 核对数据库 size/mtime 前后不变。

正式因子数据还会在准入时取得短共享租约，按请求表的 `factor_storage_state` 反查当前
published publication，核对 validated 计算运行、不可变 Catalog 正文、配方关系表、
质量记录和数值异常记录，并把类型化发布事实写入已准入计划。因子发布和研究读取相互隔离，
框架不计算或发布具体因子。旧 `factor.generic.daily` 与 `factor_values` 长表绑定已退役。

`factor.custom.daily` 使用 `trade_date + stock_code + factor_id` 物理键和 `value` 数值列；
只读准入仍须有当前 publication、修订及可见性规则。`available.daily.v1` 的
`next_session_open` 必须按真实交易日历计算；混合可见时点的配方不能套用单一日频政策。
具体因子定义、风险模型、publication 身份和项目验证结果均由项目侧声明及保存。

调用方不传内部 drift 文件，也不在研究图中手工填写 Catalog/PIT hash。

## PIT 最低要求

- 财报：报告期不等于可见时间，按公告日或明确 available_at 对齐。
- 成分、行业、ST、停牌、复权、市场规则：按历史快照或有效区间查询。
- 日期型 QueryIR `as_of` 按 `fixed_clock` 自身时区解释为该本地日的排他结束，即次日
  `00:00`；分钟型 `as_of` 是带时区的绝对时点。两者都不能晚于 `fixed_clock`。
- 日频数据声明 `available.daily.v1` 时，admit 必须确认规则内容为
  `available_after=next_session_open`，并把该业务语义写入已准入计划。ETF momentum Runtime
  消费这份计划，用实际交易日序列生成下一开盘可见时点。
- 分钟数据：盘中只使用当时已完成的 bar。
- 修订数据：只能选择 as-of 时点已经出现的版本。
- 标签：不能成为特征、信号、选模或交易输入的祖先。

日频合同在 package 编译、Catalog 准入和实际算子消费处闭合；列式数据平面不额外声称按
每一行日频数据做 PIT 过滤。当前计划合同是`admitted-query-plan-v5`；旧v4及更早计划不会
兼容加载，必须从package重新admit。计划版本升级不改写已经提交的自包含Result。

## 什么时候重新 compile Catalog

只有数据集、字段、policy、binding 或审批发生变化时。普通研究复用当前 Lock；每个研究仍在 admit 时对显式数据源观察漂移，因为物理数据可能变化。

## 失败动作

- dataset/field 不存在：修 Catalog 或缩小研究声明。
- binding 不唯一：明确 source profile，不猜默认源。
- schema 漂移：更新 Catalog 并重新审批，或修复数据源；不绕过。
- 可见性/修订证据缺失：保持 NOT_RUN。
