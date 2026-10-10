# 基本数据接入：让框架认识你的行情

本教程接入一张已有的日频行情表。完成后，你会得到一份 **Catalog Lock（数据说明的固定版本）**，能查到数据集、字段和来源，并把它交给研究包使用。接入过程只读源数据库，配置与检查结果写到单独的工作目录。

第一次使用，先完成[入门教程](workspace-quickstart.md)。那里已经准备好合成数据和数据说明；这里再解释如何换成自己的来源。

## 1. 先判断手上的数据能走哪条路

| 手上的数据 | 接入方式 | 还要准备什么 |
| --- | --- | --- |
| 已有 DuckDB 日频表 | 本页主教程：观察表结构 → 编写数据说明 → 确认 → 编译 | 字段含义、证券代码、时区、可见时间和修订方式 |
| 已有 Parquet 文件 | 可以观察结构；正式读取要符合已实现的数据绑定或输入归档格式 | 不能只把数据库路径换成文件路径就假定能运行 |
| 原始分钟 Parquet | 使用分钟数据及其来源声明 | 完成分钟边界、时区、稳定排序、历史市场规则；见[分钟规则](minute_rule_provenance.md) |
| CSV、Excel、外部 API | 先在研究流程之外整理为受支持的数据源 | 类型、时间、重复记录与缺失值的处理说明；当前没有通用一键导入按钮 |

本页不采集行情，不创建或修改源数据表。若现有字段不够，先补齐来源事实，再单独安排数据整理。

## 2. 看懂最小的日频表

假设已有 DuckDB 表 `daily_prices`，含下列字段；这是一种可接入的表形状，不要求所有供应商使用同名字段。

| 字段 | 例子 | 含义 |
| --- | --- | --- |
| `session`，DATE | `2024-01-02` | 行情所属交易日 |
| `instrument`，VARCHAR | `SYN.A` | 证券唯一标识，例子使用虚构代码 |
| `close`，DOUBLE | `10.25` | 当日收盘价，单位为元；本例声明未复权 |
| `available_at`，TIMESTAMPTZ | `2024-01-02T15:10:00+08:00` | 这条记录何时真正可供研究使用；例值仅演示格式 |

`session` 和 `available_at` 不是一回事。1月2日收盘价，不能拿去决定1月2日上午的买入。`available_at` 应来自实际发布或采集记录，不能为了让检查通过而统一填写一个猜测时点。

这张表只够演示收盘价研究的数据接入。交易回测还可能需要开盘价、成交量、交易日历、停牌、涨跌停、费用、公司行动或期货合约规则。接入成功不代表已经具备完整回测数据。

本例按 `(session, instrument)` 唯一，四列均非空，并且**不重放修订版本**。如果供应商会覆盖旧值，这种声明不能用来声称恢复当年的原始版本；需要版本与发布时间事实，或明确限制研究结论。

## 3. 只读查看表结构

在已安装 QuantWitness 的 PowerShell 中执行。将 `$db` 换成已有数据库，`$setup` 换成自己选定的仓库外目录；目录不存在才创建。

```powershell
$db = 'I:/market-data/prices.duckdb'
$setup = Join-Path (Get-Location).Path '../quantwitness-work/data-setup'
New-Item -ItemType Directory -Path $setup -Force | Out-Null
python -m research_pipeline catalog discover --source-kind duckdb --source $db --object daily_prices --profile source --environment prod --output "$setup/inventory.json" --json
if ($LASTEXITCODE -ne 0) { throw '读取表结构失败；先检查路径和表名' }
$inventory = Get-Content -LiteralPath "$setup/inventory.json" -Raw | ConvertFrom-Json
$inventory.columns | Format-Table
$inventory.schema_revision
```

`source` 是数据源的名字，`prod` 是环境名字；两者必须与后面的数据说明一致。这里的“prod”只是绑定名称，命令仍然只读。

结果里的 `columns` 告诉你实际列名、类型和是否允许空值；`schema_revision` 记录这次观察到的结构。**观察表结构不会自动判断价格单位、复权方式或历史可见性。**

## 4. 把字段和可见时间写成数据说明

将下面模板保存为 `$setup/catalog-definition.yaml`。按实际结构调整 `object_name`、`physical_column`、类型和空值约定，将 `expected_schema_revision` 的占位内容替换成上一步返回的值。其余名称是研究时使用的逻辑名称，可以与物理列名不同。

<details>
<summary>展开：一张日频收盘价表的完整 Catalog 模板</summary>

```yaml
bundle_version: catalog-bundle-v1
coverage_slots:
  - slot_id: my_daily_prices
    decision: approved
    target_ids: [my.daily_prices]
policies:
  - policy_id: my.daily.available.v1
    policy_type: availability
    rules:
      available_after: source_available_at
      available_time_field: fld_my_available_at
      missing_available_time: reject
      timezone: Asia/Shanghai
  - policy_id: my.daily.drift.v1
    policy_type: schema_drift
    rules: {schema_changed: reject, unknown: reject}
  - policy_id: my.daily.revision.v1
    policy_type: revision
    rules: {mode: none}
datasets:
  - dataset_id: my.daily_prices
    market: cn_stock
    instrument_type: equity
    frequency: daily
    object_name: daily_prices
    source_profile: source
    environment: prod
    expected_schema_revision: REPLACE_WITH_INVENTORY_SCHEMA_REVISION
    primary_key: [fld_my_session, fld_my_instrument]
    sort_order: [fld_my_session, fld_my_instrument]
    event_time_field: fld_my_session
    available_time_policy: my.daily.available.v1
    drift_policy_id: my.daily.drift.v1
    revision_policy_id: my.daily.revision.v1
    result_cardinality: one_or_more
    fields:
      - field_id: fld_my_session
        logical_name: my.daily.session
        physical_column: session
        data_type: date32
        semantic_type: date
        unit: day
        nullable: false
        availability_policy: my.daily.available.v1
        observation_model: market_event
        observation_keys: {event_time_field: fld_my_session, available_time_field: fld_my_available_at}
      - field_id: fld_my_instrument
        logical_name: my.daily.instrument
        physical_column: instrument
        data_type: string
        semantic_type: identifier
        unit: dimensionless
        nullable: false
        availability_policy: my.daily.available.v1
        observation_model: market_event
        observation_keys: {event_time_field: fld_my_session, available_time_field: fld_my_available_at}
      - field_id: fld_my_close
        logical_name: my.daily.close_raw
        physical_column: close
        data_type: float64
        semantic_type: price
        unit: CNY
        nullable: false
        availability_policy: my.daily.available.v1
        observation_model: market_event
        observation_keys: {event_time_field: fld_my_session, available_time_field: fld_my_available_at}
      - field_id: fld_my_available_at
        logical_name: my.daily.available_at
        physical_column: available_at
        data_type: timestamp
        semantic_type: datetime
        unit: microsecond
        nullable: false
        availability_policy: my.daily.available.v1
        observation_model: market_event
        observation_keys: {event_time_field: fld_my_session, available_time_field: fld_my_available_at}
```

</details>

`physical_column` 对应数据库列；`field_id` 和 `logical_name` 供研究声明引用。`rules: {mode: none}` 是本例的数据假设，不应照搬给存在历史修订的数据。模板中的 `decision: approved` 只是声明内容；正式编译仍需要单独的确认文件。

## 5. 确认数据说明，再编译为 Catalog Lock

确认的对象是字段含义、价格单位、可见时间、修订约定和实际表结构。不是确认“策略会赚钱”，也不是批准写入数据库。

<details>
<summary>展开：生成独立确认文件</summary>

将这段代码保存为 `$setup/review_catalog.py`。核对过上面的说明后再执行；输入自己的署名，生成与该说明对应的确认文件。不要复制别人的确认文件去批准修改后的数据说明。

```python
import sys
from datetime import datetime, timezone
from pathlib import Path

import yaml
from research_pipeline.catalog import load_declarative_catalog

source, output = map(Path, sys.argv[1:3])
if output.exists():
    raise SystemExit("确认文件已存在，请先检查已有记录")
reviewer = input("已核对数据说明、可见时间和修订约定，请输入确认人署名：").strip()
if not reviewer:
    raise SystemExit("未填写确认人，不生成确认文件")
proposal = load_declarative_catalog((source,), allow_generated_approvals=True)
decisions = []
for item in proposal.decisions:
    decision = item.to_dict()
    decision["decided_by"] = reviewer
    decision["decided_at"] = datetime.now(timezone.utc).isoformat()
    decision["reason"] = "已核对本次字段、单位、数据时点、修订约定和物理结构"
    decisions.append(decision)
output.write_text(
    yaml.safe_dump({"approval_version": "catalog-approvals-v1", "decisions": decisions},
                   allow_unicode=True, sort_keys=False),
    encoding="utf-8",
)
print(output)
```

```powershell
python "$setup/review_catalog.py" "$setup/catalog-definition.yaml" "$setup/catalog-approvals.yaml"
if ($LASTEXITCODE -ne 0) { throw '未生成确认文件' }
```

</details>

```powershell
python -m research_pipeline catalog validate --definition "$setup/catalog-definition.yaml" --approvals "$setup/catalog-approvals.yaml" --json
if ($LASTEXITCODE -ne 0) { throw '数据说明不完整；先处理返回的问题' }
python -m research_pipeline catalog compile --definition "$setup/catalog-definition.yaml" --approvals "$setup/catalog-approvals.yaml" --output "$setup/catalog-lock" --json
if ($LASTEXITCODE -ne 0) { throw 'Catalog 编译失败' }
$lock = Join-Path $setup 'catalog-lock'
python -m research_pipeline catalog dataset search my.daily_prices --catalog-lock $lock --format json
python -m research_pipeline catalog field search my.daily.close_raw --catalog-lock $lock --format json
```

看到数据集和收盘价字段后，说明数据目录已经建立。这里没有复制行情；`catalog-lock` 保存的是说明与确认记录。日后修改说明，应重新确认并生成一个新目录，不直接编辑已编译的 Lock。

## 6. 交给研究包：这一步才检查实际输入能否使用

按[创建研究](getting-started.md)准备研究包，令它的数据请求引用 `my.daily_prices` 和上述字段。现成合成示例引用自己的数据集与验证器，不能只换一个数据库路径就当成真实研究。

```powershell
$package = Join-Path (Get-Location).Path '../quantwitness-work/my-research'
python -m research_pipeline package lint --package $package --catalog-lock $lock --json
if ($LASTEXITCODE -ne 0) { throw '研究包仍有未填写或不一致的声明' }
python -m research_pipeline package admit --package $package --catalog-lock $lock --data-db $db --output "$setup/plan" --json
if ($LASTEXITCODE -ne 0) { throw '未通过数据与时点准入；查看具体缺失项' }
```

`$package` 要指向已经填写完整的研究包，`plan` 输出目录应不存在。自定义算法和验证器按研究包要求另外提供 bundle。准入通过后，再按研究教程执行 `run → verify → report`。

**常见卡点：** 表名不对就改绑定；类型或结构变化就重新观察和确认；可见时间没有证据就补来源；缺历史市场规则就缩小研究范围或补齐规则。不要通过关闭检查把错误的数据变成“可用”。

## 接入边界与进阶参考

- 本例演示日频、单数据库、显式行可见时间。多个数据源、分钟归档、因子发布与下一交易日可见规则见下文和[数据读取](data_plane.md)。
- Catalog 描述可见性，具体算子和独立验证器还要正确执行这些约定；不能仅靠填写 `available_at` 就宣称所有研究自动无前视。
- 工作目录含结构观察、说明、确认、Lock 和计划；不要把它们当作通用默认数据随源码分发。源数据库始终只读。

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
