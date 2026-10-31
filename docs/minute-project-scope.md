# 分钟研究的项目证券与日期范围

A股分钟研究可以使用项目提供的历史日历和规则，证券、日期不必等于随包参考样本。完整材料通过同一 ResearchPackage 准入与运行主链；缺项按证券、日期和规则字段报告。

## 来源与覆盖

发布制品身份和项目请求覆盖分别校验。随包 v1 规则、日历及发布锚点保持严格校验；项目使用显式 v2 合同，继续绑定已发布能力身份。能力身份说明框架支持的协议，项目来源材料说明本次证券和历史区间。

| 对象 | 合同版本 | 项目要求 |
| --- | --- | --- |
| 日历 bundle | `minute-project-session-bundle-v2` | 完整政策版本、能力绑定及来源证据 |
| 日历政策 | `minute-project-session-policy-v2` | 证券分类、有效期、交易日、时段、available_at及前序版本身份 |
| 规则 bundle | `minute-project-rule-bundle-v2` | 完整证券分类、来源、历史规则版本及能力绑定 |

请求覆盖检查直接读取已选日历版本的时段边界，同日相同模板复用已解析时段；不为覆盖检查构造完整交易会话。策略调度仍逐证券核对共同交易时段，并按每个交易日的真实时段检查定时事件。

每个日历版本须有能力清单之外的来源证据。同证券政策版本连续绑定前序版本，证券分类保持一致；各证券、规则的修订从1连续保留。请求可使用来源的一部分，原始bundle和完整修订链保留在计划及持久输入中，消费者引用同一来源，不裁剪成最后一版。

规则 bundle 中唯一的 repository_snapshot 来源，其 locator 以 `#<完整日历bundle_hash>` 结尾，绑定同一份日历。金融快照继续使用已有内容身份机制。

## 项目声明

项目把完整日历和规则写入包内命名输入，节点只声明 `minute_scope` 小引用。输入固定保存在 `minute-scopes/<name>/`，包含 manifest 和 Parquet 文件；完整历史记录保留，重复时段和规则参数共用模板。引用含输入名和现有日历、规则身份，不接受任意外部路径。

```python
from research_pipeline.domain.minute_scope_input import write_minute_scope_input

def attach_scope(package_root, spec, calendar, rules):
    reference = write_minute_scope_input(
        package_root, name="market", calendar=calendar, rules=rules,
    )
    for node in spec["graph"]["nodes"]:
        if node["operator_id"] == "research.bars.minute_resample":
            node["parameters"]["minute_scope"] = reference
        elif node["operator_id"] == "finance.simulation.intraday":
            node["parameters"]["minute_scope"] = reference
            node["parameters"]["rule_bundle_hash"] = rules.bundle_hash
```

calendar、rules是已构建的 SessionPolicyBundle 和 MinuteRuleSnapshotBundle。项目策略节点同样声明 minute_scope。多个行情分支按各自请求范围检查；同名输入必须绑定同一完整来源。

同次包加载中每个命名输入读取一次，准入直接消费已加载对象。计划封存输入文件；运行加载计划时恢复对象，节点之间传小引用。Workspace 初始化和参数变体复制完整输入。正式结果保留输入版本及持久位置，生产消费者按当时可见时间选择规则并执行金融计算。

QueryIR继续显式声明证券、带时区的分钟起止范围、raw行情及预算。日历的policy_id须与Catalog中的资产族session policy引用一致。仿真只依据其bars输入分支定位请求，不把公司行动等辅助分支的查询当成行情范围。

修改项目声明后仍执行 `package lint → package admit → run → report`。Catalog Lock、来源绑定、漂移证明、完成bar与PIT门禁仍适用；输入快照也走同一门禁。

## 历史可见性

- 日历覆盖请求涉及的自然日期；交易日列表明确交易与非交易日。每个交易会话选用当日最早时段开始前已可见的版本，当日公布的后续修订不改写已经开始的会话。
- 制度、上市状态、ST、停牌和费用按有效区间、可见时间选择。规则区间内的修订可见边界也纳入覆盖检查，不能跳过中途缺失或不支持的版本。
- 有涨跌停的交易日，前收盘价、涨跌停值及其有效区间须逐日声明，前收盘价在消费时点已经可见。长期适用的制度参数可使用较长有效区间。无涨跌幅限制阶段沿用既有unbounded合同，不伪造价格上下界。
- 复权和公司行动须声明来源身份；运行时继续核对目标实际消费的PIT复权快照、公司行动载荷和规则绑定。填写摘要不能替代实际事实，空列表不能单独证明无公司行动。

1分钟行情归一化、较大周期重采样、复权和仿真均消费同一冻结日历。1分钟源批在65536行内合并处理，每行仍按所属历史交易时段检查；输出保持声明的批大小和紧凑缓冲区，未对齐输入继续按原周期聚合。正式计划保留完整日历；生产消费者核对交易日、会话、时段和DAY到期，开发用例覆盖跨日、临停和规则版本边界。

## 已知限制

- 项目v2范围当前只支持A股。ETF、指数和期货继续使用其原有来源及覆盖合同；不由A股范围扩展推断其他市场已经验收。
- 这里交付声明、覆盖和运行时消费能力，不附带新增全市场历史数据，不自动采集或写库。真实研究仍须验证实际来源覆盖。
- 实现身份改变后须重新准入，新计划不跨旧身份复用checkpoint；历史Result保持只读。


完整证券与日期范围的规则覆盖在包准入时检查。运行从冻结计划加载同一请求、规则和日历，仿真节点仅核对请求及来源绑定，不再次遍历完整覆盖。逐日期、逐可见版本的PIT选择及实际下单和成交约束仍在运行时判断。
