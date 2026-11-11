# 研究包：把研究问题写成可执行说明

把研究目标、数据和算法写成完整声明。

## 开始前

- 输入：研究问题、数据范围、算法和评价方法。
- 输出：4 份声明、准入计划和正式研究结果。
- 命令中的尖括号内容是待替换参数。

## 使用顺序

1. 用 `package init` 创建草稿。
2. 补齐 4 份声明。
3. 按需要运行独立 `package lint`。
4. 用 `package admit` 检查真实来源。
5. 运行当前准入计划。

ResearchPackage 可以理解成一份实验配方：研究什么、用哪些数据、按什么步骤计算、如何评价以及需要什么输出。它把这些约定放在四份 YAML 文件中，方便检查、复用和交给别人继续研究。

自动创建的草稿仍需要填写。下面说明文件布局和必须明确的内容。


## 常用名称

| 名称 | 含义 |
| --- | --- |
| ResearchPackage | 数据、算法、样本、指标和资源的完整研究声明 |
| Catalog Lock | 已确认的数据字段、来源和可见性说明 |
| PIT（按时点可见） | 在过去某时刻只使用当时已知的信息 |
| QueryIR | 限定字段、范围、排序和预算的数据请求 |
| DAG（计算步骤图） | 节点之间的先后依赖 |
| Artifact（工件） | 带类型、内容身份和来源引用的产物 |
| checkpoint（检查点） | 通过正式验收的节点输出及恢复依据 |
| Result（正式结果包） | 正式表、运行摘要和配置、实现、输入引用 |
| holdout（最终留出集） | 开发选择之外的最终评价区间 |
| Supervisor | 监督执行、发布检查点和记录事件的主进程 |
| Worker | 执行节点计算的独立进程 |
| manifest（清单） | 记录文件、类型、范围和内容身份的文件 |
| schema（结构） | 表的字段名称、类型和约束 |

## 处理规则

ResearchPackage 是研究意图的唯一声明入口。它包含四个文件：

```text
package.yaml
localization.yaml
sources/sources.yaml
spec/research.yaml
```

`package init` 只生成四份可编辑的中性草稿，不附带证券、数据集、论文、日期或研究图。补齐字段后，同一严格 loader 才会产生正式 ResearchPackage。草稿 `lint` 指出当前阻断字段，不能准入或运行。

## 参数变体

`package expand-variants --base <基包> --manifest <清单> --output-root <目录>` 从一个完整基包原子生成多个普通 ResearchPackage。清单必须绑定基包 hash，并为每个变体提供稳定 `variant_id`、package slug、显示名、版本、graph ID。以及用 `node_id + parameter_name` 定位的既有参数覆盖。它不支持深层合并、表达式、模板、Python 或 SQL。

任一变体引用未知节点、未知参数、重复标识或不兼容值类型时，整个输出目录不发布。成功生成的包没有特殊运行语义，仍分别经过原有 loader、Operator 参数 schema、lint、admit、PIT、ResultSpec、run 和 report。

## package.yaml

记录 package 身份、当前 builder、指标合同和 claim 上限。当前创建入口只生成 operator graph package。

## sources/sources.yaml

记录论文、报告、说明或本地资料的来源方式：

- `citation_only`：只有引用，不声称离线重建正文。
- `archived_snapshot`：绑定本地归档正文、清单和内容身份。使用归档正文的命令必须显式提供归档根。

来源身份不能替代数据来源修订或 Catalog/PIT 证明。

## localization.yaml

记录原研究到本地市场、数据、成本、规则和 claim 的改造决定。每项决定说明原口径、本地口径、原因和对结论的影响，不能只写“已适配”。

## spec/research.yaml

至少声明：

- research_id、资产、频率、时钟和 seed
- 有界 QueryIR requests
- typed operator graph 与参数
- ResultSpec 与公共表
- 指标和 claim 合同
- 研究语义、holdout 和复现要求

指标按 ResultSpec 中的来源节点、端口和声明表结构绑定。同类观察节点可以分别输出正式指标表与诊断表。诊断表使用独立结构标识，不参与正式指标选择。多个节点同时声明同一指标结构时仍拒绝准入。

compiler 检查通用不变量。检查范围包括 typed ports、DAG、Feature/Label 祖先和 ResearchSemantics。时间、PIT、seed、资源和金融规则也属于检查范围，不会按 operator 集合猜测完整工作流。

当前没有获准的公共
workflow profile，因此正式 ResearchPackage 不接受未填的占位值。单项目完整流程由项目
package/extension 自己声明。

基础设施 Catalog admission 由 `package admit` 注入，研究作者不在图中手写 Catalog、漂移或 PIT hash。

## lint

```powershell
python -m research_pipeline package lint --package <package目录> --json
```

完整包的 lint 完成不需要数据库的静态检查。检查包括严格 schema、QueryIR、typed ports、DAG 无环和参数。ResultSpec、指标可达性、来源和已声明的资源预算也参与检查。

输出 `execution_ready=false`，并给出缺失项和下一条 admit 命令。草稿 lint 以 `issues` 一次报告独立可判断的缺口，包含 code、file、field、message、action。

YAML 解析失败不伪造依赖检查结果。诊断不产生半完整研究包，也不授予执行资格。

同一次包处理复用四份 YAML 的解析内容，lint、严格加载、编译准备和准入仍分别执行各自的检查。下一次命令重新读取文件。参数变体和来源导入修改声明后重新加载。返回的研究声明保持不可变，不会被其他阶段改写。

## admit

```powershell
python -m research_pipeline package admit --package <package目录> --catalog-lock <搜索结果.catalog_context.lock_reference> --data-db <source:prod只读DuckDB> --output <计划目录> --json
```

admit 先完成不依赖来源内容的声明检查，包括查询窗口、因果约束、策略、参数、ResultSpec 和指标闭包。再加载分钟规则并检查覆盖，之后才读取来源统计和执行 Catalog/PIT 准入。同次检查结果直接交接，仅补入平台生成的来源证明，不重复完整编译。

四份 YAML 在加载时解析，分钟 scope 文件在首次消费时加载。直接调用 loader 得到声明对象后，scope 文件错误在消费规则时报告。

admit 自动观察物理 schema 和 PIT，把当前 Catalog、数据 revision、clock、seed、core 实现和项目 bundle 闭包绑定到不可变计划。任一身份变化都需要重新 admit。

日期型 QueryIR `as_of=YYYY-MM-DD` 表示 `fixed_clock` 所在时区内该日的排他结束（次日
`00:00`）。分钟型 `as_of` 保留绝对时点。package compiler 会拒绝晚于 `fixed_clock` 的
QueryIR 截止。

日频 `available.daily.v1` 只有在
Catalog 规则为 `next_session_open` 时才准入。项目日频算法必须消费已准入规则和真实交易日映射。数据平面读取不等于日频逐行 PIT 过滤。

直接采集的 raw 分钟查询不额外传历史供应商版本清单。准入要求当前 Catalog binding 明确为
`raw + collected + pit_allowed`，并校验 completed bar、实际 schema、带时区的有界半开区间和
稳定主键排序。Runtime 把本次实际来源 revision 与读取内容身份写入 Result。

输入变化时不能
恢复旧 checkpoint。复权分钟和历史截面状态仍需要各自真实 PIT 快照。

当前公共 Recipe 为空，完整项目拓扑及其参数由项目侧 ResearchPackage 冻结。
市场规则的生效时间、可见性和来源仍须在 Catalog 与准入计划中核验。佣金假设不等于
交易所规则。项目使用完整声明，经 `package admit` 生成当前计划后运行。

## 项目算子

包只引用 operator ID/version，不引用 callable 或模块路径。新算法先在包外运行 `operator scaffold`。命令生成当前薄声明和 Worker ABI 示例。

替换为真实算法并补测试后，使用 `operator validate/build` 生成 bundle。将 bundle 显式交给 lint/admit。

项目 bundle 可声明单个正式 `research.feature-set.v1` 或 `research.label.v1` 输出，但必须
同时声明必填 JSON `causal_plan`。package 冻结来源、键、月份和时间窗口。实际准入核对
Catalog 时间角色，Runtime 用受限 iterator 的实际交付及继承状态生成时间事实，项目只写
键和值。

缺合同或来源时间语义不受支持仍提前拒绝，不能自行填写时间证明。具体结构、
资源限制与当前拒绝的来源见 [项目扩展合同](../project_extensions/README.md)。

项目 bundle 不能导入框架内部、采集器、数据库兼容层或因子发布器。入口只能在 Worker 的
`output_root` 下提交与声明端口和已登记 Artifact/schema 一致的真实工件。被 ResultSpec 选中的表仍
必须是正式 Parquet，不能用 JSON 或 scaffold identity 示例代替。

## 通用算子晋级

只有两个异构真实项目已经消费同一语义，并有独立 oracle、PIT/标签/边界攻击测试、通用参数与 Artifact 合同。且 core 不依赖项目目录时，才做人工晋级判断。晋级后所有消费者重新 lint/admit/run/report。不迁移旧计划和 checkpoint。

公共发现只读取已批准的定义和身份，不在日常运行中查找私有项目。正式晋级审查须显式传入两个项目的目录映射与 pytest 证据根。审查验证声明可编译、算子图异构、实际消费目标算子，并执行不同的 oracle 与攻击节点。

私有项目可在线下审查，不进入公开发行包或公开 CI。仅有临时合成包和能执行的测试节点不足以证明真实跨项目复用。证据测试如涉及写库，执行前另行确认。
