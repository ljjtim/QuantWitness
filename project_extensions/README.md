# 受控项目扩展

本文说明尚未达到框架核心晋级条件的项目算子合同。扩展源码目录只允许 Python 文件，以便生成内容寻址的受控 bundle；薄声明放在源码目录旁，不进入源码闭包。

Python 项目算子的公开构建路径为：

```powershell
python -m research_pipeline operator validate --spec project_extensions/<name>.operator.yaml --source project_extensions/<name> --format json
python -m research_pipeline operator build --spec project_extensions/<name>.operator.yaml --source project_extensions/<name> --output <仓库外 bundle 根> --format json
```

命令只读取显式声明和显式源码目录，不扫描当前目录，也不自动安装依赖。生成的内容寻址目录再通过 `--extension-bundle` 显式交给 package 准入和 Runtime。

项目 Verifier 使用独立 bundle，不复用 Operator bundle。Verifier 单独声明 verifier ID、版本、源码闭包、依赖锁，以及允许读取的 Result schema 和支持工件类型；项目专属 Metric 定义也随该 bundle 进入当前 Package 组合并冻结到 Result，不进入公共 Metric discovery。Result 未冻结对应身份、授权输入或 bundle 版本不一致时，verify 直接失败。需要支持列数多于有效观察行数的统计矩阵时，Verifier 使用 `project-verifier-output-v2` 从正式 Result 表复算矩阵行数、非零有效行数、列数和秩，公共门禁不采信 Operator 单方面声明。

项目特有的图、固定日期、参数网格、参考组合和验收语义留在 ResearchPackage；项目算法由
Python Worker bundle 实现。项目算子身份、Artifact 类型和策略只进入当前 Package 的组合
registry，不进入公共 operator discovery。Worker 只接收当前节点的 typed inputs、参数、
固定时钟、种子和资源预算，不读取完整图、兄弟节点参数或未绑定 admitted plans。
项目算法即使被多个声明使用，也不会自动成为 core 能力；通用计算进入 core 要经过公共晋级审核。

薄声明的 `resource_profile` 必须显式包含 `memory_bytes`、`cpu_slots`、
`temp_bytes`、`process_slots` 和 `wall_seconds`。`process_slots` 覆盖 Worker 及其
受支持的子进程树；`wall_seconds` 覆盖取得资源后的整个节点 attempt，逐分区和 causal
键批只继承剩余时间。资源画像属于各自扩展声明，公共 Runtime 不按项目或 Operator 名称覆盖数值。

项目专属中间工件不需要用空壳公共算子占位。声明可在顶层用
`project_artifact_types` 列出当前算子端口实际使用的项目 Artifact 类型；未在公共
registry 登记、也未显式声明的类型仍会拒绝，声明但未被当前算子使用的类型同样拒绝。
同一项目的组合准入只允许消费本批项目算子实际产出的项目类型，不能借此注册全局 schema。

静态validate/build无需安装项目依赖；实际Worker启动时会在加载扩展入口前核对dependency_lock与已安装发行包版本。缺包返回`project_dependency_missing`，版本不符返回`project_dependency_version_mismatch`。需在运行环境安装声明的精确版本或显式修改声明后重建bundle，不能把静态构建通过等同于运行环境满足锁。

需要项目源码解释复杂 JSON 参数时，可在薄声明登记 `parameter_preflight`：

```yaml
parameter_preflight: {module: operator, function: preflight}
```

对应同步函数签名为 `preflight(context)`，只能检查参数和固定运行事实，成功返回 `None`。
package lint/admit 会在数据库打开和正式运行前用真实冻结 context 调用它；项目代码应按
`collections.abc.Mapping` 和一般序列合同读取参数，不应依赖可变 `dict`/`list`。预检入口与
正式入口一起进入源码闭包和 bundle 身份。

扩展代码运行于可信本地协作环境，框架不是安全沙箱。子进程、超时和后代进程清理用于故障隔离；正式输入、输出 staging、端口、相对路径及提交内容仍由框架核验。扩展及第三方库可以正常使用 `tempfile`、`os.replace` 和动态导入，可能产生 staging 外本地副作用，代码作者负责其行为；框架不拦截任意 Python 的文件操作或数据库访问。研究数据库只读仍是必须遵守的使用合同，不能把能调用数据库当成写库授权。

`analyst_revision_event_study/` 的诊断只接受本次已验证的 events 批输入，保留行业/市值和同行可见性检查，并引用实际输入工件。合成事件及预期值只存在于 tests/fixtures；缺输入或必要字段时拒绝，不生成替代数据。当前研究包没有这项诊断所需的真实来源，因此移除了原无输入诊断节点和对应Result表，核心事件研究链仍保留。

活动声明和 bundle 只接受当前 project-operator v2 合同。v1 声明、bundle、plan 和 checkpoint 不转换、不兼容；需要继续研究时必须用当前声明重新 build、lint、admit 并新建 run。历史 Result 和验收记录只作只读证据，不是旧代码入口。

项目算子默认 `reuse_scope: same_run`。只有正式输出完全由 typed 输入、参数、fixed clock 和 seed 决定，且不依赖 project/run/node/attempt 等易变 ID 时，薄声明才可显式写 `reuse_scope: cross_run`。Runtime 还会同时检查确定性合同和 `artifact_write_scope: output_only`；没有该字段的既有声明保持同 run 恢复语义。

公开分发只保留本目录的扩展合同；使用者自行提供项目源码、声明和独立 Verifier。可执行的合成示例位于独立公开源码仓库的 `examples/`，私人研究项目及其专用构建脚本不随包分发。

## 分批输入、输出和状态

分钟输入保留 `ProjectPartitionInput.iter_batches()` 与普通跨月 state。非分钟 Parquet
输入支持同一已验证工件下的多个文件，按稳定文件顺序调用 `iter_batches(columns=...,
batch_size=...)`，必须完整消费；未允许的列、过大批次或漏读会失败。
`output_root` 保留 Path 行为，并提供 `write_batches(port=..., artifact_type=...,
relative_path=..., schema=..., batches=...)` 与 `write_state(relative_path=...,
schema_hash=..., chunks=...)`；每批或块最多 32 MiB，返回原 commit 字典。
表格按固定 8,192 行 row group 输出，改变输入批大小不改变文件身份。worker 输出和状态
只返回 staging 路径描述，父进程流式提交；普通 state 继续沿现有分区 checkpoint 恢复。
需要在同一输出端口封存多张表时，将文件写入 `output_root` 下的同一子目录，调用
`output_root.commit_directory(port=..., artifact_type=..., relative_path=...,
files=(...))` 返回一份目录提交。`files` 是该目录内全部文件的相对路径；Worker 和
Supervisor 都核对文件闭合与内容，正式 Result 仍从一个端口按表前缀选择。
目录默认以端口名作为 ExternalArtifact 前缀；若公共工件合同要求根级文件，可显式传入
`publish_at_artifact_root=True`。该选项只改变已核对目录在 ExternalArtifact 中的落点，
不会放宽输出目录、文件闭合或内容身份校验；项目不得提交根级 `manifest.json` 或
`COMMITTED` 及其大小写变体，这两个文件由 ExternalArtifact 核心生成。
异构列式数据端口和显式绑定的原始 1 分钟扫描端口都可通过
`inputs[...].admission(request_id)` 读取 Supervisor 从本次正式准入计划投影的 dataset、字段、
时间范围、原始 `as_of`、按固定时钟时区解释的排他 `as_of_cutoff`、来源 revision、
publication、日频可见规则和 claim ceiling。分钟扫描端口还会核对
已验证 `data.minute-bars.1m.v1` 中的 request、plan hash、source revision 和来源快照，只投影
准入元数据，不向该输入开放分钟数据文件。只有节点参数显式绑定的 request 可见，Worker 必须
完整读取并返回 metadata-only 消费轨迹；列式数据仍同时核对实际消费的列和行数。扩展不得自填
准入事实，也不能用元数据输入绕过正式分钟分区读取合同。

## 正式 Feature / Label

在 operator 声明 parameters 中加入必填 `causal_plan`（`value_type: json`），每个算子
处理一种正式因果输出。package 节点参数使用以下固定结构，没有表达式或回调：

```yaml
causal_plan:
  kind: feature
  output_port: features
  key_columns: [event_id]
  state_scope: independent
  sources:
    - port: data
      request_id: prices
      columns: [event_time, available_time, close]
      observation_column: event_time
      available_column: available_time
  work_items:
    - key_rows: [[event-a], [event-b]]
      decision_time: '2024-01-03T10:00:00+08:00'
      window_start: '2024-01-02T09:00:00+08:00'
      window_end: '2024-01-03T10:00:00+08:00'
      source_partitions: {data: ['2024-01']}
```

键使用对应正式 Feature/Label 表的完整键。source port 必须直接绑定声明的数据请求，
源列必须在 QueryIR 投影内；观察和可见时间角色必须由 Catalog 准入证明。逻辑月份按
已准入源时区解释，实际物理分区身份由核心绑定。Label 用 `kind: label`，窗口在决策之后。
需要逐决策 revision/interval 选择或日线开盘/收盘可见性编译的输入尚不接受。

核心按决策和窗口稳定调度，同窗最多 8,192 键一批，最低内存预算 256 MiB；不会随内存
预算改变 opaque state 的调用次数。扩展通过带 `port` 的受限输入读取，只能请求冻结列、
月份和窗口；默认读仅交付窗口内数据，显式越界请求立即失败。扩展忽略已经交付的部分行
仍保守依赖整批。扩展只输出当前键批的键和值，不得填写任何核心时间列。

`state_scope: independent` 每个键批独立；`carry` 在下一键批前由核心检查继承来源和时间
窗口，超界失败，不静默清空。输出事实与 state 同时继承本次交付和已有来源的并集。
这是受支持 ABI 的因果约束，不是恶意 Python 沙箱；不能用绕开 ABI 的文件读取声称合规。

### 中国股票日频来源

Catalog 以 `available.daily.v1/next_session_open` 准入的原始日线，可以在 source 中
显式绑定日期列和冻结交易日历：

```yaml
sources:
  - port: bars
    request_id: daily_prices
    columns: [date, code, close]
    observation_column: date
    available_column: date
    daily_time:
      rule: next_session_open
      timezone: Asia/Shanghai
      calendar_id: cn-equity-example
      calendar_source: 已归档交易日历的来源和提取范围
      sessions: ['2024-01-04', '2024-01-05', '2024-01-08', '2024-01-09']
```

`sessions` 必须严格递增，覆盖请求范围以及最后一个输入日之后的下一交易日，缺失行情
不能压缩日历。`calendar_id` 和 `calendar_source` 标识日历及其来源，完整绑定随计划、
输出元数据和继承来源冻结。日历变更不能继承旧时间证据或复用旧计划身份。
示例日期仅用于说明结构，真实研究须提供其实际冻结日历。

核心把 D 日数据的观测时间解释为 D 日 15:00，把可见时间解释为日历中下一交易日
09:30，时区固定为 `Asia/Shanghai`。项目不能自填更早的时点；原始 `date` 列仍以
日期交给算子。Feature 仅收到决定时点已可见的窗口内输入；Label 使用未来观测窗口，
其成熟时间由最后实际观测对应的下一交易日开盘确定。Label 请求不得流入 Feature。

日频正式输出使用 `features/` 或 `labels/` 表目录，核心生成 `artifact-metadata.json`，
绑定当前 `ResearchSemantics`、表内容和完整因果计划。每个键批单独保存为 Parquet；
用于机器学习的 Label 键批须保持单一 `label_end_time` 与 `horizon_sessions`，使开发区
读取可在物理行组边界排除 holdout。日期行键在计划中采用 ISO 字符串，在 Arrow 输出中
可采用 date 类型，由核心按输出 schema 对齐后附加逐行时间事实。日频模型工件中的
观察、决定、标签区间和可见时间统一保存为带 UTC 时区的时间戳，保留同一实际时刻。项目仍不得填写
`CORE_FEATURE_TIME_COLUMNS` 或 `CORE_LABEL_TIME_COLUMNS` 中的核心列。

## 待著而救统计指标

覆盖率使用输入全集分母，分组成员变化使用前后成员并集分母。指标身份、独立复核和历史结果边界见 [待著而救指标合同](dai_zhu_er_jiu_metrics.md)。

## 日频模型预测有效性

`qlib_prediction_validity/` 从已提交的 Feature、Label、切分、拟合审计、预测、选模、模型配置和 holdout 账本收集预测诊断事实。数据观察算子不适用于包含模型训练和候选选择的研究。

该扩展输出 `research-validity-facts-v1`，其中 `model_diagnostics.mode` 为 `walk_forward_prediction_v1`。事实明确绑定 Result 的表 schema 与模型配置；框架独立复核时间切分、拟合范围、逐时点选模、唯一 holdout 收据和 MSE。研究公式与原始输入的对应关系仍由研究包冻结的项目 Verifier 检查。正式指标从独立的 `project.stage3.prediction-metrics.v1` 端口输出，保留汇总MSE原值，并从封存预测补齐单位、观察日期范围、样本数和计算状态；这些字段由核心统计门禁核对。

Result 显式交付模型清单及 holdout 表时，模型诊断同时封存四阶段 holdout 账本。验证从 Result 读取，不访问原运行目录。该合同用于原始收益标签的回归诊断，只有交易仿真门禁不适用；标签、选模、holdout和统计诊断均需实际通过，结论上限为 `research_observation`。
