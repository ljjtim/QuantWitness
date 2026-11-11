# 受控项目扩展

编写带类型和因果约束的项目算子。

## 开始前

- 输入：显式源码目录、薄声明、输入和输出合同。
- 输出：冻结的项目 bundle 与正式类型工件。
- 命令从包含 `pyproject.toml` 的公开源码根执行。
- 尖括号内容是待替换参数，输出目录必须尚不存在。

## 使用顺序

1. 按实际需求声明参数和端口。
2. 实现同步 Worker 入口。
3. 声明精确依赖和资源额度。
4. 执行 `operator validate/build`。
5. 通过研究包准入并执行。

本文说明尚未达到框架核心晋级条件的项目算子合同。扩展源码目录只允许 Python 文件，以便生成内容寻址的受控 bundle。薄声明放在源码目录旁，不进入源码闭包。

项目策略通过 ResearchPackage 声明并交给 LEAN 执行。回调接收 context、event 和 state，返回交易意图与 JSON 状态。合同和限制见[策略事件协议](../docs/strategy-protocol.md)。

Python 项目算子的公开构建路径为：

```powershell
python -m research_pipeline operator validate --spec project_extensions/<name>.operator.yaml --source project_extensions/<name> --format json
python -m research_pipeline operator build --spec project_extensions/<name>.operator.yaml --source project_extensions/<name> --output <仓库外 bundle 根> --format json
```

命令只读取显式声明和显式源码目录，不扫描当前目录，也不自动安装依赖。生成的内容寻址目录再通过 `--extension-bundle` 显式交给 package 准入和 Runtime。

项目专属指标通过研究声明的 `metric_definitions` 保存计算口径和生产实现引用，随 Result 保留。公式正确性由生产算法的手算与确定性测试覆盖。

项目特有的图、固定日期、参数网格、参考组合和验收语义留在 ResearchPackage。项目算法由
Python Worker bundle 实现。项目算子身份、Artifact 类型和策略只进入当前 Package 的组合
registry，不进入公共 operator discovery。

Worker 只接收当前节点的 typed inputs、参数、固定时钟、种子和资源预算。完整图、兄弟节点参数和未绑定 admitted plans 不进入 Worker。项目算法即使被多个声明使用，也不会自动成为 core 能力。通用计算进入 core 要经过公共晋级审核。

薄声明的 `resource_profile` 必须显式包含 `memory_bytes`、`cpu_slots`、
`temp_bytes`、`process_slots` 和 `wall_seconds`。`process_slots` 覆盖 Worker 及其
受支持的子进程树。`wall_seconds` 覆盖取得资源后的整个节点 attempt，逐分区和 causal
键批只继承剩余时间。资源画像属于各自扩展声明，公共 Runtime 不按项目或 Operator 名称覆盖数值。

项目专属中间工件不需要用空壳公共算子占位。声明可在顶层用
`project_artifact_types` 列出当前算子端口实际使用的项目 Artifact 类型。未在公共
registry 登记、也未显式声明的类型仍会拒绝，声明但未被当前算子使用的类型同样拒绝。
同一项目的组合准入只允许消费本批项目算子实际产出的项目类型，不能借此注册全局 schema。

静态validate/build无需安装项目依赖。实际Worker启动时会在加载扩展入口前核对dependency_lock与已安装发行包版本。缺包返回`project_dependency_missing`，版本不符返回`project_dependency_version_mismatch`。

需在运行环境安装声明的精确版本或显式修改声明后重建bundle。不能把静态构建通过等同于运行环境满足锁。

需要项目源码解释复杂 JSON 参数时，可在薄声明登记 `parameter_preflight`：

```yaml
parameter_preflight: {module: operator, function: preflight}
```

对应同步函数签名为 `preflight(context)`，只能检查参数和固定运行事实，成功返回 `None`。
package lint/admit 会在数据库打开和正式运行前用真实冻结 context 调用它。项目代码应按
`collections.abc.Mapping` 和一般序列合同读取参数，不应依赖可变 `dict`/`list`。预检入口与
正式入口一起进入源码闭包和 bundle 身份。

扩展代码运行于可信本地协作环境，框架不是安全沙箱。子进程、超时和后代进程清理用于故障隔离。正式输入、输出 staging、端口、相对路径及提交内容仍由框架核验。

扩展及第三方库可以正常使用 `tempfile`、`os.replace` 和动态导入，可能产生 staging 外本地副作用，代码作者负责其行为。框架不拦截任意 Python 的文件操作或数据库访问。研究数据库只读仍是必须遵守的使用合同，不能把能调用数据库当成写库授权。



活动声明和 bundle 使用 `project-operator-declaration-v2`。实现或声明变化后重新 build、admit 并新建 run。独立 lint 用于主动诊断声明。

项目算子默认 `reuse_scope: same_run`。只有正式输出完全由 typed 输入、参数、fixed clock 和 seed 决定，且不依赖 project/run/node/attempt 等易变 ID 时，薄声明才可显式写 `reuse_scope: cross_run`。Runtime 还会同时检查确定性合同和 `artifact_write_scope: output_only`。没有该字段的既有声明保持同 run 恢复语义。

公开分发只保留本目录的扩展合同。使用者自行提供项目源码、声明和指标定义。可执行的合成示例位于独立公开源码仓库的 `examples/`，私人研究项目及其专用构建脚本不随包分发。



## 受控Qlib项目模型

项目模型复用已有算子bundle的源码闭包和精确依赖锁，通过公开`research_pipeline.extensions.build_project_model_adapter`生成候选描述：

```python
from research_pipeline.extensions import build_project_model_adapter

adapter = build_project_model_adapter(
    bundle_path, "models", "ProjectModel",
    network_attribute="GAT_model", batch_contract="daily_cross_section",
)
candidate = {
    "candidate_id": "project-model",
    "model": {
        "class": "ProjectQlibModel",
        "module_path": "research_pipeline.research.modeling.project_model",
        "kwargs": {"adapter": adapter, "hidden_size": 64, "GPU": -1},
    },
    "dataset": {"class": "TSDatasetH", "step_len": 20,
                "missing_policy": "complete_window",
                "batch_contract": "daily_cross_section"},
    "processors": {"infer": [], "learn": [{"class": "DropnaLabel", "kwargs": {}}]},
    "fit": {},
}
```

模型类必须继承Qlib Model并满足当前序列模型fit/predict、网络与参数状态合同。项目构造参数由冻结源码解释，核心不登记项目专属参数。模块必须在bundle的sources中，依赖锁必须与实际环境一致。

源码建议使用相对导入。加载器按bundle身份建立独立包名，不把普通项目模块名加入全局搜索路径。

fit将完整bundle复制到模型支持文件，predict从该封存副本恢复。迁移Result不需要保留开发目录。原始收益、学习标签和图批次继续遵守正式模型主链。详情见[模型研究](../docs/walk_forward_model.md#图注意力与受控项目模型)。

## 分批输入、输出和状态

分钟输入保留 `ProjectPartitionInput.iter_batches()` 与普通跨月 state。非分钟 Parquet
输入支持同一已验证工件下的多个文件，按稳定文件顺序调用 `iter_batches(columns=...,
batch_size=...)`，必须完整消费。未允许的列、过大批次或漏读会失败。

普通同构 request 句柄在构造时核对各文件 schema，并在该句柄首次消费时复用。新句柄重新核对，实际数据仍逐文件读取。异构表按确切路径读取时各自检查 schema。

`output_root` 保留 Path 行为，并提供 `write_batches(port=..., artifact_type=...,
relative_path=..., schema=..., batches=...)` 与 `write_state(relative_path=...,
schema_hash=..., chunks=...)`。每批或块最多 32 MiB，返回原 commit 字典。

显式模型预测资格使用`output_root.write_prediction_index(port=..., relative_path=...,
schema=..., batches=..., semantics_hash=..., source_admission=...)`，返回
`research.model-prediction-index.v1`的完整目录提交。研究语义身份来自冻结声明，来源准入信息
使用输入的`admission(request_id)`。资格字段见[模型样本合同](../docs/walk_forward_model.md)。
该接口按固定8,192行合批并封存模型表元数据，项目只提供Arrow批次，无需导入框架内部运行时。

表格按固定 8,192 行 row group 输出，改变输入批大小不改变文件身份。worker 输出和状态
只返回 staging 路径描述，父进程流式提交。普通 state 继续沿现有分区 checkpoint 恢复。

若要在同一输出端口封存多张表，将文件写入 `output_root` 的同一子目录。调用 `output_root.commit_directory(port=..., artifact_type=..., relative_path=..., files=(...))` 返回目录提交。`files` 是该目录内全部文件的相对路径。Worker 和
Supervisor 都核对文件闭合与内容，正式 Result 仍从一个端口按表前缀选择。

目录默认以端口名作为 ExternalArtifact 前缀。若公共工件合同要求根级文件，可显式传入
`publish_at_artifact_root=True`。该选项只改变已核对目录在 ExternalArtifact 中的落点，
不会放宽输出目录、文件闭合或内容身份校验。

项目禁止提交根级 `manifest.json` 或
`COMMITTED` 及其大小写变体，这两个文件由 ExternalArtifact 核心生成。异构列式端口和显式绑定的原始 1 分钟扫描端口支持准入元数据读取。调用入口为 `inputs[...].admission(request_id)`。

Supervisor 从本次正式准入计划投影 dataset、字段和时间范围。投影还包括原始 `as_of`、来源 revision、publication、日频可见规则和 claim ceiling。排他 `as_of_cutoff` 按固定时钟时区解释。

分钟扫描端口核对已验证 `data.minute-bars.1m.v1` 的来源。核对内容包括 request、plan hash、source revision 和来源快照。端口只投影准入元数据，不开放分钟数据文件。

只有节点参数显式绑定的 request 可见，Worker 必须
完整读取并返回 metadata-only 消费轨迹。列式数据仍同时核对实际消费的列和行数。扩展禁止自填
准入事实，也不能用元数据输入绕过正式分钟分区读取合同。

## 正式 Feature / Label

在 operator 声明 parameters 中加入必填 `causal_plan`（`value_type: json`），每个算子
处理一种正式因果输出。package 节点参数使用这些固定结构，没有表达式或回调：

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
源列必须在 QueryIR 投影内。观察和可见时间角色必须由 Catalog 准入证明。

逻辑月份按
已准入源时区解释，实际物理分区身份由核心绑定。Label 用 `kind: label`，窗口在决策之后。逐决策 revision/interval 选择继续受当前支持范围限制。

日频日期规则按下文的 `daily_time` 显式声明。

核心按决策和窗口稳定调度，同窗最多 8,192 键一批，最低内存预算 256 MiB。不会随内存
预算改变 opaque state 的调用次数。扩展通过带 `port` 的受限输入读取，只能请求冻结列、
月份和窗口。

默认读仅交付窗口内数据，显式越界请求立即失败。扩展忽略已经交付的部分行
仍保守依赖整批。扩展只输出当前键批的键和值，禁止填写任何核心时间列。

`state_scope: independent` 每个键批独立。`carry` 在下一键批前由核心检查继承来源和时间
窗口，超界失败，不静默清空。输出事实与 state 同时继承本次交付和已有来源的并集。

这是受支持 ABI 的因果约束，不是恶意 Python 沙箱。不能用绕开 ABI 的文件读取声称合规。

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

核心把 D 日数据的观测时间解释为 D 日 15:00。可见时间为冻结日历中下一交易日 09:30。时区固定为 `Asia/Shanghai`。

项目不能自填更早的时点。原始 `date` 列仍以
日期交给算子。

Feature 仅收到决定时点已可见的窗口内输入。Label 使用未来观测窗口，
其成熟时间由最后实际观测对应的下一交易日开盘确定。Label 请求禁止流入 Feature。

同一 `daily_time` 结构也支持这些已准入日期规则：

| rule | 观测时间 | 可见时间 | 日历要求 |
| --- | --- | --- | --- |
| `next_session_open` | D 日 15:00 | 下一实际交易日 09:30 | 含全部输入日及末日的下一交易日 |
| `session_open` | D 日 09:30 | D 日 09:30 | 开盘日期须在冻结日历中，覆盖请求范围 |
| `business_effective_date` | D 日 00:00 | D 日 00:00 | 覆盖请求日期范围，业务生效日可为非交易日 |

时间统一按 `Asia/Shanghai` 解释。原始日期列保持不变。`session_open` 必须与 Catalog 的
开盘可见日期角色一致。

`business_effective_date` 只接纳已批准的逐日业务状态记录。上市日期不能代替整条证券参考记录的观测日期。退市属性必须按其实际可见时间交付。

`current_source_snapshot` 与 `research_observation` 的来源和结论上限继续保留，不表示存在
供应商历史发布版本链。日期窗口按来源本地日期核对，不随固定时钟显示时区改变。

准入调用须提供与请求身份一致的冻结 Catalog。规则、日历和来源身份随计划及
lineage 保存，继承状态禁止改换规则或日历。正式因果读取器按实际来源日期计算观测及
可见时间，Feature 只能消费决定时点已可见的数据。

日频正式输出使用 `features/` 或 `labels/` 表目录，核心生成 `artifact-metadata.json`，
绑定当前 `ResearchSemantics`、表内容和完整因果计划。每个键批单独保存为 Parquet。用于机器学习的 Label 键批须保持单一 `label_end_time` 与 `horizon_sessions`，使开发区
读取可在物理行组边界排除 holdout。

日期行键在计划中采用 ISO 字符串。Arrow 输出允许采用 date 类型。核心按输出 schema 对齐后，附加逐行时间事实。

日频模型工件统一使用带 UTC 时区的时间戳。时间戳覆盖观察、决定、标签区间和可见时间，并保留同一实际时刻。项目仍禁止填写
`CORE_FEATURE_TIME_COLUMNS` 或 `CORE_LABEL_TIME_COLUMNS` 中的核心列。

## 日频模型

模型使用 split、fit、predict、metrics、selection、holdout 六节点。开发研究只执行开发节点，汇总算法直接生成正式预测指标。Feature、Label、切分、拟合范围、预测、模型及实际使用的 holdout 账本由 Result 保存。完整声明见 [Qlib 示例](../examples/qlib_portfolio/README.md)。
