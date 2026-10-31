# RD-Agent：让 AI 参与量化研究

这个可选集成让 AI 提取论文定义、生成计算代码、提出因子或模型候选，再由 QuantWitness 执行正式研究。每轮直接读取 Result 中的正式开发指标。数据可见时间、开发范围、模型训练范围和最终 test/holdout 隔离在生产入口及消费者中执行。

## 从哪种研究开始

| 你现在有什么 | 可以做什么 | 指南 |
| --- | --- | --- |
| 一篇 PDF 和想复现的公式 | 提取指定页、人工确认、生成代码并执行 | [公式复现](docs/formula-reproduction.md) |
| 想先体验，不调用付费模型 | 用教学 PDF、合成分钟行情和固定响应跑通流程 | [成交量集中度示例](examples/volume_concentration/README.md) |
| 已有研究包与参数候选 | 在固定开发范围内比较方案 | [研究循环](docs/research-campaign.md) |
| 想提出新因子 | 根据正式开发指标提案和反思 | [因子研究](docs/factor-research.md) |
| 想改变模型结构 | 在支持的前馈结构范围内生成和评价 | [模型研究](docs/model-research.md) |
| 想同时探索因子和模型 | 共用开发范围与预算，保留候选来源 | [联合研究](docs/joint-research.md) |

## 一轮 AI 研究怎样流转

```text
问题与人工确认的约定
  → RD-Agent 提出或实现候选
  → Workspace 准入并执行研究
  → Result 与报告
  → 只返回允许的开发反馈
  → 下一轮候选，或按预算停止
```

公式复现坚持已确认的定义，不能为了提高收益改变原公式。正式执行成功表示产生了完整 Result；公式的数学正确性通过项目算法测试和手算案例验收。技术失败保留执行诊断，不生成金融指标。最终 test/holdout 不进入方向、提案、反思或知识提示。

## 安装与入口

本包复用上游 LoopBase 与 CoSTEER，固定提交为 `484776c211e4fbbeef03e0ec00d6bbee7362a4f4`。普通核心命令不自动安装或加载 RD-Agent。许可和来源见[第三方说明](../../THIRD_PARTY_NOTICES.md)。可选集成需要 Python 3.11+；固定上游调度使用独立 Linux 环境，Windows 可通过 WSL 使用。

`QUANTWITNESS_REPO` 指向含 `src/` 和 `integrations/` 的公开源码根；单仓库中为 `research_pipeline/`。环境、缓存和产物位于调用者选择的研究磁盘。

```bash
git clone https://github.com/microsoft/RD-Agent.git "$RD_AGENT_SOURCE"
git -C "$RD_AGENT_SOURCE" checkout --detach 484776c211e4fbbeef03e0ec00d6bbee7362a4f4
python3.12 -m venv "$RD_AGENT_ENV"
export RD_AGENT_PYTHON="$RD_AGENT_ENV/bin/python"
"$RD_AGENT_PYTHON" -m pip install -r "$QUANTWITNESS_REPO/integrations/rdagent/requirements-linux.txt"
"$RD_AGENT_PYTHON" -m pip install --no-deps "$RD_AGENT_SOURCE"
"$RD_AGENT_PYTHON" -m pip install --no-deps -e "$QUANTWITNESS_REPO/integrations/rdagent"
export PYTHONPATH="$RD_AGENT_SOURCE:$QUANTWITNESS_REPO/integrations/rdagent/src"
export TMPDIR="$RESEARCH_TEMP_ROOT"
export LITELLM_LOCAL_MODEL_COST_MAP=True
"$RD_AGENT_PYTHON" -m quantwitness_rdagent run --request "$RD_REQUEST"
"$RD_AGENT_PYTHON" -m quantwitness_rdagent inspect --request "$RD_REQUEST"
"$RD_AGENT_PYTHON" -m quantwitness_rdagent resume --request "$RD_REQUEST"
```

Windows RP 环境无需安装上游 RD-Agent。承担实时模型调用的环境安装本包 `live` 依赖，提取 PDF 的环境安装 `materials` 依赖。发行包和新环境操作见[安装说明](docs/installation.md)。

## 冻结请求

公式复现请求必填 `request_id`、`mode=formula_reproduction`、`base_package`、`source_archive_root`、`input_snapshot_manifest`、`editable_source_root`、`development_scope`、`runtime_binding`、`budget`。可选字段为 `code_generation`、`confirmed_formula`、`coding_knowledge`。`development_scope.role` 必须为 `development`。

`runtime_binding` 必须声明：

| 字段 | 含义 |
| --- | --- |
| windows_python | Windows RP Python 绝对路径 |
| windows_repo、linux_repo | 同一源码在两侧的绝对路径 |
| windows_session_root、linux_session_root | 同一会话在两侧的绝对路径 |
| operator_spec | 固定项目算子声明的 Windows 路径 |
| catalog_lock | 已批准 Catalog Lock 的 Windows 路径 |
| extension_bundles | 固定算子 bundle 列表，不含候选自身 |
| fixed_responses | 离线模式的一至三份 compute.py 源码；live 时为空列表 |
| runtime_options | RP 原生资源、分钟根等明确参数 |

其余请求路径也使用 Windows 路径。候选只替换复制目录中的 `compute.py`。研究包、来源归档、输入清单、Catalog、固定源码、扩展和预算在首轮冻结，同路径内容改变也拒绝恢复。Windows 绝对路径和反斜杠相对路径在 Linux 侧按请求映射。新声明使用新会话，历史 Result 保持只读。

研究包的自定义指标直接声明在 `spec/research.yaml` 的可选 `metric_definitions` 列表中，每项使用 `MetricDefinition.to_dict()` 格式。因子、模型和知识导出从研究包读取指标定义，并核对目标 schema、单位与方向。

## 执行、恢复与反馈

每个候选委托现有 Workspace 主链，执行完成后直接从 `research_pipeline.results.load_result_context()` 读取 Result。反馈保存 `command_status`、`execution_status`、`result_ref`、执行引用、结论层级和诊断。开发指标要求命令与执行成功，并从正式结果表按列、日期、可见时点、validation 阶段和预算投影；失败候选不携带金融指标。正式执行限制与输入结论上限随开发结果进入提案、反思和研究知识，保存在独立的 `limitations` 文字列表中。

候选使用持久顺序编号，编码尝试另行计数。重复失败文本仍消耗尝试。Windows worker 和同机轻量子进程复用同一 Workspace allocation；恢复通过 execution 引用接续。已有 Result 复用正式输出，缺报告时只补报告。Runtime 中断按 `inspect.recommended_action` 委托 `resume` 或 `retry-node`；wait/readmit 保留诊断。替代 execution 的显式采用见[联合研究恢复](docs/joint-research.md#正式执行与替代执行恢复)。

只有语法、支持范围错误和明确归属于候选算子的代码异常进入编码修复。资源、进程、准入或报告异常沿用原候选恢复，仍失败则停止付费调用。公式生成提示只接收接口、确认公式、候选代码和技术诊断类别。

## 实时生成与预算

离线预算固定为：

```json
{"outer_loops": 1, "coder_attempts": 3, "parallel": 1, "live_llm_calls": 0}
```

live 请求增加 `code_generation`，`fixed_responses=[]`。初始 stub 只包含两个接口函数的 `pass` 或 `raise NotImplementedError`；可用 `initial_source` 显式提供待修复代码，它先作为基准评价，占用编码尝试。

```json
{
  "code_generation": {
    "mode": "live",
    "model": "gpt-6.1-sol",
    "base_url": "https://your-provider.example/v1",
    "interface": "daily_value 与 rolling_value 的参数、返回字段及缺失规则",
    "formula": "人工确认的公式、窗口和可见日期规则",
    "initial_stub": "def daily_value(rows):\n    raise NotImplementedError\n\ndef rolling_value(rows):\n    raise NotImplementedError\n",
    "max_output_tokens_per_call": 8192
  },
  "budget": {"outer_loops": 1, "coder_attempts": 3, "parallel": 1, "live_llm_calls": 3, "max_output_tokens": 24576}
}
```

仓库外 `.env` 显式提供 `MODEL`、`API_KEY`、`BASE_URL`、`PROXY_URL`；跨系统客户端另提供 `WINDOWS_PYTHON`、`WINDOWS_REPO`，可选 `TIMEOUT_SECONDS`。模型身份与冻结请求一致，凭据不进入请求、环境变量或 RD 快照。

```bash
"$RD_AGENT_PYTHON" -m quantwitness_rdagent run --request "$RD_REQUEST" --model-env-file "$MODEL_ENV_FILE"
"$RD_AGENT_PYTHON" -m quantwitness_rdagent resume --request "$RD_REQUEST" --model-env-file "$MODEL_ENV_FILE"
```

每次模型调用先持久预约次数及最大输出。已完成响应可复用，reserved/failed 不自动再次付费，恢复不重置预算。公式候选只允许受支持的纯计算语法；不接收文件、网络、环境或数据库操作。上游配置和运行日志位于显式会话根。

## 材料确认与知识

`spec-extract`、`spec-review`、`spec-confirm`、`spec-render` 和 `request-build` 完成材料提取、人工确认及请求冻结。引文定位与来源一致，语义决定由人确认。公开流程见[公式复现指南](docs/formula-reproduction.md)。

`knowledge-export` 从已终止的开发会话导出正式 Result 指标、研究设计和技术失败记录。下一会话冻结知识索引，按资产、日期、口径和指标语义筛选。编码经验另保存失败代码及成功实现的来源、计划和正式封存源码，不携带金融标签或留出指标。操作见[编码经验](docs/coding-knowledge.md)。

## 检查与运行产物

开发期用例覆盖公式手算、完成 bar、历史特征、开发阶段、标签可见性、候选失败、预算和恢复。合成闭环与真实研究分别声明数据和模型预算；测试通过不代表投资绩效。

测试输出放在批准的仓库外根目录，使用 pytest 时显式指定 `--basetemp`。候选、bundles、日志、临时工作区、模型响应、Result 与报告均保留在调用者指定目录，不提交源码仓库。测试命令和安装验收见[安装指南](docs/installation.md)。
