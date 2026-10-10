# RD-Agent：让 AI 参与量化研究

这个可选集成让 AI 帮你提取论文定义、生成计算代码、提出因子或模型候选，再由 QuantWitness 正式执行与独立检查。它不直接接管数据库，也不以“代码运行了”代替“公式实现对了”。

## 从哪种研究开始

| 你现在有什么 | 可以做什么 | 指南 |
| --- | --- | --- |
| 一篇 PDF 和想复现的公式 | 提取指定页，人工确认，再生成和验证代码 | [公式复现](docs/formula-reproduction.md) |
| 想先体验，不调用付费模型 | 用教学 PDF、合成分钟行情和固定响应跑通流程 | [成交量集中度示例](examples/volume_concentration/README.md) |
| 已有研究包与参数候选 | 在固定开发范围内比较方案 | [研究循环](docs/research-campaign.md) |
| 想提出新因子 | 根据已验证开发指标提案和反思 | [因子研究](docs/factor-research.md) |
| 想改变模型结构 | 在支持的前馈结构范围内生成和评价 | [模型研究](docs/model-research.md) |
| 想同时探索因子和模型 | 共用开发范围与预算，保留候选来源 | [联合研究](docs/joint-research.md) |

## 一轮 AI 研究怎样流转

```text
问题与人工确认的约定
  → RD-Agent 提出或实现候选
  → QuantWitness 检查数据、执行研究
  → 独立验证 + Result
  → 只返回允许的开发反馈
  → 下一轮候选，或按预算停止
```

公式复现只根据实现正确性修复，不能为提高收益偷偷修改原定义。探索研究可以提出新候选，但最终 test/holdout 不进入开发反馈。

## 运行前要准备什么

- 完整公开源码、可读数据或已封存输入、研究包和对应的独立验证器。
- 核心 QuantWitness 运行环境，以及本集成的 Python 3.11+ 环境。当前调度采用固定版本的 Linux RD-Agent 与指定 RP 执行环境；Windows 可通过 WSL 使用，细节见下文。
- 实时生成时准备模型服务与明确预算。固定响应的教学流程不产生实时模型调用。

本包复用上游 LoopBase 与 CoSTEER，固定提交为 `484776c211e4fbbeef03e0ec00d6bbee7362a4f4`。普通核心命令不自动安装或加载 RD-Agent。许可和来源见[第三方说明](../../THIRD_PARTY_NOTICES.md)。

下面是安装、请求格式和运行约定。初次体验可以先照教学示例操作，需要调整接口时再阅读完整字段。

## 安装与入口

使用 I 盘独立 Linux Python 环境。将固定提交源码纳入 PYTHONPATH，并安装其本场景实际导入的依赖；上游普通 wheel 未打包子模块，因此使用固定源码路径。Windows RP环境无需安装RD-Agent。承担模型客户端调用的Windows环境需安装本包`live`可选依赖（`pip install -e "integrations/rdagent[live]"`），显式包含httpx与python-dotenv。

先准备 Python 3.12 虚拟环境和固定源码。`RD_AGENT_SOURCE` 必须指向新目录；`RD_AGENT_ENV`、`PIP_CACHE_DIR`、`RESEARCH_TEMP_ROOT` 应位于所选研究磁盘。

```bash
git clone https://github.com/microsoft/RD-Agent.git "$RD_AGENT_SOURCE"
git -C "$RD_AGENT_SOURCE" checkout --detach 484776c211e4fbbeef03e0ec00d6bbee7362a4f4
python3.12 -m venv "$RD_AGENT_ENV"
export RD_AGENT_PYTHON="$RD_AGENT_ENV/bin/python"
"$RD_AGENT_PYTHON" -m pip install -r "$QUANTWITNESS_REPO/integrations/rdagent/requirements-linux.txt"
"$RD_AGENT_PYTHON" -m pip install --no-deps "$RD_AGENT_SOURCE"
"$RD_AGENT_PYTHON" -m pip install --no-deps -e "$QUANTWITNESS_REPO/integrations/rdagent"
```

`requirements-linux.txt` 固定了本场景实际使用的直接依赖版本，不安装上游包含 Docker、网页和其他场景的完整 requirements。Ubuntu 若缺少 ensurepip，先安装发行版对应的 python3.12-venv，或用 virtualenv 在同一目标目录创建环境。运行测试另安装 `pytest==9.1.1`；WSL 挂载盘上的测试使用 `pytest -s`。

```bash
export PYTHONPATH="$RD_AGENT_SOURCE:$QUANTWITNESS_REPO/integrations/rdagent/src"
export TMPDIR="$RESEARCH_TEMP_ROOT"
export LITELLM_LOCAL_MODEL_COST_MAP=True
"$RD_AGENT_PYTHON" -m quantwitness_rdagent run --request "$RD_REQUEST"
"$RD_AGENT_PYTHON" -m quantwitness_rdagent inspect --request "$RD_REQUEST"
"$RD_AGENT_PYTHON" -m quantwitness_rdagent resume --request "$RD_REQUEST"
```

`RD_AGENT_SOURCE` 指向上述固定提交的源码目录，`QUANTWITNESS_REPO` 指向同时包含`src/`和`integrations/`的QuantWitness源码根（单仓库中为`research_pipeline/`子目录），`RD_AGENT_PYTHON` 指向独立 Linux 虚拟环境的 Python；`RESEARCH_TEMP_ROOT` 与 `RD_REQUEST` 分别指向显式临时目录和请求文件。Windows/Linux 两侧目录应位于同一共享磁盘。

## 冻结请求

顶层必须声明 `request_id`、`mode=formula_reproduction`、`base_package`、`source_archive_root`、`input_snapshot_manifest`、`editable_source_root`、`reference_bundle`、`development_scope`、`runtime_binding`、`budget`。来源和研究包是已有完整 RP 声明，不重新建立 ResearchSpec。

`development_scope` 必须声明 `role=development` 及本次范围。`budget` 固定为：

```json
{"outer_loops": 1, "coder_attempts": 3, "parallel": 1, "live_llm_calls": 0}
```

`runtime_binding` 必须完整声明：

| 字段 | 含义 |
| --- | --- |
| windows_python | Windows RP Python 的绝对路径 |
| windows_repo、linux_repo | 同一源码在两侧的绝对路径；支持独立QuantWitness根与包含research_pipeline子目录的单仓库根 |
| windows_session_root、linux_session_root | 同一个会话目录在两侧的绝对路径 |
| operator_spec | 固定项目算子声明的 Windows 路径 |
| catalog_lock | 已审批归档 Catalog Lock 的 Windows 路径 |
| extension_bundles | 固定上游算子 bundle 的 Windows 路径列表，不含候选自身 |
| fixed_responses | 一至三份 compute.py 源码文本 |
| runtime_options | RP 原生资源、分钟根等明确运行参数；不能改写数据库或运行目录 |

其余请求路径也使用 Windows 路径。可编辑源码目录包含固定 adapter 与初始 compute.py；候选只替换复制目录中的 compute.py。输入快照、归档正文、Verifier、评价范围和资源参数在首轮冻结，变更需要新会话。会话直接保存并比较四份包声明、固定 Python 源码、operator 声明、输入清单、外部原计划正文、Catalog 的 CURRENT 与当前版本 lock/source-manifest/audit/schema，以及已有 bundle 控制记录；同一路径内改变这些材料也会拒绝，不增加一套内容摘要。

外部 `original_plan` 路径相对输入清单所在目录解析；Windows 绝对路径和反斜杠相对路径在 Linux 侧映射后读取。内联原计划已由输入清单冻结。冻结只比较现有声明正文，不新增摘要。缺少这些新增冻结字段的历史 RD 会话需新建会话，不能补写旧会话的首次输入来续跑；历史 RP Result 保持可独立读取。

## 执行与恢复

候选使用持久顺序编号。相同文本和冻结请求重用同一编号；编码尝试另持久计数，重复失败文本仍消耗一次尝试；编码期评价和最终 Runner 共同调用 Windows worker。worker 用候选编号作为 Workspace allocation.label，恢复时先从 execution.json 重建索引；即使分配过程在更新索引前中断，也不重复分配。

Windows worker 为派生进程设置 Windows 格式的 PYTHONPATH、UTF-8 与会话内 scratch 临时目录。运行参数通过正式 `run` parser 解析后传给 Workspace，省略值沿用 CLI 默认值（包括 `resource_stale_seconds=30`），用户显式值保持有效；冻结来源和执行位置仍由请求及 Workspace 绑定。候选重入先复验已提交 bundle 并复用；编译完成后即使候选关联未落盘，也能从唯一 COMMITTED bundle 恢复。

所有阶段调用 RP 原生服务。已有 Result 时直接复验封存结果；缺 VerificationResult 时继续 verify；缺报告时只补 report。Runtime 中断依据公开 inspect 的 recommended_action 委托 resume/retry-node。Runtime 成功但 Result 尚未封存时仍由 resume 复用完成节点并封存；Result 已发布而引用未写成时，按 inspect 的正式发布位置复验 Result 并继续 verify/report。其他动作如 wait/readmit 保留诊断，不自动新建执行。未发布的 `.admitted.tmp` 只在确认属于当前 execution 后移入其 `interrupted-admission/`，重新准入仍沿用原 execution。

RD 恢复只读取最新成功阶段快照，`checkout=False`。CoSTEER 回退同步恢复源码候选与反馈引用。生成器只接收固定目标和技术反馈；收益指标不会作为公式改写奖励。

## 反馈与验证范围

反馈包含命令、执行、独立验证和公式验证四种状态。显式`formula_evaluation`声明决定公式Verifier身份和正式覆盖表；未声明时只接受首例的`dai-zhu-er-jiu-fixed-session-formula-check@2.0.0`或联合验证器`dai-zhu-er-jiu-formula-and-statistics-check@2.0.0/3.0.0`。公式pass须有已封存验证结果、正式验证通过及非空覆盖表；含`formula.*`发现时为fail，仅其他验证失败时为not_run。普通统计pass不提升为公式通过。coverage反映实际证券、session和分钟行数，不将局部覆盖称为全量复现。

默认技术验收使用合成输入完成真实 RP 执行与 VerificationResult，不能用伪造成功反馈替代主链。真实研究须单独批准来源、计算范围及资源预算，批准后可用同一入口运行归档输入；真实模型调用另按冻结live预算执行。固定adapter、分析或Verifier改变时须构建新bundle、重新准入并使用新冻结会话，历史execution和Result保持只读。


## 合成闭环验收

`tests/prepare_formula_loop.py` 调用项目合成 fixture，准备两个证券、25 个虚构 session、12,000 行分钟数据及冻结请求。固定响应先对公式值增加 0.5，再提供正确公式；错误候选必须完成真实执行并获得独立公式 fail，正确候选必须获得正式 Result、VerificationResult 和公式 pass。日期仅作为虚构 session，不代表真实交易日历。

在 Windows RP 环境将 `research_pipeline/src`、`research_pipeline/tests` 与本包 `tests` 加入 PYTHONPATH，然后调用 `prepare_formula_loop.prepare(合成根目录, 仓库目录, Windows Python路径)`；返回请求路径。Linux 环境将该请求的共享盘路径写入 `RD_FORMULA_TEST_REQUEST` 后执行：

```bash
"$RD_AGENT_PYTHON" -m pytest -s -q "$QUANTWITNESS_REPO/integrations/rdagent/tests/test_linux_formula_loop.py"
```

测试同时检查 RD 快照落后于 RP、缺验证输出和缺报告三种恢复情况：候选只对应一次 execution，编码预算不重置，Runtime 事件和 checkpoint 不重写。该测试只接受显式标记的合成请求，阶段模拟移动的小型输出保存在会话 `recovery-evidence/`。临时合成输入、bundle、工作区、日志与报告均留在所选合成根目录，便于复核。


Windows 的 `tests/test_finalize_recovery.py` 使用 `RD_FORMULA_WINDOWS_REQUEST` 指定同一合成请求，覆盖 Runtime 成功但未发布 Result、Result 已发布而引用未写成、准入临时目录尚未发布三个中断点。测试保留移动前材料，生成 `finalize-acceptance.json`、`published-reference-acceptance.json` 和 `admission-acceptance.json`；Linux 完整闭环生成 `acceptance.json`，包含候选失败到通过、覆盖范围、候选执行次数及 Runtime/Result 文件复用记录。


`tests/test_formula_feedback.py` 验证已复验上下文的反馈投影：只接收两个明确ID和版本，拒绝普通统计身份、缺结论身份、缺coverage及空coverage。该合同测试不替代真实Verifier执行。`test_request_and_recovery.py`覆盖Catalog各控制文件与外部原计划同路径变更的重入拒绝；`test_linux_freezing.py`只验证共享I盘的绝对、相对Windows路径映射，不加载RD模型依赖。


## live生成与诊断修复

固定请求增加`code_generation`，`runtime_binding.fixed_responses`设为`[]`。普通生成只提交接口、固定公式和未实现stub；`initial_stub`只允许两个接口函数的`pass`或`raise NotImplementedError`。模型不会收到已有正确实现、独立Verifier源码、行情、标签或收益指标。

```json
{
  "code_generation": {
    "mode": "live",
    "model": "gpt-6.1-sol",
    "base_url": "https://your-provider.example/v1",
    "interface": "daily_value与rolling_value的参数、返回字段及缺失规则",
    "formula": "固定公式、交易日窗口和可见日期规则",
    "initial_stub": "def daily_value(rows):\n    raise NotImplementedError\n\ndef rolling_value(rows):\n    raise NotImplementedError\n",
    "max_output_tokens_per_call": 8192
  },
  "budget": {
    "outer_loops": 1,
    "coder_attempts": 3,
    "parallel": 1,
    "live_llm_calls": 3,
    "max_output_tokens": 24576
  }
}
```

修复已有用户代码时，可在`code_generation.initial_source`提供待修复源码。该源码先作为`user_baseline`候选进入正式评价，占用一次编码尝试但不调用模型；仅在失败后将候选源码和技术诊断交给模型。原始模型响应始终原样留存，不替换为预设答案。最多三次编码尝试包含baseline，因此此模式最多再生成两次。

```bash
"$RD_AGENT_PYTHON" -m quantwitness_rdagent run --request /mnt/i/research/request.json --model-env-file /mnt/i/research-private/model.env
"$RD_AGENT_PYTHON" -m quantwitness_rdagent resume --request /mnt/i/research/request.json --model-env-file /mnt/i/research-private/model.env
```

.env放在仓库外，显式声明`MODEL`、`API_KEY`、`BASE_URL`、`PROXY_URL`。从Linux调用Windows本机代理时另提供`WINDOWS_PYTHON`和`WINDOWS_REPO`，可用`TIMEOUT_SECONDS`声明超时。调用前核对模型与接口地址是否匹配冻结请求；更换密钥不改变模型身份。客户端通过指定代理请求Responses接口，`trust_env=False`、`store=False`，不自动重试。模型名称、密钥、接口地址、代理均不写入进程环境变量；上游RD导入使用`LITELLM_LOCAL_MODEL_COST_MAP=True`读取随包价格表，日志、工作目录和签名路径则直接设置RD配置对象。

配置文件示例（直接保存为`.env`，不执行`export`或`setx`）：

```dotenv
MODEL=gpt-6.1-sol
API_KEY=填写私密密钥
BASE_URL=https://your-provider.example/v1
PROXY_URL=http://127.0.0.1:7890
WINDOWS_PYTHON=I:/research/envs/qlib/Scripts/python.exe
WINDOWS_REPO=I:/research/QuantWitness
TIMEOUT_SECONDS=180
```

每次调用前先写`model-calls/call-NNNN.json`预约调用次数和输出上限，保存提示、原始返回源码、模型身份和整数用量；.env路径与凭据不进入请求、模型记录或RD快照。预约使用上限扣减，不按实际用量退款，恢复不会重置预算。`completed`响应可以直接复用；`reserved`或`failed`表示调用状态不确定或已失败，恢复会停止该次付费，需先核查记录再决定是否另开获准会话。HTTP失败只保留脱敏错误码，如`model_http_status_429`。

Windows worker在构建bundle前检查生成代码：只允许`math`、`statistics`、`__future__`导入、两个公式接口与纯计算helper，拒绝文件/网络接口、动态执行、反射和双下划线属性访问。语法和支持范围错误进入候选技术反馈。该范围检查用于限定公式实现，不是通用Python沙箱。修复提示只投影执行状态、错误类型与`formula.*`/`code.*`错误码；完整诊断、路径、行情值和统计结果留在本地工件。

仅语法/支持范围错误、正式公式fail及明确归属于候选算子的代码异常触发模型修复。worker保留Runtime正式错误码，并通过inspect及已准入算子身份定位失败节点。资源采样、进程、准入、验证执行或报告异常均复用原候选恢复一次；仍失败即停止，不增加候选与模型调用。恢复建议要求重新准入或其他人工处理时，沿用正式诊断，不绕过门禁。

`tests/test_live_generation.py`用替身客户端验证预约、恢复、配置身份、技术反馈与支持语法；`tests/test_linux_live_generation.py`使用真实CoSTEER和测试语法评价器验证生成修复及候选中断恢复。这些测试不调用真实API、不读取研究数据库，也不替代正式RP公式验收。`test_failure_classification.py`验证正式诊断投影与可修复范围，Linux调度测试包含资源/verify/report失败时原候选恢复且不增加付费调用。

## 从确认规格创建请求

`request-build`把有效确认文件与显式模板组装为新请求，支持live和固定响应。它重验确认正文及目标研究包的来源快照，将完整公式和接口写入`confirmed_formula`；live同时使用相同的`code_generation.formula/interface`。模板若已包含不同公式或接口则拒绝，不静默覆盖。目标包可以与材料提取包位于不同目录，但须绑定同一来源快照和题录。

```powershell
python -m quantwitness_rdagent request-build --template I:/research/request-template.json --draft I:/research/paper-spec/draft.json --materials I:/research/paper-spec/materials.json --decisions I:/research/paper-spec/decisions.json --confirmation I:/research/paper-spec/confirmation.json --output I:/research/request.json
```

模板完整声明输入、两侧路径、独立Verifier、开发范围与本批预算；live模板只可省略由确认提供的`formula/interface`。命令不读取`.env`、不创建确认或会话、不调用模型、不执行研究，输出必须是新文件。构建请求不授予执行权限。

第二公式可在顶层显式声明：

```json
{
  "formula_evaluation": {
    "verifier_id": "volume-concentration-check",
    "verifier_version": "1.0.0",
    "coverage_schema_id": "project.volume-concentration.coverage.v1"
  }
}
```

该声明随请求冻结。实际VerificationResult必须绑定指定Verifier身份及其已封存结果，并包含声明的正式coverage表。覆盖表使用`entity`、`session`、`raw_rows`；`formula.*`验证发现对应公式失败，其他验证失败不冒充公式通过。未声明时仍只接受首例原有明确验证器。恢复不能修改公式或验证器绑定。

[成交量集中度公开示例](examples/volume_concentration/README.md)提供合成输入、项目adapter、独立公式Verifier与固定响应，所有路径由使用者显式提供，不依赖任务脚本。

## 实际验收覆盖

模型从人工确认的公式规格生成代码、依据正式失败反馈修复，已通过真实API合成验收。已保存的模型原始代码也通过“待著而救”冻结真实归档验收：八节点、联合Verifier3.0.0、正式Result与报告均通过，21类结果表与固定公式基线一致。

真实验收采用2525只证券、2013年1—4月评价；15150条月末记录包含预热和标签闭包。独立公式逐值参考覆盖两只固定证券、117个session及56160行分钟。该范围证明代码实现与研究执行的复现能力，结论仍为research_observation；任意论文的无人审核理解不属于已验收能力。开发区有限候选的多轮校准研究使用独立campaign入口，范围见下文。

## 研报材料到公式规格

`spec-*`命令在可选集成层运行，调用RP已有来源快照校验，不导入RD-Agent循环，不访问数据库。运行环境需安装RP、本包的`live`和`materials`可选依赖；`materials`固定`pypdf==6.10.0`，仅支持带可提取文本的PDF。页码为1基PDF物理页，行号对应本次抽取器输出，不是印刷页码或跨版本稳定编号。

```powershell
python -m quantwitness_rdagent spec-extract --package I:/research/package --source-archive-root I:/research/sources --source-id paper_source --pages 4 5 6 --output I:/research/paper-spec --model-env-file I:/private/rd-model.env --max-calls 2 --max-output-tokens 8192
python -m quantwitness_rdagent spec-review --draft I:/research/paper-spec/draft.json --materials I:/research/paper-spec/materials.json --output I:/research/paper-spec/review.md
```

`spec-extract`只向模型发送来源题录和指定页带行号的文本；保存原始返回、使用量、提取提示及出处。不传已有因子实现、独立Verifier、行情、标签或人工答案。规则分为原文明示`paper_explicit`、推断`inference`和项目决定`project_decision`，未明确的金融细则列为歧义。引文须与指定行逐字一致；定位检查不证明规则在语义上正确。提取结果始终为待人工确认。

同一输出目录冻结材料、抽取器、模型和预算。最多2次请求，单次输出最多8192 token，调用前保存预算预约；已完成响应可重复读取，不再请求；若draft.json已有人工修改则停止并保留正文，修订稿建议另存独立文件。网络失败或预约后中断不自动重试。结构或引文检查失败时，可在原命令上加`--repair`使用剩余一次预算修正；该选项不能解决金融歧义。不能通过新建输出目录绕过已批准的批次预算。

审阅者须核对规则是否忠实于原文、项目决定是否被误写成原文，并解决全部歧义。模型给出的建议没有批准效力。人工决策文件示例：

```json
{
  "accepted_rule_ids": ["r1"],
  "resolutions": [
    {"ambiguity_id": "a1", "decision": "明确选定的处理", "rationale": "选择依据"}
  ],
  "interface": "daily_value(rows), rolling_value(rows)",
  "review_notes": "对照原文的语义审阅结论"
}
```

规则和歧义ID必须覆盖当前草稿全部条目。确认命令仅由已明确认可这些具体内容的本地协作者执行；`--approve`不代表工具自动批准。

```powershell
python -m quantwitness_rdagent spec-confirm --draft I:/research/paper-spec/draft.json --materials I:/research/paper-spec/materials.json --decisions I:/research/paper-spec/decisions.json --confirmed-by reviewer --approve --output I:/research/paper-spec/confirmation.json
python -m quantwitness_rdagent spec-render --draft I:/research/paper-spec/draft.json --materials I:/research/paper-spec/materials.json --decisions I:/research/paper-spec/decisions.json --confirmation I:/research/paper-spec/confirmation.json --output I:/research/paper-spec/formula.txt
```

确认保存完整规格、材料和决定。渲染前重新校验来源快照并抽取原页，直接比较确认正文；来源、规格或决定发生变化须重新确认。使用`request-build`可将输出文本原样绑定到显式请求模板，不会自动生成代码、运行研究或改变信号可用时间。原手工填写formula路径保持可用，不具有本流程的材料审阅记录。

### 材料限制

空文本页阻断提取；图表、公式图像、文本阅读顺序须人工核对，不提供OCR或自动找齐整篇公式的能力。原文引文、人工推断和本地实现决定必须分开。论文发布时间与历史回测样本期分别记录，历史样本复现不代表当时已知该方法。快照保留文件版本身份；来源URL为占位值时，须在审阅材料中说明其未核实。

## 待著而救材料提取验收

已在归档研报第4—6页完成一次真实提取：9条规则（8条原文明示、1条推断）、13项歧义、26处引文定位通过。语义审阅确认固定20交易session、总体标准差和原值等权建议与既有项目一致；事件间隔使用自然时钟还是交易bar、跟随窗口是否跨午休需要采用明确项目决定。用户已确认13项项目决定，并通过spec-confirm/spec-render生成公式文本；事件间隔与窗口采用已冻结的交易bar网格，允许同日跨午休。确认文本已进入新一轮模型生成：首个候选通过完整合成执行、联合Verifier3.0.0和23项独立边界样例。本轮新增1次模型调用、10641token，覆盖3证券、129虚构session和61920行分钟；候选源码与原始响应一致。同一原始源码随后完成一次真实2013归档执行：8节点、联合Verifier3.0.0和21类表对照通过，15150条月末记录最大浮点差8.88e-16；真实回放新增API0次。材料到真实Result链路累计2次模型调用、23876token。独立原始分钟参考覆盖两只指定证券，不等同全市场逐条复算。单次执行收据的来源路径使用单独补证关联，原收据保留；结果文件的查看方法见[公式复现指南](docs/formula-reproduction.md#结果与恢复)。该验收证明指定文本章节经人工确认后的因子复现能力，不代表任意论文自主理解、自动改变研究方法或盈利证明。


## 有界多轮研究

`campaign-run`、`campaign-resume`与`campaign-inspect`支持已验证Qlib开发预测上的多轮校准比较。允许变化为冻结菜单内的原模型候选与向零收缩系数，RD-Agent负责提案、评价、反馈与记录调度。跨轮调用、输出、评价和轮数预算持久保存；最终holdout不用于提案或选优。该研究过程收据引用原Result，不产生新的正式Result。

使用方法见[研究循环说明](docs/research-campaign.md)，零费用入口见[预测校准示例](examples/prediction_campaign/README.md)。原`run/resume`仍负责已确认公式的代码生成和技术修复，两类任务使用各自冻结请求。


## 安装与参与贡献

核心与可选集成分别提供wheel/sdist。[安装验收指南](docs/installation.md)给出独立环境安装、公开公式验证、CI覆盖与可认领任务；运行产物保留在调用者指定目录。贡献规范见[项目贡献指南](../../CONTRIBUTING.md)。


## 多轮正式研究

`campaign-run/resume/inspect` 支持已有预测校准和新的ResearchPackage参数研究。研究包模式每轮形成独立Result与VerificationResult，反馈仅包含通过验证的开发指标；最终test/holdout留给单独冻结的最终研究。说明见[研究循环](docs/research-campaign.md)，无需私有数据的入口见[两轮正式研究例](examples/package_campaign/README.md)。

## 文档基线后的新因子研究

`factor-run/factor-resume/factor-inspect`提供基线复现、原生假设、表达式实验和研究反思循环。每轮执行正式研究包并独立验证；新候选不必预列菜单。当前独立验证范围为日频收盘动量、均价偏离、相对波动和区间位置，配置与教学例见[因子研究](docs/factor-research.md)。该模式不使用上游ChatSession缓存，模型身份与代理继续由显式.env提供。`knowledge-export`可从正式开发会话导出研究知识；新会话通过可选`knowledge`声明冻结索引，引用历史结果选择已支持的四类价格因子方向。因子与模型联合方向使用 `joint-run/joint-resume/joint-inspect`，复用同一开发范围及统一预算，模型特征绑定通过验证的因子；见[联合研究指南](docs/joint-research.md)。

## 编码经验复用

公式复现请求可声明 `coding_knowledge`，从已通过正式独立验证的会话读取实际错误与修复源码。默认精确匹配公式和接口；`retrieval_scope: technical_transfer` 允许相同接口下按共同计算结构迁移不同公式的经验。模型研究支持逐候选节点定义、当前编译失败和正式模型来源复用。提示保留来源身份与修复配对，排除完整诊断和 test/holdout 金融指标；恢复读取冻结记录。字段、预算和正式来源核验见[编码经验](docs/coding-knowledge.md)。

生成式前馈模型入口为 `model-run/model-resume/model-inspect`，通过上游模型假设、结构编译修复与反思驱动正式开发研究。范围与教学例见[生成式模型研究](docs/model-research.md)。

## 因子与模型联合研究

`joint-run/joint-resume/joint-inspect`统一方向选择、调用预算和研究反思。模型消费已验证因子中开发指标最好的表达式，并在正式设计及Result中保存特征绑定；两分支共用冻结输入、开发范围和指标。外部真实归档与已确认材料沿用各自请求接口。三轮合成准备例及恢复方法见[联合研究指南](docs/joint-research.md)。
