# 安装、构建与公开发布

使用框架和发布框架是两件事。
普通使用者按 README 安装并运行示例。
维护者使用本页的源码导出、构建和发行检查，把允许公开的文件交付给别人。

wheel 是可安装程序包，sdist 是用于构建的源码包，完整公开仓库还包括示例和文档。
不要把包含本地数据的开发仓库整体当作公开发行物。

## 使用本页前

- 普通研究使用者按[入门教程](workspace-quickstart.md)安装。
- 构建维护者准备干净源码、离线依赖和仓库外输出目录。
- 发布维护者准备已通过验收的候选提交及平台审批。

当前源码版本为 `1.6.0`。
协议、工件和数据规则各自保留独立版本。
软件版本升级不代表协议编号全部改变。

## 安装者与维护者

最低支持 Python 3.10。
版本、Python 范围、运行依赖、extras 和 `quantwitness` 入口只在 `pyproject.toml` 中声明，由标准 setuptools PEP 517 backend 生成 wheel 和 sdist。
展示品牌为 `QuantWitness`，PyPI 发行名和安装命令使用规范化小写 `quantwitness`，Python import 保留 `research_pipeline`。
`test`、`dev` 在 Python 3.10 下显式依赖 `tomli`，其他版本使用标准库 `tomllib`。

开发安装使用 `python -m pip install ".[dev]"`。
测试环境使用 `python -m pip install ".[test]"`。
两者同时准备`setuptools>=68`、`build`和`wheel`，满足无隔离发行测试的本机构建依赖。
这些 extras 不包括可选机器学习模型，按研究需求另装 `ml`（Qlib、XGBoost 与 Plotly）。

## 分发内容与命令适用范围

sdist和源码zip按显式清单附带公开文档。
清单包含根目录说明、扩展合同和示例说明。
源码 zip 另含核心测试及 RD 集成的执行恢复、隔离和错误分类专项。
RD sdist 附带同一集成的公开测试。

wheel 只安装运行所需的包代码和资源。
分钟参考资源分发当前规则 v6、会话 v6 和能力 v3。期货日频与结算可见性由本次项目冻结日历生成。
当前文件内部的历史日期、修订和可见时间继续保留。

本页命令从独立公开源码仓库根目录执行。
完整单仓使用者先进入 `research_pipeline/`。
发布工具、`tests/test_release_metadata_ssot.py` 及通用研究示例的可执行源码随独立公开源码仓库提供。
sdist 和源码 zip 包含 `integrations/lean/` 安装源码。可执行示例包含 `lean_quickstart`、`lean_futures`、`custom_metric` 和 `turnover_factor`。按各示例 README 安装依赖并运行。

wheel 只提供 Python 框架。
LEAN 安装流程要求 Windows x64 与 PowerShell 7，Linux 核心或模型验收不代表支持 Linux LEAN 安装。

文档由 `tools/release_allowlist.py` 的 `CORE_DOC_FILES` 与 `MANIFEST.in` 显式列入，不递归收录历史文档。
三类发行产物均排除个人研究项目和真实数据。
私有Catalog声明、Lock和内部整改基线也不分发。
需要 Catalog 的命令由调用方提供自己的持久 Lock。

公开仓库按 `public_source_paths()` 返回的文件集合同步，不是整个本机目录的镜像。
功能退役时同步检查测试调用、夹具、辅助模块、文档和分发条目。
公开仓库的受跟踪文件须与当前集合一致，包括移除已退出清单的文件。
历史版本可从Git历史查阅，用户研究Result及其恢复依赖不属于源码清理范围。

当前 Result 加载器使用 `research-result-v4`，计划与 checkpoint 绑定当前输入、配置、实现和资源身份。
升级后重新准入。
用户保存的输入、Result 和持久留出账本按其自身生命周期保留。

公开示例与可选集成说明随核心文档分发。
文档的相对链接须在发行物内可读。
这 4 个示例支持从源码包运行。其他示例脚本和可选集成代码使用完整公开源码。
当前操作文档与安装验收随源码分发。

恢复专项使用已安装的核心和 RD 发行，不访问数据库或模型/API。
安装核心测试依赖和 RD 发行后，从公开源码根或源码 zip 解压根运行：

```powershell
python -m pytest -s -q -o pythonpath= integrations/rdagent/tests/test_package_campaign_execution.py integrations/rdagent/tests/test_package_execution_isolation.py integrations/rdagent/tests/test_failure_classification.py
```

从 RD sdist 解压根运行同一专项：

```powershell
python -m pytest -s -q -o pythonpath= tests/test_package_campaign_execution.py tests/test_package_execution_isolation.py tests/test_failure_classification.py
```

## 正式构建

先在独立环境准备 `pyproject.toml` 的构建依赖以及 `build`。
离线机器需提前准备当前平台和 Python 版本兼容的 wheelhouse。
构建本身不联网安装依赖。

```powershell
python -m pip install --no-index --find-links <wheelhouse目录> "setuptools>=68" wheel build
python tools/build_release_artifacts.py --project . --output <不存在的仓库外输出目录>
```

工具先按 allowlist 复制源码到临时 staging，排除单仓的私有 Catalog 声明与默认 Lock。
再调用标准 backend，检查 wheel/sdist 的库存和元数据，最后生成带核心测试的源码 zip。
三类产物通过检查后才发布到输出目录。
METADATA、WHEEL 和 RECORD 由标准构建后端生成。

人工发布只使用本次命令 JSON 回执中的 `wheel`、`sdist` 和 `source_archive` 绝对路径，并记录候选提交及库存验收结果。
确认新目录恰好包含这三件文件。
混入第二个 wheel 禁止发布。
历史 `dist/` 文件不属于本次候选，不能按通配符选择，更不能以旧文件代替当前源码构建。

干净 wheel 验收创建不继承系统包的 venv。
调用方须显式提供离线 wheelhouse，包含 `release/dependency-distributions.json` 中锁定版本的 Windows/Python 3.10 wheel 及其传递依赖。
脚本先安装这些依赖，再安装本次 wheel，执行 `pip check` 和锁定版本、字节核验。
依赖缺失或不匹配会失败，不能由系统环境补齐。

还需显式提供一个持久 Catalog Lock，供安装后资源读取测试使用。
它不是发行包资源。
验收还检查公共 Recipe 为空、`package init` 可用。
新建中性草稿的 `lint` 必须返回 exit=1，且 JSON 中 `status=fail`、`error_code=research_package_invalid`、`data.execution_ready=false`，聚合 `data.issues` 明确指出 `sources/sources.yaml` 的 `sources` 为空。
意外成功、其他错误或无效 JSON 都使验收失败：

```powershell
./scripts/verify_clean_wheel.ps1 -Wheel <wheel路径> -ReceiptOut <仓库外新收据路径> -CatalogLock <持久Catalog-Lock目录> -Wheelhouse <离线wheelhouse目录>
```

标准构建也排除私有Catalog和默认Lock。
正式交付仍使用上述staging工具执行完整清单检查。
正式发布候选要求工作树干净，并绑定本次构建输入。
BuildManifest 的源码检查支持不含私有 Catalog 的独立公开仓库，并继续拒绝未提交的包文件删除或修改。

收据复验器从显式传入的 wheel METADATA 读取版本，与安装后的 CLI 版本精确比较。
中性草稿 lint 记录必须为预期失败且 exit=1。

完整交付还需按[独立使用验收](external-acceptance.md)保留同版本 Linux、远端 CI 和首次使用记录。

## 最低版本验收

独立公开源码清单中的 `tests/test_release_metadata_ssot.py` 比较 pyproject、wheel、sdist、安装后元数据的名称、版本、Python 范围、依赖、extras 和 console script。
使用最低Python版本创建3个隔离虚拟环境。
分别安装wheel、sdist和源码zip。
检查命令行、依赖完整性和核心功能。

这组检查不访问数据库。

该验收只证明安装与发布元数据合同，不代表通过真实研究、全部发布门禁或能力晋级。

```powershell
$env:RP_MINIMUM_PYTHON = "<Python 3.10 的 python.exe>"
$env:RP_RELEASE_BUILD_PYTHON = "<已准备构建依赖的 python.exe>"
$env:RP_RELEASE_WHEELHOUSE = "<包含运行、test、dev 和 build 依赖的 wheelhouse目录>"
python -m pytest tests/test_release_metadata_ssot.py -q
```

测试解释器也需安装 test extras。
缺少最低版本解释器或离线依赖时验收失败，不能跳过。
干净环境安装 pyproject 所允许的新版本不等于满足当前发布锁。

## Qlib 与 RD-Agent 环境版本

核心 `ml`、`ml-sequence` 与可选 RD-Agent 分别安装，保持各自支持的 Python 范围。

| 组件 | 当前研究基准 | 声明位置与安装方式 |
| --- | --- | --- |
| Qlib | `pyqlib==0.9.7` | 核心 `pyproject.toml` 的 `ml` 和 `ml-sequence`。`pip install ".[ml]"` |
| Torch | `2.9.1`，设备按研究声明冻结 | 核心 `ml-sequence`。生成模型同时安装 `ml` 与 `ml-sequence` |
| RD-Agent | 提交 `484776c211e4fbbeef03e0ec00d6bbee7362a4f4` | 可选集成的 `UPSTREAM_COMMIT`。固定源码与 `requirements-linux.txt`，研究循环使用 Linux Python 3.12 |

核心最低 Python 为 3.10，可选 `quantwitness-rdagent` 最低 Python 为 3.11。
上游 RD-Agent 普通 wheel 未包含本场景需要的子模块，研究循环继续从上述固定提交的源码导入。
核心和本地可选集成使用当前发行包。
安装命令、wheel/sdist 公开公式验收见[可选集成安装说明](../integrations/rdagent/docs/installation.md)。

升级先确定一个目标版本或提交，并说明实际受影响的因子、Processor、模型、求解器及研究调度调用点。
同步修改依赖声明、固定上游提交、场景直接依赖和公开文档，在新隔离环境中安装。
保留原环境和历史 Result 的身份。
升级后的研究使用新冻结请求，不跨依赖身份恢复或复用旧模型节点。

按调用点验证升级：Qlib 因子复核数值、历史窗口及未来输入扰动。
模型复核处理器拟合范围、成熟标签、训练与跨进程预测恢复。
组合复核求解状态、约束、目标与实际成交。
RD-Agent 复核提案、编码、反思、知识引用、预算与中断恢复。

新增联合方向还须在同一会话实际执行因子和模型分支，只消费成功Result中的开发反馈。
已有未受影响的正式结果继续作为历史证据，不替代升级后发生变化的行为验收。

源码冻结后，从同一公开清单构建核心 wheel、sdist、源码包和可选集成发行文件。
在仓库外空环境确认导入来自安装目录、`pip check` 通过及新增入口可用，再运行受影响的公开回归。
提交和推送获准后，补齐该提交对应的远端 CI。
本地安装或旧提交 CI 不能代替这一记录。

## 依赖分发身份

当前依赖锁使用 `research-dependency-distribution-lock-v2`。
身份绑定分发名称、版本、METADATA和入口声明。
身份也绑定排序后的分发内容RECORD。
内容包括源码、二进制扩展和包数据。

分发自带且有摘要的字节码也参与身份。
它不绑定安装器生成的 INSTALLER、REQUESTED、direct_url.json、自身 RECORD 条目和没有摘要/大小的安装字节码。

入口包装器须同时满足2项条件才排除身份。
它属于console_scripts/gui_scripts的已声明入口。
它还须安装在当前解释器的scripts目录。
包内同名程序、未声明的脚本和其他文件仍参与身份。

这样同一分发包安装到不同路径时身份保持一致，分发内容记录或入口声明变化仍改变身份。
该检查使用安装分发清单，不逐次重新扫描所有依赖文件。
手工改动已安装文件而不更新其分发记录，不属于依赖锁能独立发现的变化。

依赖锁由当前安装的可信分发包建立。
重新安装或改变依赖后，生成当前锁并执行干净 wheel 脚本和收据复验。
恢复与跨运行复用边界见 [Runtime](runtime.md)。

## 公开仓库 CI

`tests/test_workspace.py`、`tests/test_cli_failure_details.py` 及依赖身份、恢复边界回归随公开源码分发。
公开CI在源码环境和隔离wheel环境运行用例。
覆盖工作区布局、运行与恢复、参数转发。
还检查无效组合拒绝和Result交付。

模型安装验收覆盖 Windows 与 Linux。
组合执行在 Windows 上显式安装 LEAN 后执行。
RD-Agent 发行安装验收检查失败候选的运行诊断，以及成功 Result 指标与手算值的一致性。
各项是否通过以候选提交的实际 CI 记录为准。

公开CI的平台与版本范围以该候选实际工作流为准。
项目Worker的进程额度覆盖Supervisor、启动器与后代，Windows虚拟环境按实际进程树声明，见[资源预算](project_resource_budgets.md)。

公开 `ci` 使用Python 3.10和3.13矩阵。
检查源码清单、框架边界和内建算子身份。
检查未经批准的晋级。
`sealed`声明须具备当前候选的独立发布证据。

每个矩阵环境按正式允许清单构建wheel。
在隔离环境安装，并从仓库外检查导入位置。
临时合成DuckDB用于4个示例的完整流程。
这些验收示例额外执行lint诊断。

完整整改基线依赖父仓库材料，不进入公开候选。
公开 CI 的能力状态限制以独立仓库可执行的门禁为准。

在独立Git仓库根运行公开源码清单检查。

```powershell
python tools/public_source_inventory.py --project . --check
```

工具比较已跟踪文件与公开允许清单。
额外文件或遗漏文件都会使检查失败。
`.github/CODEOWNERS`也须纳入Git。
父仓库内的 `research_pipeline/` 仍用该工具的导出命令生成独立候选，不以嵌套目录的 `--check` 代替公开仓库检查。

四个示例的项目 Worker 固定依赖 `pyarrow 21.0.0`。
CI 的源码测试环境和隔离 wheel 环境均安装此版本、执行 `pip check`，再核对隔离环境的版本和 wheel 来源。
该固定版本只约束示例验收，不收窄 `pyproject.toml` 面向用户的依赖范围。
CI 的 wheel 路径来自本次构建回执，不扫描历史 `dist/`。

工作流检查通过不代表 GitHub ruleset 或 required checks 已生效。
首次公开前需在平台按批准的治理方案配置并验证。
4个项目的完整流程只证明合成研究。
真实市场表现、费用和保证金需要各自验证。

## PyPI 正式发布

`.github/workflows/publish.yml` 在 `main` 推送的 `ci` 工作流通过后检查版本请求。
CI 记录本次推送前后的提交，发布工作流按这两个提交中的 `pyproject.toml` 比较版本：版本未变直接跳过。
递增的 `X.Y.Z` 正式版本才进入构建。
一次推送包含多个提交时，比较的是整次推送前后的版本，不是最后一个提交的父节点。

预发布版本、版本回退及低于已有正式 Tag 的版本不进入发布。

已有 `v<版本>` Tag 的版本跳过，不重复构建或上传。
普通代码、文档、依赖调整只要版本未变，就不会发布 Release 或 PyPI。
手工创建 Release 或推送 Tag 也不触发上传。
构建固定使用通过 CI 的候选提交，而不是工作流启动时最新的 `main`。

工作流重新检查公开源码、发布合同和隔离 wheel，只使用本次正式构建 JSON 回执中的 wheel 与 sdist。
源码 zip 作为 GitHub Release 附件保留，不上传 PyPI。

PyPI 使用 Trusted Publishing。
PyPI 项目绑定 GitHub owner `ljjtim`、仓库 `QuantWitness`、工作流 `publish.yml` 和 Environment `pypi`。
GitHub 的 `pypi` Environment 负责正式上传前的审批。
发布任务只授予 `contents: read` 和 `id-token: write`，仓库不保存 PyPI API Token。

上传失败禁止使用 `skip-existing` 绕过。
已经发布的版本不能用不同文件覆盖，修复后发布新版本。

正式发布按版本 PR 和平台工作流完成。
本机无需配置 PyPI Token。

1. 提高 `pyproject.toml` 的 `project.version`。
2. 在 `CHANGELOG.md` 中填写对应版本章节。
3. 提交版本 PR，完成候选验收。
4. 将通过检查的 PR 合并到 `main`。
5. 在「Actions → publish」查看版本判定和构建结果。
6. 由指定审批人在「Review deployments」批准上传。

例如，版本从 `1.5.0` 提高为 `1.6.0`。
变更记录包含非空的 `## 1.6.0` 章节。
日期允许紧随版本号，格式沿用现有记录。
PR 检查本身不会发布。
合并后的 main CI 通过后，工作流才生成发布请求。

上传后，工作流在 Python 3.10 隔离环境安装产物。
检查包括版本、导入位置、依赖和命令入口。
安装核验通过后，工作流创建 Tag 和 Release。
Tag 名为 `v<版本>`，Release 名为 `QuantWitness <版本>`。
发布说明取自 CHANGELOG 对应章节。
附件来自同一次构建。

以 `publish` 工作流的版本判定、构建、PyPI 上传和 GitHub Release 四个任务全部通过为新版本发布完成标准。
版本未变时，后面三个任务跳过是正常结果。
发布队列串行运行，批准上传前保持一次仅有一个待发布版本。

PyPI 上传失败时 Release 任务不会执行。
安装核验或 Release 创建失败时，PyPI 版本可能已经存在。
查看失败任务后，在原工作流中重跑失败任务。
已经成功的上传任务保持原状。

普通提交不重新发布同一版本。
工作流不覆盖已有 Tag、Release 或 PyPI 版本。
临时环境问题可在原 CI 或发布工作流中重跑相应失败任务。
源码需要修复时，用更高版本提交新的版本 PR。

运行工件保留 7 天，依赖原工件的重跑应在保留期内完成。


## 生成本地发布验收证据

ReleaseEnvelope 汇总同一干净候选的六类验收，`profile=local` 只说明该次本地平台、依赖和输入范围。
它不授予全部能力 `sealed` 状态，也不表示已在 GitHub 或包索引发布。
四个公开示例的证据范围为合成工程验收，不是历史真实市场收益或完整交易执行验收。

| Gate | 当前验收内容 |
| --- | --- |
| A | 安装态股票研究、checkpoint 后中断恢复、恢复结果与正常运行一致、篡改结果被拒绝 |
| C | 四个研究、至少三类 DAG。正式Result、显式数据窗口、算法开发用例与生产时点约束 |
| D | checkpoint 提交和恢复、内容损坏拒绝、金融状态恢复不重复成交与费用 |
| F | 合成输入下股票/ETF/期货、资金精度、交易语义与 DSR 数学合同 |
| I/B | 确定性公共 CLI 发现、草稿诊断和四个研究完整执行，不作为真人或陌生 AI 试用证明 |
| L | Result直接消费、项目身份隔离、必需表及结果身份不符拒绝 |

先准备当前构建产物、BuildManifest和隔离安装环境。
3个工具的 `--help` 提供完整参数：

```powershell
python tools/run_release_workflows.py --help
python tools/build_gate_c_evidence.py --help
python tools/run_release_gate_tests.py --help
```
所有验收输出使用不存在的仓库外目录。
工作流生产器会在该目录创建合成数据库，执行前取得所在环境要求的写入批准。
运行与验证阶段只读访问这些数据库。

Gate D/F/L 使用候选内的 `release/gate-test-protocol.json`，测试源码和协议均进入 BuildManifest。
执行时关闭源码导入配置，从仓库外使用安装态 runtime。
有失败、跳过或零测试均不生成通过收据。
Gate L 的前置 Gate C 必须属于相同候选，不能沿用其他批次的通过记录。

Gate C 先通过正式 Recipe codec 复验完整定义与 `recipe_hash`，再按正式计划的 `validated-recipe-ref-v1` 身份投影核对 `plan_hash`。
日期或 Recipe 内容变动仍须与 Result的原计划绑定一致。

`tools/build_release_envelope.py` 接受六个 `--gate-evidence gate-id=path`，校验候选、BuildManifest 和 Gate 类型，并把原始证据、能力清单、依赖锁与 BuildManifest 一起封存。
能力清单和依赖锁必须与构建输入一致。
工具中的 `verify_release_envelope_files` 可在搬移目录后复验封套及封存内容。
执行日志、测试报告和研究工件另随验收目录保留。

## 能力声明与晋级

当前能力状态由 `src/research_pipeline/capabilities.json` 声明。
公开门禁允许已有 `local_only` 能力。
`sealed`声明须提供授权能力基线与当前发布封套。
还须提供正式执行、算法错误样本和独立审查记录。

证据须覆盖能力承诺的全部范围。
4个合成示例不足以证明全部交易和资源行为。

独立公开仓库可通过 `QUANTWITNESS_CAPABILITY_BASELINE`、`QUANTWITNESS_RELEASE_EVIDENCE_ROOT` 和 `QUANTWITNESS_RELEASE_CANDIDATE_ID` 显式提供晋级材料。
没有这些材料时，`sealed` 声明会失败。
材料存在但候选、源码、测试结果或授权上限不匹配时同样失败。
历史证据保留原身份，不自动迁移。

## 分钟来源清单的公开分发

分钟规则继续使用已发布的能力清单和规则身份。
含本机路径、查询语句及文件时间戳的 `minute_inventory.v3.json` 是内部验收原件，不进入公开源码、wheel、sdist 或源码包。
公开包提供 `minute_inventory.v3.public.json`。
文件保留观察窗口、行数、缺失值和原始清单身份。

其内容按已发布能力登记核验。

如果原件存在，加载器完整核验原件。
读取失败或摘要不符时，加载器返回错误。
原件不存在时，只有已登记的能力身份才能读取对应公开摘要，并核对摘要内容及原始证据身份。
缺失、篡改或未登记版本均拒绝。

公开摘要用于规则来源追溯，不包含行情数据，也不代替使用者自己的只读数据源与PIT准入。
来源清单、交易日历和规则正文使用当前发布身份。

来源清单按原始字节保存，Git检出和归档不转换换行。
当前默认分钟规则v6随wheel分发，安装态开发验收检查规则资源与发布身份。
