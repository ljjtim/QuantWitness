# 安装与发布构建

最低支持 Python 3.10。版本、Python 范围、运行依赖、extras 和 `quantwitness` 入口只在 `pyproject.toml` 中声明，由标准 setuptools PEP 517 backend 生成 wheel 和 sdist。展示品牌为 `QuantWitness`，PyPI 发行名和安装命令使用规范化小写 `quantwitness`，Python import 保留 `research_pipeline`。`test`、`dev` 在 Python 3.10 下显式依赖 `tomli`，其他版本使用标准库 `tomllib`。

开发安装使用 `python -m pip install ".[dev]"`；测试环境使用 `python -m pip install ".[test]"`。
两者同时准备`setuptools>=68`、`build`和`wheel`，满足无隔离发行测试的本机构建依赖。
这些 extras 不包括可选机器学习模型，按研究需求另装 `ml`（Qlib、XGBoost 与 Plotly）。

## 分发内容与命令适用范围

sdist 和源码 zip 均按显式清单附带当前公开操作文档、文档链接所需的根目录说明、项目扩展合同及示例说明。源码 zip 另含核心测试；wheel 只安装运行所需的包代码和资源。

本文的发布构建、最低版本验收、干净 wheel 验收及公开 CI 命令从独立公开源码仓库的根目录执行，也可在完整单仓的 `research_pipeline/` 下执行发布构建和验收。发布工具、`tests/test_release_metadata_ssot.py` 及可执行合成示例随独立公开源码仓库提供，不包含在 sdist 和源码 zip 中；压缩包中的示例说明用于查阅，执行示例须使用独立公开源码仓库。

文档由 `tools/release_allowlist.py` 的 `CORE_DOC_FILES` 与 `MANIFEST.in` 显式列入，不递归收录历史文档。个人研究项目源码、真实研究数据、私有 Catalog 声明和 Lock、内部整改基线及维护脚本均不进入三类发行产物。需要 Catalog 的命令由调用方提供自己的持久 Lock。

公开示例与可选集成的使用说明随核心文档一起分发，保持本地文档链接可读；对应示例脚本和可选集成代码仍需完整公开源码。日频现金节点的本地准入说明单独列入文档清单，内部发布证据不随包分发。

## 正式构建

先在独立环境准备 `pyproject.toml` 的构建依赖以及 `build`。离线机器需提前准备当前平台和 Python 版本兼容的 wheelhouse；构建本身不联网安装依赖。

```powershell
python -m pip install --no-index --find-links <wheelhouse目录> "setuptools>=68" wheel build
python tools/build_release_artifacts.py --project . --output <不存在的仓库外输出目录>
```

工具先按 allowlist 复制源码到临时 staging，排除单仓的私有 Catalog 声明与默认 Lock；再调用标准 backend，检查 wheel/sdist 的库存和元数据，最后生成带核心测试的源码 zip。三类产物通过检查后才发布到输出目录。脚本不再生成 METADATA、WHEEL 或 RECORD，也没有 `setup.py` 回退入口。

人工发布只使用本次命令 JSON 回执中的 `wheel`、`sdist` 和 `source_archive` 绝对路径，并记录候选提交及库存验收结果。确认新目录恰好包含这三件文件；混入第二个 wheel 不得发布。历史 `dist/` 文件不属于本次候选，不能按通配符选择，更不能以旧文件代替当前源码构建。

干净 wheel 验收创建不继承系统包的 venv。调用方须显式提供离线 wheelhouse，包含 `release/dependency-distributions.json` 中锁定版本的 Windows/Python 3.10 wheel 及其传递依赖；脚本先安装这些依赖，再安装本次 wheel，执行 `pip check` 和锁定版本、字节核验。依赖缺失或不匹配会失败，不能由系统环境补齐。还需显式提供一个持久 Catalog Lock，供安装后资源读取测试使用；它不是发行包资源。验收还检查公共 Recipe 为空、`package init` 可用；新建中性草稿的 `lint` 必须返回 exit=1，且 JSON 中 `status=fail`、`error_code=research_package_invalid` 并明确指出 sources 为空。意外成功、其他错误或无效 JSON 都使验收失败：

```powershell
./scripts/verify_clean_wheel.ps1 -Wheel <wheel路径> -ReceiptOut <仓库外新收据路径> -CatalogLock <持久Catalog-Lock目录> -Wheelhouse <离线wheelhouse目录>
```

直接在完整工作树运行标准构建不承担正式 allowlist 筛选；正式交付使用上面的 staging 工具。历史 release 证据不随当前构建覆盖，工作树未提交时也不能把测试构建称为干净发布候选。BuildManifest 的源码检查支持不含私有 Catalog 的独立公开仓库，并继续拒绝未提交的包文件删除或修改。收据复验器从显式传入的 wheel METADATA 读取版本，与安装后的 CLI 版本精确比较；中性草稿 lint 记录必须为预期失败且 exit=1。

完整交付还需按[独立使用验收](external-acceptance.md)保留同版本 Linux、远端 CI 和首次使用记录。

## 最低版本验收

独立公开源码清单中的 `tests/test_release_metadata_ssot.py` 比较 pyproject、wheel、sdist、安装后元数据的名称、版本、Python 范围、依赖、extras 和 console script。使用最低版本创建三个不继承系统包的虚拟环境，分别安装 wheel、sdist 和源码 zip，并运行 CLI、依赖完整性检查及不访问数据库的核心 smoke。

该验收只证明安装与发布元数据合同，不代表通过真实研究、全部发布门禁或能力晋级。

```powershell
$env:RP_MINIMUM_PYTHON = "<Python 3.10 的 python.exe>"
$env:RP_RELEASE_BUILD_PYTHON = "<已准备构建依赖的 python.exe>"
$env:RP_RELEASE_WHEELHOUSE = "<包含运行、test、dev 和 build 依赖的 wheelhouse目录>"
python -m pytest tests/test_release_metadata_ssot.py -q
```

测试解释器也需安装 test extras。缺少最低版本解释器或离线依赖时验收失败，不能跳过。干净环境安装 pyproject 所允许的新版本不等于满足当前发布锁。

## 依赖分发身份

当前依赖锁使用 `research-dependency-distribution-lock-v2`。身份绑定 distribution 名称、版本、METADATA、入口声明及排序后的分发内容 RECORD，包含源码、二进制扩展、包数据与分发自带的有摘要字节码。它不绑定安装器生成的 INSTALLER、REQUESTED、direct_url.json、自身 RECORD 条目和没有摘要/大小的安装字节码。

入口包装器只有同时满足两个条件才不进入身份：属于 console_scripts/gui_scripts 的已声明入口，且安装在当前解释器的 scripts 目录。包内同名程序、未声明的脚本和其他文件仍参与身份。这样同一分发包安装到不同路径时身份保持一致，分发内容记录或入口声明变化仍改变身份。该检查使用安装分发清单，不逐次重新扫描所有依赖文件；手工改动已安装文件而不更新其分发记录，不属于依赖锁能独立发现的变化。

v1 的原始 METADATA/RECORD 摘要与 v2 不兼容；验证器拒绝 v1 锁，不能只改合同版本字段或把旧摘要填进新锁。升级时按可信的同版本分发包重新安装并建立 v2 锁，再执行干净 wheel 脚本和收据复验。旧 BuildManifest、Result 和 VerificationResult 保留原字节，恢复与跨运行复用边界见 [Runtime](runtime.md)。

## 公开仓库 CI

`tests/test_workspace.py`、`tests/test_cli_failure_details.py` 及依赖身份、恢复边界回归随公开源码分发。公开 CI 在源码环境及隔离 wheel 环境均执行两者，覆盖 Workspace 布局、运行与恢复委托、复用参数转发、无效组合拒绝和 Verifier 进程槽参数转发。

公开 CI 运行于 Ubuntu。Windows venv 的 Verifier 启动器可能增加进程层级，独立验证可显式传入 `--verification-process-slots 3`；省略时默认仍为 2，详见 [Workspace 合成研究入门](workspace-quickstart.md) 和[资源预算](project_resource_budgets.md)。

公开 `ci` 保留 Python 3.10 与 3.13 矩阵，检查公开源码清单、框架边界、公共 Operator 的内建身份和未经批准的晋级，以及 `sealed` 声明必须具备当前候选的独立发布证据。每个矩阵环境都从正式 allowlist 构建 wheel，在独立虚拟环境安装，从仓库外检查导入位置，并在临时合成 DuckDB 上运行四个示例的 `lint → admit → run → Result → verify → VerificationResult → report` 闭环。完整整改基线依赖父仓库材料，不进入公开候选；公开 CI 的能力状态限制以独立仓库可执行的门禁为准。

公开仓库中的 `python tools/public_source_inventory.py --project . --check` 要在独立 Git 仓库根运行：它比较 Git 已跟踪文件与公开允许清单，拒绝额外提交或未纳入 Git 的允许文件，并要求正式治理文件 `.github/CODEOWNERS` 已跟踪。父仓库内的 `research_pipeline/` 仍用该工具的导出命令生成独立候选，不以嵌套目录的 `--check` 代替公开仓库检查。

四个示例的项目 Worker 固定依赖 `pyarrow 21.0.0`；CI 的源码测试环境和隔离 wheel 环境均安装此版本、执行 `pip check`，再核对隔离环境的版本和 wheel 来源。该固定版本只约束示例验收，不收窄 `pyproject.toml` 面向用户的依赖范围。CI 的 wheel 路径来自本次构建回执，不扫描历史 `dist/`。

工作流检查通过不代表 GitHub ruleset 或 required checks 已生效；首次公开前需在平台按批准的治理方案配置并验证。四项目闭环只证明合成研究，不证明交易费用、保证金或真实市场表现。

## PyPI 正式发布

`.github/workflows/publish.yml` 在 `main` 推送的 `ci` 工作流通过后检查版本请求。CI 记录本次推送前后的提交，发布工作流按这两个提交中的 `pyproject.toml` 比较版本：版本未变直接跳过；递增的 `X.Y.Z` 正式版本才进入构建。一次推送包含多个提交时，比较的是整次推送前后的版本，不是最后一个提交的父节点。预发布版本、版本回退及低于已有正式 Tag 的版本不进入发布。

已有 `v<版本>` Tag 的版本跳过，不重复构建或上传。普通代码、文档、依赖调整只要版本未变，就不会发布 Release 或 PyPI；手工创建 Release 或推送 Tag 也不触发上传。构建固定使用通过 CI 的候选提交，而不是工作流启动时最新的 `main`。工作流重新检查公开源码、发布合同和隔离 wheel，只使用本次正式构建 JSON 回执中的 wheel 与 sdist；源码 zip 作为 GitHub Release 附件保留，不上传 PyPI。

PyPI 使用 Trusted Publishing。PyPI 项目绑定 GitHub owner `ljjtim`、仓库 `QuantWitness`、工作流 `publish.yml` 和 Environment `pypi`；GitHub 的 `pypi` Environment 负责正式上传前的审批。发布任务只授予 `contents: read` 和 `id-token: write`，仓库不保存 PyPI API Token。上传失败不得使用 `skip-existing` 绕过；已经发布的版本不能用不同文件覆盖，修复后发布新版本。

发布不需要在本机切换 GitHub 账号或配置 PyPI Token，也不需要手工创建 Tag 或 Release：

1. 在版本 PR 中提高 `pyproject.toml` 的 `project.version`，例如从 `1.1.0` 改为 `1.1.1`，并在 `CHANGELOG.md` 写入非空的 `## 1.1.1` 章节。日期可紧随版本号，写法沿用现有变更记录。
2. 合并到 `main`，记录候选提交及验收结果；受改动影响的发布证据需重新生成并复验。PR 检查本身不发布，合并后的 main CI 通过才生成发布请求。
3. 在 **Actions → publish** 查看版本判定和构建结果。构建通过后，由 `pypi` Environment 的指定审批人在 **Review deployments** 中批准上传。
4. 工作流上传 PyPI，在全新 Python 3.10 环境从正式 PyPI 安装该版本并检查版本、导入位置、依赖和 CLI。
5. 安装核验通过后，工作流在已验收的候选提交上自动创建 `v<版本>` Tag 与 `QuantWitness <版本>` Release，发布说明只取 CHANGELOG 的对应章节，附上同一次构建的 wheel、sdist 和源码 zip。

以 `publish` 工作流的版本判定、构建、PyPI 上传和 GitHub Release 四个任务全部通过为新版本发布完成标准；版本未变时，后面三个任务跳过是正常结果。发布队列串行运行，批准上传前保持一次仅有一个待发布版本。

PyPI 上传失败时 Release 任务不会执行；安装核验或 Release 创建失败时，PyPI 版本可能已经存在。先查看失败任务，在原发布工作流中仅重跑失败任务，不重跑已成功的上传任务，也不靠普通提交重发该版本。工作流不覆盖已有 Tag、Release 或 PyPI 版本。临时环境问题可在原 CI 或发布工作流中重跑相应失败任务；源码需要修复时，用更高版本提交新的版本 PR。运行工件保留 7 天，依赖原工件的重跑应在保留期内完成。


## 当前候选的本地发布验收

ReleaseEnvelope 汇总同一干净候选的六类验收，`profile=local` 只说明该次本地平台、依赖和输入范围。它不授予全部能力 `sealed` 状态，也不表示已在 GitHub 或包索引发布。四个公开示例的证据范围为合成工程验收，不是历史真实市场收益或完整交易执行验收。

| Gate | 当前验收内容 |
| --- | --- |
| A | 安装态股票研究、checkpoint 后中断恢复、恢复结果与正常运行一致、篡改结果被拒绝 |
| C | 四个研究、至少三类 DAG；完整 Result v3、匹配且通过的独立 VerificationResult、独立 Verifier 源码和显式数据窗口 |
| D | checkpoint 提交和恢复、内容损坏拒绝、金融状态恢复不重复成交与费用 |
| F | 合成输入下股票/ETF/期货、资金精度、交易语义与 DSR 数学合同 |
| I/B | 确定性公共 CLI 发现、草稿诊断和四个研究完整执行，不作为真人或陌生 AI 试用证明 |
| L | 自包含 Result 与 VerificationResult 消费、项目身份隔离、指标或结果篡改拒绝 |

在完整公开源码目录准备好当前构建产物、BuildManifest 和不继承系统包的安装态 Python 后，使用 `tools/run_release_workflows.py --help`、`tools/build_gate_c_evidence.py --help` 和 `tools/run_release_gate_tests.py --help` 查看完整参数。所有验收输出使用不存在的仓库外目录；工作流生产器会在该目录创建合成数据库，执行前取得所在环境要求的写入批准。运行与验证阶段只读访问这些数据库。

Gate D/F/L 使用候选内的 `release/gate-test-protocol.json`，测试源码和协议均进入 BuildManifest。执行时关闭源码导入配置，从仓库外使用安装态 runtime；有失败、跳过或零测试均不生成通过收据。Gate L 的前置 Gate C 必须属于相同候选，不能沿用其他批次的通过记录。

`tools/build_release_envelope.py` 接受六个 `--gate-evidence gate-id=path`，校验候选、BuildManifest 和 Gate 类型，并把原始证据、能力清单、依赖锁与 BuildManifest 一起封存。能力清单和依赖锁必须与构建输入一致。工具中的 `verify_release_envelope_files` 可在搬移目录后复验封套及封存内容；执行日志、测试报告和研究工件另随验收目录保留。

## 能力声明与晋级

当前能力状态由 `src/research_pipeline/capabilities.json` 声明。公开门禁允许已有 `local_only` 能力；任何 `sealed` 声明必须同时给出授权的能力基线、当前候选发布封套、正式执行、负例和独立验证三类可核验记录。证据须覆盖该能力公开承诺的全部范围，不能把四个合成示例外推为分钟交易、费用真实性或任意规模资源保证。

独立公开仓库可通过 `QUANTWITNESS_CAPABILITY_BASELINE`、`QUANTWITNESS_RELEASE_EVIDENCE_ROOT` 和 `QUANTWITNESS_RELEASE_CANDIDATE_ID` 显式提供晋级材料。没有这些材料时，`sealed` 声明会失败；材料存在但候选、源码、测试结果或授权上限不匹配时同样失败。历史证据保留原身份，不自动迁移。
