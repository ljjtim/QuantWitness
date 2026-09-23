# 安装与发布构建

最低支持 Python 3.10。版本、Python 范围、运行依赖、extras 和 `quantwitness` 入口只在 `pyproject.toml` 中声明，由标准 setuptools PEP 517 backend 生成 wheel 和 sdist。展示品牌为 `QuantWitness`，PyPI 发行名和安装命令使用规范化小写 `quantwitness`，Python import 保留 `research_pipeline`。`test`、`dev` 在 Python 3.10 下显式依赖 `tomli`，其他版本使用标准库 `tomllib`。

开发安装使用 `python -m pip install ".[dev]"`；测试环境使用 `python -m pip install ".[test]"`。
两者同时准备`setuptools>=68`、`build`和`wheel`，满足无隔离发行测试的本机构建依赖。
这些 extras 不包括可选机器学习模型，按研究需求另装 `ml` 或 `ml-lightgbm`。

## 正式构建

先在独立环境准备 `pyproject.toml` 的构建依赖以及 `build`。离线机器需提前准备当前平台和 Python 版本兼容的 wheelhouse；构建本身不联网安装依赖。

```powershell
python -m pip install --no-index --find-links <wheelhouse目录> "setuptools>=68" wheel build
python tools/build_release_artifacts.py --project . --output <不存在的仓库外输出目录>
```

工具先按 allowlist 复制源码到临时 staging，排除单仓的私有 Catalog 声明与默认 Lock；再调用标准 backend，检查 wheel/sdist 的库存和元数据，最后生成带核心测试的源码 zip。三类产物通过检查后才发布到输出目录。脚本不再生成 METADATA、WHEEL 或 RECORD，也没有 `setup.py` 回退入口。

人工发布只使用本次命令 JSON 回执中的 `wheel`、`sdist` 和 `source_archive` 绝对路径，并记录候选提交及库存验收结果。确认新目录恰好包含这三件文件；混入第二个 wheel 不得发布。历史 `dist/` 文件不属于本次候选，不能按通配符选择，更不能以旧文件代替当前源码构建。

干净 wheel 验收需要调用方显式提供一个持久 Catalog Lock，供安装后资源读取测试使用；它不是发行包资源。验收还检查公共 Recipe 为空、`package init/lint` 可用：

```powershell
./scripts/verify_clean_wheel.ps1 -Wheel <wheel路径> -ReceiptOut <仓库外新收据路径> -CatalogLock <持久Catalog-Lock目录>
```

直接在完整工作树运行标准构建不承担正式 allowlist 筛选；正式交付使用上面的 staging 工具。历史 release 证据不随当前构建覆盖，工作树未提交时也不能把测试构建称为干净发布候选。

## 最低版本验收

`tests/test_release_metadata_ssot.py` 比较 pyproject、wheel、sdist、安装后元数据的名称、版本、Python 范围、依赖、extras 和 console script。使用最低版本创建三个不继承系统包的虚拟环境，分别安装 wheel、sdist 和源码 zip，并运行 CLI、依赖完整性检查及不访问数据库的核心 smoke。

该验收只证明安装与发布元数据合同，不代表通过真实研究、全部发布门禁或能力晋级。

```powershell
$env:RP_MINIMUM_PYTHON = "<Python 3.10 的 python.exe>"
$env:RP_RELEASE_BUILD_PYTHON = "<已准备构建依赖的 python.exe>"
$env:RP_RELEASE_WHEELHOUSE = "<包含运行、test、dev 和 build 依赖的 wheelhouse目录>"
python -m pytest tests/test_release_metadata_ssot.py -q
```

测试解释器也需安装 test extras。缺少最低版本解释器或离线依赖时验收失败，不能跳过。发布依赖锁仍按其记录的实际 distribution 版本和字节核验；干净环境安装 pyproject 所允许的新版本不等于满足某份历史发布锁。

## 公开仓库 CI

公开 `ci` 保留 Python 3.10 与 3.13 矩阵，检查公开源码清单、框架边界、公共 Operator 的内建身份和未经批准的晋级，以及首次公开时能力不得自称 `sealed`。每个矩阵环境都从正式 allowlist 构建 wheel，在独立虚拟环境安装，从仓库外检查导入位置，并在临时合成 DuckDB 上运行四个示例的 `lint → admit → run → Result → verify → VerificationResult → report` 闭环。完整整改基线依赖父仓库材料，不进入公开候选；公开 CI 的能力状态限制以独立仓库可执行的门禁为准。

公开仓库中的 `python tools/public_source_inventory.py --project . --check` 要在独立 Git 仓库根运行：它比较 Git 已跟踪文件与公开允许清单，拒绝额外提交或未纳入 Git 的允许文件，并要求正式治理文件 `.github/CODEOWNERS` 已跟踪。父仓库内的 `research_pipeline/` 仍用该工具的导出命令生成独立候选，不以嵌套目录的 `--check` 代替公开仓库检查。

四个示例的项目 Worker 固定依赖 `pyarrow 21.0.0`；CI 的源码测试环境和隔离 wheel 环境均安装此版本、执行 `pip check`，再核对隔离环境的版本和 wheel 来源。该固定版本只约束示例验收，不收窄 `pyproject.toml` 面向用户的依赖范围。CI 的 wheel 路径来自本次构建回执，不扫描历史 `dist/`。

工作流检查通过不代表 GitHub ruleset 或 required checks 已生效；首次公开前需在平台按批准的治理方案配置并验证。四项目闭环只证明合成研究，不证明交易费用、保证金或真实市场表现。
