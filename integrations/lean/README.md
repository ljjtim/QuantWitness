# LEAN 干净安装与正式研究验收

安装独立 LEAN 环境，并执行正式研究包。

## 开始前

- 输入：Windows x64、PowerShell 7、完整公开源码和输入声明。
- 输出：独立安装、准入计划、Result、报告和日志。
- 命令中的尖括号内容是待替换参数。

## 使用顺序

1. 从合成示例生成完整起步包。
2. 运行本页的统一运行入口。
3. 检查各阶段日志和正式 Result。
4. 用报告读取结果。
5. 长期保存安装与研究恢复依赖。

本目录将项目 ResearchPackage 接入本地 LEAN。`install.ps1` 从固定版本源码和依赖创建独立安装。`run.ps1` 在准备前自动同步源码，再执行 `prepare → admit → run → report`，每阶段失败立即停止并保留日志。所有 Python 命令使用安装目录内的虚拟环境和本仓库构建的 wheel，不依赖测试虚拟环境或 `PYTHONPATH`。

## 统一运行入口

当前安装路径支持 Windows x64 和 PowerShell 7，需要网络或对应下载缓存。命令在包含 `pyproject.toml` 的 QuantWitness 公开源码根目录执行。在数据系统仓库中先进入 `research_pipeline`。新用户从[合成日线与一分钟示例](../../examples/lean_quickstart/README.md)生成起步包，无需已有 LEAN 安装、私人数据或数据库。

LEAN 使用完整公开源码中的安装脚本和插件工程。wheel 提供 Python 框架。只安装 wheel 的用户还须取得相同版本的完整源码。

sdist 包含 LEAN 安装资源，解压后可按相同步骤使用。Linux/macOS 不适用当前安装脚本。

已有研究使用显式来源包、Catalog Lock、只读源库和分钟根：

```powershell
& ./integrations/lean/run.ps1 `
  -FromPackage '<原ResearchPackage目录>' `
  -CatalogLock '<批准的Catalog Lock目录>' `
  -DataDb '<源DuckDB文件>' `
  -MinuteRoot '<分钟数据根目录>' `
  -Output '<新验收输出目录>' `
  -DownloadCache '<下载归档缓存目录>'
```

`Output` 必须不存在。默认安装目录为 `Output-installation`。`LeanHome` 可显式指定已有独立安装，否则自动完成安装。

源数据库严格只读。报告直接读取本次正式 Result。入口不删除已有运行或历史结果，不提交文件。

纯日线研究可以省略 `-MinuteRoot`。一分钟请求须提供分钟根。输入默认直接读取，添加 `-PersistInputs` 才保存普通查询输入。日行情与逐日规则通过本机数据流交给 LEAN，执行当日时预读下一交易日，不写交接副本。

已提交的 Parquet 归档使用 `-InputSnapshotManifest '<输入清单JSON>'` 替换 `-DataDb`，两者必须且只能提供一个。归档经过同一 Catalog 和 PIT 准入，运行不连接数据库。含分钟请求时仍需显式提供 `-MinuteRoot`。

run 的内存、CPU、运行临时空间、进程及节点时限沿用原包声明，不自动提高额度。内存调度、进程树采样比较和运行记录使用同一声明值。实际采样超过额度时拒绝成功。资源租约等待上限120秒，不是节点执行时限。

## 安装阶段

可单独执行安装，不打开数据库或启动研究：

```powershell
& ./integrations/lean/install.ps1 `
  -LeanHome '<新安装目录>' `
  -FromPackage '<原ResearchPackage目录>' `
  -DownloadCache '<下载归档缓存目录>'
```

安装目录必须不存在。脚本下载或复用归档缓存，安装.NET SDK 10.0.403、uv 0.13.6、CPython 3.11.18。Python依赖见 `requirements.txt`。

安装继续读取来源包全部配套bundle的 `dependency_lock`，冲突时拒绝，按精确版本安装后逐项复验并执行pip check。证据保存在 `dependency-evidence/bundle-dependencies.json`。从仓库源码构建并安装独立wheel，编译固定LEAN版本 `a3bf4cc3e1e11a461b44d43894b0c6445f1100e6` 的Launcher及本目录插件。

安装对本地上游源码应用Python.NET关闭GIL处理和Windows虚拟环境路径初始化兼容补丁。后者直接过滤site-packages，不将Windows路径插入Python代码，保留用户及系统site-packages隔离。两项均不修改交易撮合、账户计算或仓库engine C#。

`NugetCache` 可显式提供本地NuGet下载缓存。它只复用依赖包，不复用已编译引擎或现有Python site-packages。包缓存不构成研究输出或checkpoint复用。

市场元数据由 `prepare_metadata.py` 读取来源包的完整证券和冻结minute-scope生成，基础LEAN元数据来自本次下载的上游Data。当前安装入口按证券保留上市起点和已可见日历版本，使用共同的不跨日A股时段。不兼容的时段会明确拒绝。

交易日历、规则和PIT仍由正式包准入检查。证券基础LotSize为1，历史数量规则由中国规则模型执行。

安装证据为 `clean-installation.json`、`wheels/`及`logs/`，构建资产位于 `rp-integration/`。安装完成后引擎、Python依赖和元数据被转换器冻结。改动后必须重新转换、准入。

## 转换与正式主链

`prepare.py --from-package PATH --output NEW_PATH --lean-home INSTALLATION` 只转换声明及配套bundle，不运行研究、不打开数据库、不从历史Result取数。输入须包含唯一项目策略、直接绑定的bars/snapshot/signals、因果Feature及配套bundle，bundle位于包内 `bundles/` 或同级 `../bundles/`。

转换保留四份包声明、QueryIR、minute-scope、数据和因果Feature节点、策略源码及参数。唯一策略节点声明 `execution_backend=lean` 和完整安装锁，一分钟采用 `lean_next_completed_close_v1`，日线采用 `lean_next_session_open_v1`。原生声明的输出包含 `package/`、`bundles/` 和 `conversion.json`。

统一运行入口对已有 LEAN 声明冻结当前安装。安装内容变化时生成新的 `package/` 和配套 `bundles/`，只更新安装绑定，保持数据请求、策略、指标和结果表声明。安装内容未变时沿用原包。

单独调用 `prepare.py` 保留来源包的冻结安装，不自动重绑。

同次运行只加载一次完整声明和分钟来源，准入编译一次，并将准入对象直接交给执行。独立 `package lint` 用于主动诊断。已有 LEAN 声明直接使用当前包和 bundle。

原生声明在准备阶段生成 LEAN 包。恢复从持久计划读取，检查输入、实现和安装身份。

已有 LEAN 声明的准备阶段只检查安装锁结构和安装根绑定，不逐个读取安装文件。LEAN 节点启动前检查实际文件集合和内容。跨进程计划加载与恢复同样检查安装内容。安装变化会拒绝执行，须更新声明并重新准入。

安装冻结与内容检查使用固定四路文件读取，仍逐个计算实际内容并按文件名排序交付身份。不使用文件大小或修改时间替代内容检查。

正式结果为同一 `research.lean-simulation.v2` 工件的 orders、intents、daily、trace、fills、positions、account_events、lifecycle 八张执行表，以及金融指标表 metrics。HTML报告请求使用 `lean-research-report-v2` 并绑定九表，字段与期货收益口径见[期货结果合同](../../docs/lean-futures-results.md)。行情、规则和公司行动保留在准入输入及运行依赖中，Result 不再复制这些输入。

原Feature源表单独保留，schema为`research.lean-signal-source.v1`。收益、风险、换手、费用与交易数量进入正式指标及报告。报告直接读取Result。

HTML请求与执行边界见[执行合同](../../docs/lean-execution.md)和[引擎接口](engine/README.md)。

## 计时和交付

每次运行保留 `acceptance-timing.json`、prepare/admit/run/report各自日志、正式Result、`report.md`和`report.html`。计时从prepare之前开始，到report完成结束。完整端到端时间包含来源准备、上游节点、执行、正式封存和report，不扣除上游工作或缓存收益。入口记录实际耗时，不设置固定秒数的性能通过门槛。

report阶段连续生成Markdown与自包含HTML，HTML请求绑定本次Result ID及实际九表ID，两次报告均计入完整report耗时。安装时间单独记录，不能混作研究性能。新安装、新进程、新运行目录不表示操作系统文件缓存已清空。

不声称磁盘冷缓存。节点`preparation.stage_seconds`只用于归因。`output_write_before_commit`不含正式commit，不能当作完整封存时间。

临时文件为安装的downloads/cache/temp、构建bin/obj和运行的temp。失败时保留诊断，不自动清理。正式Result、包、bundle与恢复所需工件须保留。


## 自动同步源码

`run.ps1` 默认比较当前源码与安装的 `source-sync.json`：Python 源码或打包输入变化时重建并安装 wheel。插件源码变化时执行 `engine/build.ps1 -Install`。未变化时跳过相应构建。

记录缺失时首次同步两部分。干净安装成功后直接记录源码，不重复构建。摘要只用于避免重复安装，正式安装锁仍由准备与执行流程验证。

源码同步在新研究准备之前完成，不启动后台监听。安装正在被其他进程使用时拒绝更新。安装或编译失败时停止，不进入研究。

从其他 Python 环境通过独立 CLI 启动的正式研究，其安装绑定可能只存在于计划中。因此同步也等待这些运行结束，不扫描大型计划。更新不修改原研究包或历史 Result。

旧计划和 checkpoint 不能跨安装变化恢复。研究需要长期恢复时，应为其保留独立安装目录。

只同步安装、不启动研究：

```powershell
& '<安装目录>/python-env/Scripts/python.exe' -B -X utf8 `
  ./integrations/lean/sync_installation.py --lean-home '<安装目录>'
```

`-RefreshPackage` 仍可用于强制重装 Python 包。SDK、上游 LEAN 和依赖环境继续沿用已有安装。同步后执行 `pip check`，依赖不满足时按当前依赖创建新安装。

构建日志、wheel 和源码副本保存在安装的 `package-builds/`，插件构建材料保存在 `rp-integration/`。同步时间属于安装准备，不计入正式研究阶段耗时。


## 常用名称

| 名称 | 含义 |
| --- | --- |
| ResearchPackage | 数据、算法、样本、指标和预算的完整声明 |
| Catalog Lock | 已确认的字段、来源和可见性说明 |
| PIT（按时点可见） | 过去某时刻只使用当时已知的信息 |
| bar（周期行情） | 一个日线或分钟区间的行情记录 |
| OHLCV（开高低收量） | 开盘价、最高价、最低价、收盘价和成交量 |
| Feature（特征） | 决策时已知的研究输入 |
| Label（标签） | 用于评价预测的未来目标 |
| bundle（封存包） | 完整代码、依赖或数据声明及其内容身份 |
| checkpoint（检查点） | 通过验收的节点输出和恢复依据 |
| Result（正式结果包） | 正式表、运行摘要及配置、实现和输入引用 |
| minute_scope（分钟范围引用） | 指向同一冻结日历和历史规则的小引用 |
| Supervisor | 监督运行和提交结果的主进程 |
| Worker | 执行节点计算的独立进程 |
