# 示例目录

[lean_quickstart](lean_quickstart/README.md)提供 Windows x64 的日线与一分钟回测起点：合成 Parquet、完整策略包和 Catalog 在安装 LEAN 前生成，不依赖个人数据库或已安装引擎。按其说明执行独立安装、正式回测及报告。

推荐从 `equity_cross_section/` 开始，按 [Workspace 合成研究入门](../docs/workspace-quickstart.md) 完成第一次报告和样本变体比较。其余三个项目展示不同研究类型的扩展方式。这里的可执行文件随完整公开源码提供，wheel 不包含示例源码。

这里放四个可以独立分发的合成研究项目：

- `equity_cross_section/`：股票横截面排序与事后收益差；
- `etf_time_series/`：单标的时间序列动量；
- `event_study/`：公告版本、可见时间和重叠事件；
- `futures_term_structure/`：多合约期限结构与当时可见的合约选择。

每个项目都包含 ResearchPackage 模板、项目 Operator 薄声明和源码、确定性合成数据及贴身测试。构建后的完整包携带生产指标定义。项目逻辑只在自己的目录中，不注册公共 Operator，也不进入 `src/research_pipeline/`。示例不依赖个人数据库、供应商导出、cookie、远端配额或未公开的 Catalog。

这些项目各自提供已填写的 ResearchPackage，不从 `package init` 草稿继承研究身份、数据请求或图节点。

四个示例的项目节点统一声明 1 GiB 内存，覆盖 Supervisor、Worker 及其后代，并为 Python 和数值库的启动开销预留空间；其他额度为 1 CPU、4 个进程槽、64 MiB 临时空间和 60 秒。4 个进程槽覆盖 Supervisor、Windows venv 启动器、实际 Worker，以及环境身份采集可能短暂启动的系统版本查询进程。该预算仍由 Runtime 强制检查，超额时停止执行并返回实测值与限额。

合成数据和项目源码随 QuantWitness 以 Apache-2.0 发布，不代表真实市场，不提供投资建议。四个项目使用一次性合成 DuckDB 与临时 Catalog Lock，按 `package lint → admit → run → Result → report` 执行；验收会写临时环境和结果工件，但准入与运行不会修改合成数据库。它不替代真实市场、费用或实盘验收。

生成一次性合成环境时必须指定一个尚不存在的目录：

```powershell
python examples/prepare_synthetic_environment.py --output <必须不存在的临时目录>
```

脚本只写调用方指定的目录，生成合成 DuckDB、Catalog 声明、审批文件和 Catalog Lock；它不会读取或修改本机研究数据库。

不写数据库的检查：

```powershell
python -m pytest -q examples/equity_cross_section/test_operator.py examples/etf_time_series/test_operator.py examples/event_study/test_operator.py examples/futures_term_structure/test_operator.py
python examples/build_bundles.py --output <必须不存在的临时目录>
```

构建脚本只构建 Operator bundle，并将每个源模板复制到 `<输出目录>/<项目>/package`。返回清单中的 `package` 是完整包实际路径；指标定义以现有 `MetricDefinition.to_dict()` 格式写入其 `spec/research.yaml`，`implementation_ref` 与 `implementation_digest` 绑定实际生产算法。运行使用构建返回的路径，源码模板保持原样。

报告直接读取正式 Result：

```powershell
python -m research_pipeline report --result <Result目录> --result-store <Result存储根> --output <报告路径>
```

Python 消费入口：

```python
from research_pipeline.results import ResultContext, load_result_context

context: ResultContext = load_result_context(result_path, result_store=result_store)
metrics = context.metrics
claim_level = context.claim_level
limitations = context.limitations
snapshot = context.snapshot
```

在已允许创建临时合成数据库的测试环境中执行完整闭环：

```powershell
python -m pytest -q tests/test_public_examples.py::test_public_examples_complete_formal_cli_workflow
```


## Qlib 日频模型起点

[qlib_portfolio](qlib_portfolio/README.md)使用公开合成ETF行情和Parquet归档。`development`仅训练和评价validation，`model`继续完成逐时点选择及单次最终holdout；`portfolio`将test预测按冻结规则转为组合目标，经LEAN日频引擎生成订单、意图、账户、策略轨迹与金融指标，并由正式Result交付。三种模式共享特征公式和模型实现，最终holdout不用于组合规则选优。运行需要安装`ml`可选依赖。

安装模型依赖后，可单独检查 development 的正式执行、MSE、Result 报告及留出隔离：

```powershell
python -m pytest -q tests/test_public_examples.py::test_public_qlib_development_complete_cli_workflow --basetemp <必须不存在的临时目录>
```


[qlib_risk](qlib_risk/README.md)在同一模型研究上接入风险目标，直接交付Result中的风险输入、优化诊断与LEAN日频执行事实。

## 运行产物

构建目录保存 Operator bundles 与完整包副本；合成环境目录保存临时 DuckDB、Catalog 与 Lock。研究运行目录保存计划、工件和 Result；报告直接从 Result 读取。所有输出均使用调用方指定目录。
