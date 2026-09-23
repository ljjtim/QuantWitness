# 示例目录

这里放四个可以独立分发的合成研究项目：

- `equity_cross_section/`：股票横截面排序与事后收益差；
- `etf_time_series/`：单标的时间序列动量；
- `event_study/`：公告版本、可见时间和重叠事件；
- `futures_term_structure/`：多合约期限结构与当时可见的合约选择。

每个项目都包含 ResearchPackage、项目 Operator 薄声明和源码、独立 verifier、确定性合成数据及贴身测试。项目逻辑只在自己的目录中，不注册公共 Operator，也不进入 `src/research_pipeline/`。示例不依赖个人数据库、供应商导出、cookie、远端配额或未公开的 Catalog。

这些项目各自提供已填写的 ResearchPackage，不从 `package init` 草稿继承研究身份、数据请求或图节点。

合成数据和项目源码随 QuantWitness 以 Apache-2.0 发布，不代表真实市场，不提供投资建议。四个项目已在一次性合成 DuckDB 与临时 Catalog Lock 上完成 `package lint → admit → run → Result → verify → VerificationResult → report` 验收；验收会写临时环境和结果工件，但准入与运行不会修改合成数据库。它不替代真实市场、费用或实盘验收。

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

在已允许创建临时合成数据库的测试环境中执行完整闭环：

```powershell
python -m pytest -q tests/test_public_examples.py::test_public_examples_complete_formal_cli_workflow
```
