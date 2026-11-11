# 自定义指标：Arrow工件到正式Result

将自定义指标写入正式 Result。

## 开始前

- 输入：已安装框架、Arrow 依赖和新输出目录。
- 输出：指标定义、正式工件、Result 和报告。
- 命令从包含 `pyproject.toml` 的公开源码根执行。
- 尖括号内容是待替换参数，输出目录必须尚不存在。

## 使用顺序

1. 安装框架及示例依赖。
2. 运行本页的示例命令。
3. 从回执取得 Result 目录。
4. 生成正式指标报告。
5. 核对样本、单位与状态。

这个最小示例计算三个合成日收益的算术均值：`(0.01-0.02+0.04)/3=0.01`。它执行完整的ResearchPackage声明、Catalog/PIT准入、Runtime、正式Result发布及Python和CLI报告消费。输入为合成Parquet归档，不连接数据库。

## 运行

在完整源码仓库根目录执行。本数据仓库需先进入`research_pipeline/`。输出目录必须尚不存在。

```powershell
python examples/custom_metric/run.py --output <新的示例输出目录>
python -m research_pipeline report --result <receipt.json中的result_directory> --result-store <receipt.json中的result_store> --output <新的报告.md> --format markdown --json
```

运行由三份源码组成：

- `inputs.py`生成三个观察日的Arrow数据，编译独立Catalog，按可见时点准入并发布Parquet归档清单。
- `source/operator.py`只读取准入后的列式输入、计算均值并交付观察表及指标表。
- `run.py`构建算子bundle和完整四份研究声明，调用正式`package admit`与`run`，最后从Result生成报告。

输出目录保存`catalog/`、`inputs/`、`inputs.json`、`package/`、`bundle/`、`plan/`、`run/`、`results/`、`metric-definition.json`、`admission.json`、`run-receipt.json`、`receipt.json`和`report.md`。计划、事件、检查点、可达性证明及Result均由正式入口生成。

运行总容量声明为8GiB内存、4个CPU槽、4个进程槽和32GiB临时空间，满足当前数据物化节点预算。预算上限不代表这个3 行示例的实际占用。项目均值节点自身声明1GiB、1个CPU槽、4个进程槽、64MiB临时空间和60秒。

## 定义与输出

`run.py`的`definition()`用`MetricDefinition.build()`提供完整定义。`to_dict()`的结果可放入研究包`spec/research.yaml`的`metric_definitions`列表。定义绑定实际实现、单位、频率、年化和空值政策，以及五个测量语义字段：

|字段|本例含义|
|---|---|
|quantity|观察日收益的均值|
|numerator|观察日收益之和|
|denominator|观察交易日数|
|observation_timing|各观察交易日收盘后|
|aggregation|算术平均|

`numerator`和`denominator`都必须填写，即使分母是1也要明确。实现摘要和定义摘要由当前合同要求，用于绑定实际生产算法。

正式指标表使用下列Arrow类型。`sample_start`、`sample_end`必须是ISO日期或时间**字符串**，不能直接写成Arrow date或timestamp。

|字段|Arrow类型|要求|
|---|---|---|
|metric_ref|string|与指标定义引用一致|
|value|float64|有限实数|
|unit|string|与定义单位一致，本例decimal_return|
|sample_start|string|本例2024-01-03|
|sample_end|string|本例2024-01-05，不早于开始日期|
|sample_size|int64|正整数，本例3|
|status|string|正式可消费值使用computed|

`source/operator.py`的`calculate_metric()`输出显式`METRIC_SCHEMA`。研究包的`result.tables`选中指标表，`metric_definitions`绑定算法与测量语义，包的`metric_contract`声明消费目标。准入自动生成可达性证明，Runtime生成正式工件，框架在原子发布前核对指标行。

```python
from research_pipeline.results import load_result_context, render_result_report

context = load_result_context(result_directory, result_store=result_store)
metric, = context.metrics
assert metric.status == "computed"
markdown = render_result_report(context)
```

## 验收

```powershell
python -m pytest -q tests/test_custom_metric_example.py --basetemp <新的临时目录> -p no:cacheprovider
```

测试用手算样本核对公式和schema，再实际执行`operator build → package admit → run → Result`。通过`load_result_context`、`build_result_report`以及CLI `report`核对正式指标值、样本、状态、成功节点和报告引用。只写Parquet、JSON、事件及报告文件，不创建或修改数据库。

本例结论固定为`research_observation`，合成收益不是市场数据或可交易账户结果。所有运行文件由调用方指定目录保存。源码旁不生成临时数据。

独立 `package lint` 用于主动诊断。普通正式流程从完整声明直接准入。
