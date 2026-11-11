# 第一项研究：换手率因子

本教程从换手率因子的研究方法开始。你会运行完整研究，并读懂结果文件。

教程使用虚构证券和合成数据。不需要聚宽账号、行情数据库或模型服务。

## 研究要回答什么

换手率表示一段时间内股票交易的活跃程度。本例检验：过去换手率较低的股票，之后收益是否更高。

研究方法参考华泰证券的换手率类因子报告。本教程保留“历史窗口、排序分组、未来评价”的结构。它是教学实验，不是原文12个因子的完整复现。

| 名称 | 本例的含义 |
| --- | --- |
| 因子 | 用于给证券排序的数字 |
| 观察窗口 | 计算平均换手率的过去交易日 |
| 信号 | 按平均换手率从低到高形成的排序 |
| 分组 | 将证券分为5组 |
| 标签 | 信号形成之后的收益，用于评价 |
| 研究包ResearchPackage | 数据、算法、时点和输出的声明 |
| 扩展包bundle | 封存的项目算法及其依赖 |
| 结果包Result | 正式结果表、指标和来源引用 |

## 准备环境

需要Python 3.10或更新版本。需要同一版本的完整源码。

示例命令使用PowerShell。命令从公开仓库根目录执行。

1. 获取源码。

```powershell
git clone https://github.com/ljjtim/QuantWitness.git
cd QuantWitness
```

2. 创建隔离环境。

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Linux或macOS改用 `source .venv/bin/activate`。

3. 安装框架和示例依赖。

```powershell
python -m pip install --upgrade pip
python -m pip install ".[dev]" "pyarrow==21.0.0"
```

4. 检查命令入口。

```powershell
python -m research_pipeline --help
```

如果终端找不到模块，确认已激活上一步的环境。

## 运行研究

输出目录必须尚不存在。示例只向这个目录写运行文件。

1. 指定输出目录。

```powershell
$work = Join-Path (Get-Location).Path '../quantwitness-work/turnover-first'
```

2. 执行示例。

```powershell
python examples/turnover_factor/run.py --output $work
if ($LASTEXITCODE -ne 0) { throw '研究未完成，查看终端错误' }
```

脚本依次生成合成输入、构建项目扩展、声明研究包。框架检查来源与时点后，执行算法并发布Result。

3. 打开研究报告。

```powershell
Get-Content -LiteralPath (Join-Path $work 'report.md')
```

4. 查看结果入口。

```powershell
Get-Content -LiteralPath (Join-Path $work 'receipt.json')
```

收据记录正式结果位置。不要把终端进度或临时表当作最终结果。

## 怎样读结果

先确认程序退出码为0。再确认收据包含正式Result的位置。

正式 `observations` 表按因子值从低到高分为 5 组。
第 1 组为低换手率，第 5 组为高换手率。
Markdown 报告展示指标摘要，不逐行展示分组成员。

本例有 20 只证券和 9 个合成交易日。
窗口使用观察日之前的 3 个交易日，不包含观察日。
正式 Result 保存 120 行分组观察。
最后 20 行的未来标签为空，分组仍保留。

正式指标使用 5 个合格观察日。
指标为低组减高组收益差的算术均值：`0.016`。
单位是小数收益，对应 1.6 个百分点。
这不是年化收益，也不是交易账户收益。

若要查看分组成员，读取正式 `observations` 表。
在示例输出后，用同一环境运行：

```powershell
$env:TURNOVER_RESULT_RECEIPT = Join-Path $work 'receipt.json'
@'
import json
import os
from pathlib import Path
from research_pipeline.results import load_result_context

receipt = json.loads(Path(os.environ['TURNOVER_RESULT_RECEIPT']).read_text(encoding='utf-8'))
context = load_result_context(
    receipt['result_directory'], result_store=receipt['result_store'],
    additional_table_ids=('observations',),
)
for batch in context.snapshot.iter_table_batches(
    'project.turnover_factor.observations.v1', batch_size=20,
):
    print(batch.to_pydict())
    break
'@ | python -
```

命令只读取正式 Result 的首批 20 行。
观察列为 `session`、`code`、`mean_turnover`、`group` 和 `forward_return`。

本例使用等权分组。低组减高组是两个组收益的差。它不是实际多空账户的资金收益。

合成数据用于核对算法。不要用合成收益判断市场中的投资价值。

## 文件为什么要分开

| 文件或目录 | 用途 | 是否保留 |
| --- | --- | --- |
| `package/` | 本次研究的完整声明 | 保留 |
| `bundle/` | 已封存算法 | 保留 |
| `catalog/` | 数据字段、来源与时点规则 | 保留 |
| `inputs/` | 本次输入归档 | 保留 |
| `plan/` | 已准入计划 | 保留 |
| `run/` | 事件与检查点 | 需要恢复时保留 |
| `results/` | 正式结果 | 保留 |
| `receipt.json` | 结果位置 | 保留 |
| `report.md` | 可阅读的报告 | 保留 |

具体输出以[示例说明](../examples/turnover_factor/README.md)为准。Result可能引用目录外的输入，迁移时一并携带。

## 怎样接入真实数据

真实研究需要自己的合法数据源。框架不附带个人行情库或付费数据。

1. 确认换手率的单位和分母。
2. 声明行情、换手率与可见时间。
3. 按历史日期确定证券资格。
4. 保留完整观察窗口。
5. 把未来收益单独作为标签。
6. 声明分组、权重和费用。
7. 用小样本核对计算。
8. 使用新研究目录运行完整样本。

原报告的完整研究还涉及行业与市值中性化。它包含月度回归、IC和多种分层组合。真实复现还需核对原文样本、复权和上市历史。

信息系数（IC）度量因子排序与未来收益的关系。IC不是组合收益，也不包括交易费用。

详细操作见[创建研究](getting-started.md)和[数据接入](catalog.md)。项目公式放在扩展源码，避免写入框架核心。

## 常见错误

| 现象 | 原因或处理 |
| --- | --- |
| 输出目录已存在 | 使用新的目录，避免覆盖结果 |
| 找不到示例脚本 | 获取完整源码，安装包不包含示例目录 |
| 模块导入失败 | 激活环境，重新核对安装命令 |
| 来源或时点检查失败 | 核对Catalog声明与输入，不删除检查 |
| 资源不足 | 查看节点请求和运行总容量 |
| 运行中断 | 用`inspect`查看原运行，再按返回建议恢复 |

```powershell
python -m research_pipeline inspect --run-root "$work/run" --json
```

修改输入、算法或配置后，创建新的准入计划。只重建报告时，直接读取已有Result。

## 接下来学什么

- [研究包](research_package.md)：理解声明文件。
- [项目扩展](../EXTENSIONS.md)：添加自己的公式。
- [模型研究](walk_forward_model.md)：训练并评价模型。
- [LEAN教程](../examples/lean_quickstart/README.md)：处理订单与账户。
- [AI研究](../integrations/rdagent/README.md)：自动提出开发候选。
