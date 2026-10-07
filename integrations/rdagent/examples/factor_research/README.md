# 教学文档的因子研究基包

本例用十只虚构ETF、最终评估边界前的79个价格会话，准备正式Qlib开发研究的起点。输入是确定性Parquet，不需要数据库。`prepare_base(output)`生成ResearchPackage、Catalog、封存输入、算子bundle、独立Verifier和单基线研究包请求。

## 定义与时间

教学基线为`$close/Ref($close,5)-1`，表示前一交易会话收盘价相对其五期前收盘价的变化。每个研究会话09:30决定时，仅把前一会话及更早的收盘交给Qlib；当前会话收盘不参与特征。既有`historical_return`的5、10窗口槽都采用同一声明表达式，5、10窗口的`volatility`仍保留原公式。

新假设支持`$close/Ref($close,N)-1`和`$close/Mean($close,N)-1`（N为1至5），以及`Std($close,N)/Mean($close,N)`和`($close-Min($close,N))/(Max($close,N)-Min($close,N))`（N为2至5）。字段仅`close`，公式内各处窗口必须一致。Ref要求N+1期完整合法收盘；Mean要求N期完整合法收盘。使用Qlib数值算子计算，并由Verifier直接从正式Result中的封存原价独立手算。Std使用样本标准差ddof=1，平坦区间的位置缺失。其他公式尚不在本例独立复核范围内。

开发目标为相同validation样本的预测均方误差，方向是最小化。模型参数保持不变，只修改因子定义。最终test/holdout不进入循环。合成结果不表达真实市场收益或投资效果。

## 运行

在具备核心ml依赖、RD集成和固定上游源码的Linux环境中，从公开源码根执行：

```bash
python integrations/rdagent/examples/factor_research/prepare.py --output /mnt/i/research/factor-example
python -m quantwitness_rdagent factor-run --request /mnt/i/research/factor-example/request.json
python -m quantwitness_rdagent factor-inspect --request /mnt/i/research/factor-example/request.json
python -m quantwitness_rdagent factor-resume --request /mnt/i/research/factor-example/request.json
```

默认使用固定文本响应完成基线加两轮实验，验证研究组件接线及恢复，不证明语言模型自主提出了这些想法。每轮均执行真实Qlib拟合、正式结果和独立验证。`request.json`显式保存三轮和七次响应预算；语言模型付费调用为零。live请求通过`prepare.py --model-env-file <明确的.env路径>`生成，实际执行同样显式提供该文件，调用前应批准模型预算。

## 准备接口

调用方使用本文件同目录的`prepare.py`：

```python
from pathlib import Path
from prepare import prepare_base

package_template = prepare_base(Path("I:/research/factor-example"))
```

输出目录必须尚不存在。调用仅准备材料，不训练模型、不请求语言模型，也不运行研究。`package_template`直接嵌入`rd-factor-research-v1`请求；新候选由研究循环生成，不能把该单基线模板解释为有限候选菜单。

输出内容：

- `input/`：冻结输入、完整声明、bundles、独立Verifier；`input/request.json`记录design与模型事实。
- `package-template.json`：单基线的正式研究包执行请求。
- `confirmed-definition.md`：教学文档。
- `confirmed-definition.json`：`kind=synthetic`、`confirmed_by=example_definition`的明确教学定义，并非真实论文的人工确认记录。
- `source-archive/`：正式执行所用来源归档目录。

后续研究请求的`confirmed_spec`使用`{"kind":"synthetic","path":"<输出目录>/confirmed-definition.json"}`，基线采用上述公式。完整循环入口与调用预算见[研究循环说明](../../docs/factor-research.md)。原文基线和后续改进分别记录，不以改进公式冒充原文复现。

Qlib固定0.9.7；bundle还记录准备环境实际pandas、numpy及pyarrow版本。执行环境应与封存依赖一致。准备阶段不生成临时数据库；正式结果及VerificationResult由后续研究执行产生。

## 继续上一会话的研究

```bash
python -m quantwitness_rdagent knowledge-export --session /mnt/i/research/factor-example/session --output /mnt/i/research/prior-knowledge.json
python integrations/rdagent/examples/factor_research/prepare.py --output /mnt/i/research/factor-next --campaign-id synthetic_factor_next --knowledge-index /mnt/i/research/prior-knowledge.json --knowledge-output /mnt/i/research/next-knowledge.json
python -m quantwitness_rdagent factor-run --request /mnt/i/research/factor-next/request.json
```

教学响应从传入索引取得真实记录ID，选择其中尚未研究过的窗口，用来验证引用和跨会话接线。正式运行仍复核来源、开发范围和指标语义；准备出请求不代表索引已获准使用。两轮包含基线与一个新方向，固定响应共五次，没有付费调用。live模式同样可传入知识路径，方向和公式由显式模型服务提出。

输出索引必须与输入分开，并在会话目录之外。来源会话、正式结果和验证材料须保留；归档后恢复原路径再使用索引。知识不会把未改善结果提升为有效收益。
