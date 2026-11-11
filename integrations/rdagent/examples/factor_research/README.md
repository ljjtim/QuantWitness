# 合成因子研究

本例使用10只虚构ETF和79个价格会话。
准备阶段生成Parquet、Catalog和完整研究包。
运行阶段执行3轮正式Qlib开发研究。
固定响应不调用付费模型。

## 公式与时点

基线为`$close / Ref($close, 5) - 1`。
每个会话09:30只使用此前完整会话收盘价。
historical_return的5和10窗口槽使用同一公式。
volatility特征保持原公式。

`prepare.factor_space()`枚举4类候选。
Ref和Mean相对变化使用1至5期窗口。
相对标准差和区间位置使用2至5期窗口。
Std采用样本标准差，平坦区间位置缺失。
项目算子检查合法价格和完整窗口。
项目算法测试使用独立手算期望值。

请求与正式design共同保存factor_space。
空间声明字段、公式、方向、槽和资格列。
框架只在声明范围内选择表达式。
开发目标是相同validation样本的均方误差。
最终test/holdout不进入研究循环。

## 运行

前置条件：已安装核心ml依赖、可选集成和固定RD-Agent。
在公开源码根执行命令。
WORK指向仓库外目录。

```bash
python integrations/rdagent/examples/factor_research/prepare.py --output "$WORK/factor"
python -m quantwitness_rdagent factor-run --request "$WORK/factor/request.json"
python -m quantwitness_rdagent factor-inspect --request "$WORK/factor/request.json"
python -m quantwitness_rdagent factor-resume --request "$WORK/factor/request.json"
```

准备命令只创建材料。
请求使用rd-factor-research-v2。
默认预算为3轮、7次固定响应。
实际Qlib拟合和Result由运行命令生成。
live请求在准备和运行时都传入`--model-env-file`。

## 准备接口

```python
from pathlib import Path
from prepare import prepare_base, factor_space

package_template = prepare_base(Path("research-output/factor"))
space = factor_space()
```

输出目录必须尚不存在。
package-template.json保存单基准执行模板。
confirmed-definition.json保存明确的合成定义。
input目录保存正式声明、输入清单和算子bundle。
source-archive目录保存来源归档。
prepare_base把同一空间写入正式设计与身份参数。

跨会话知识见[研究知识](../../docs/coding-knowledge.md)。
新项目提供自己的factor_space和计算算子。
合成结果不表示真实市场收益。
