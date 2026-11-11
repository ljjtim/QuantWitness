# 有界研究循环

研究循环在冻结开发范围内比较候选。
正式研究包执行复用Workspace主链。
循环只读取成功Result中的开发指标。
最终test/holdout需要独立研究流程。

## 准备完整研究包

包声明来源、证券、日期、时点和正式指标。
项目算法通过算子bundle接入。
PackageCampaign使用research_kind=package。
source绑定研究包、Catalog、输入清单和扩展bundle。
runtime_options声明正式执行选项。

development固定start、end和as_of。
objective声明表、schema、值列、日期列和可见时间列。
objective还声明过滤、聚合方式和优化方向。
指标语义必须与正式MetricDefinition相符。

candidates只覆盖已有节点参数。
parameter_overrides包含node_id、parameter_name和value。
覆盖值必须与原参数类型兼容。
不使用深层合并、自由SQL或项目runner。

## 运行合成示例

前置条件：已安装固定RD-Agent和可选集成。
从公开源码根执行命令。
`WORK`指向仓库外新目录。

```bash
python integrations/rdagent/examples/package_campaign/prepare.py --output "$WORK/campaign"
python -m quantwitness_rdagent campaign-run --request "$WORK/campaign/request.json"
python -m quantwitness_rdagent campaign-inspect --request "$WORK/campaign/request.json"
python -m quantwitness_rdagent campaign-resume --request "$WORK/campaign/request.json"
```

示例生成合成输入、Catalog和研究包。
示例只验证工程行为，不提供投资效果证据。
预测校准示例见[两轮教学例](../examples/prediction_campaign/README.md)。

## 停止与恢复

预算限制轮数、评价、模型调用和输出token。
stop声明目标值、最小改善幅度和耐心轮数。
达到停止条件后保留已有记录。
失败执行只反馈诊断。

恢复复用冻结输入、完成调用和正式执行引用。
请求改变时创建新会话。
未知模型调用结果须先核对收据。
自动恢复不会重复付费请求。

因子和模型的专用研究协议见[因子研究](factor-research.md)与[模型研究](model-research.md)。
项目公式及资格规则留在项目扩展。
