# 因子与模型联合研究

联合研究共用开发范围、正式指标和总预算。
首轮复现已确认因子基线。
后续轮次选择因子或模型分支。
模型使用开发指标最好的已验证因子。

## 请求合同

请求使用`rd-joint-research-v2`。
`factor_request`使用`rd-factor-research-v2`。
`model_request`使用`rd-model-research-v2`。
两分支共用source、development、objective和proposer。
正式研究设计和指标定义也必须相同。
因子空间通过`factor_space`明确声明。

预算字段为rounds、evaluations、model_calls、output_tokens和max_output_tokens_per_call。
两分支的提案、修复和反思共用预算。
每次正式评价先登记预约。
失败评价保留预约记录。

可选`research_schedule`明确每轮分支。
列表长度必须等于rounds。
第1项为baseline，其余项为factor或model。
例如`["baseline","factor","factor","model"]`声明4轮。
未声明时，方向选择按开发证据选择分支。
`stop`可提前结束研究。

## 因子交接

因子方向来自`factor_space.candidates`。
模型方向为`generated_model`。
模型提案记录factor_record_id和factor_expression。
正式模型包把表达式写入声明的feature_slot。
框架保留其他特征，并同步更新研究身份。

每轮从正式Result读取study_design、samples和validation_predictions。
共同资格只检查population_feature_columns声明的列。
零值是有效数值，缺失值会拒绝金融反馈。
样本、标签时点、fold和validation成员须与基线相同。
候选值可以变化，评价成员保持固定。
最终test/holdout表禁止进入共同资格检查。

## 运行示例

前置条件：已安装核心ml依赖、固定RD-Agent和可选集成。
在公开源码根执行命令。
`WORK`指向仓库外目录。

```bash
python integrations/rdagent/examples/joint_research/prepare.py --output "$WORK/joint"
python -m quantwitness_rdagent joint-run --request "$WORK/joint/request.json"
python -m quantwitness_rdagent joint-inspect --request "$WORK/joint/request.json"
python -m quantwitness_rdagent joint-resume --request "$WORK/joint/request.json"
```

示例使用合成行情与固定响应。
负结果按正式开发指标保存。
恢复继续原分支、预算、收据和Result引用。
改变factor_space或正式设计时创建新会话。
