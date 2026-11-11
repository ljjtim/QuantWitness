# 研究知识与编译经验

研究知识保存正式开发事实。
编译经验保存实现与修复的技术事实。
两种材料都引用来源会话。
失败状态不能充当金融指标。

## 导出研究知识

完成因子会话后导出索引。
`WORK`指向仓库外研究目录。

```bash
python -m quantwitness_rdagent knowledge-export --session "$WORK/factor/session" --output "$WORK/factor-knowledge.json"
python integrations/rdagent/examples/factor_research/prepare.py --output "$WORK/factor-next" --campaign-id next-study --knowledge-index "$WORK/factor-knowledge.json" --knowledge-output "$WORK/factor-next-knowledge.json"
python -m quantwitness_rdagent factor-run --request "$WORK/factor-next/request.json"
```

知识合同为`rd-research-knowledge-v3`。
导出读取已终止会话和正式Result。
入选来源须保留完整材料。
输出索引必须位于来源会话之外。
相同索引重复导出不会改变内容。

## 匹配范围

检索比较完整factor_space声明。
检索同时比较资产、来源、日历和价格口径。
开发区间、as_of和指标语义也必须匹配。
相同space_id不能替代完整声明一致。
改变字段、槽、候选或资格列会排除旧记录。

索引只帮助筛选，不替代正式来源。
入选后系统复核会话、研究包和Result。
最终test/holdout禁止进入提案上下文。
正式负结果继续保留，不改写为成功结论。
当前索引不读取旧合同知识索引。

## 编译经验

公式实现请求可显式提供coding_knowledge。
配置引用正式来源会话及冻结范围。
模型研究使用coding_knowledge.source_sessions和model_family。
经验只提供适用范围内的实现事实。
技术反馈使用错误类别和受支持的修复范围。
原始日志、价格和最终评价不会进入技术提示。

会话目录、研究包、模型和Result是实际依赖。
迁移时一起保留。
删除来源后，索引不能继续作为正式证据。
