# 公式复现

本流程复现已确认的公式计算。
论文收益结论还依赖数据、资产池和交易假设。
公式数值正确不能代替完整实证复现。

## 准备材料

准备归档PDF和完整ResearchPackage。
准备Catalog、输入快照清单和来源归档。
项目提供固定adapter及候选源码接口。
项目声明正式指标和算法验收案例。
所有输出放入仓库外研究目录。

首次体验使用[成交量集中度示例](../examples/volume_concentration/README.md)。
示例提供教学PDF、2400条合成分钟和手算案例。
示例不需要付费模型或数据库。

## 提取和确认定义

`STUDY_PACKAGE`指向研究包。
`SOURCE_ARCHIVE`指向来源归档。
`SOURCE_ID`使用包内来源身份。
`MODEL_ENV`指向仓库外模型配置文件。
`WORK`指向新研究目录。
在公开源码根执行命令。

```bash
python -m quantwitness_rdagent spec-extract --package "$STUDY_PACKAGE" --source-archive-root "$SOURCE_ARCHIVE" --source-id "$SOURCE_ID" --pages 4 5 6 --output "$WORK/spec" --model-env-file "$MODEL_ENV" --max-calls 1 --max-output-tokens 8192
python -m quantwitness_rdagent spec-review --draft "$WORK/spec/draft.json" --materials "$WORK/spec/materials.json" --output "$WORK/spec/review.md"
```

提取结果是待审草稿。
核对原文、端点、窗口、单位和缺失处理。
在decisions.json记录接受条目与歧义决定。
确认人完成审阅后执行确认命令。

```bash
python -m quantwitness_rdagent spec-confirm --draft "$WORK/spec/draft.json" --materials "$WORK/spec/materials.json" --decisions "$WORK/spec/decisions.json" --confirmed-by reviewer --approve --output "$WORK/spec/confirmation.json"
python -m quantwitness_rdagent spec-render --draft "$WORK/spec/draft.json" --materials "$WORK/spec/materials.json" --decisions "$WORK/spec/decisions.json" --confirmation "$WORK/spec/confirmation.json" --output "$WORK/spec/formula.txt"
```

确认记录绑定材料、决定和具体接口。
更改材料后重新审阅确认。

## 绑定和运行

请求模板声明完整执行材料和源码修改范围。
模型只能实现确认接口。
项目adapter继续负责正式输入和输出。

```bash
python -m quantwitness_rdagent request-build --template "$WORK/request-template.json" --draft "$WORK/spec/draft.json" --materials "$WORK/spec/materials.json" --decisions "$WORK/spec/decisions.json" --confirmation "$WORK/spec/confirmation.json" --output "$WORK/request.json"
python -m quantwitness_rdagent run --request "$WORK/request.json" --model-env-file "$MODEL_ENV"
python -m quantwitness_rdagent inspect --request "$WORK/request.json"
python -m quantwitness_rdagent resume --request "$WORK/request.json" --model-env-file "$MODEL_ENV"
```

固定响应模式不传模型配置文件。
运行保存候选代码、诊断、正式执行引用和Result。
失败状态不产生金融指标。
算法正确性使用项目手算和确定性案例验收。
后续改进另记为新研究，不改写原公式定义。

## 限制

固定上游研究调度需要独立Linux环境。
Windows使用WSL，跨系统路径须显式绑定。
源数据保持只读。
真实金融可见性由准入和项目算子共同约束。
论文发表时间不代表历史样本当时已知该方法。
