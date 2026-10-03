# 首次使用

第一次接触框架，先完成 [Workspace 合成研究入门](workspace-quickstart.md)：已有股票横截面项目包含填写完整的声明、算子和独立 Verifier，可以贯穿运行、报告与变体比较。按 [README 的源码获取步骤](../README.md)取得同版文档与 `examples/`；仅安装 wheel 不包含示例。

本页用于创建自己的研究。`package init` 生成中性草稿，不能直接运行；现成示例与公共 Recipe 是不同入口，当前公共 Recipe 清单为空。

## Windows 当前会话使用 UTF-8

机器 JSON 始终按 UTF-8 字节输出。Windows 下先在当前 PowerShell 会话设置：

```powershell
$env:PYTHONUTF8='1'
```

该设置同时保证 argparse help 和第三方库文本经过 UTF-8 通道；只影响当前 PowerShell 会话。

## 1. 先固定研究口径

在创建文件前明确：研究假设、资产、频率、决定时点、执行时点、样本区间、预热、复权、公司行动、停牌/涨跌停/ST、基准、费用、参数空间、holdout、主要指标、claim 上限、clock、seed 和资源预算。

缺失值可以在 `package lint` 后补，但不能把猜测写成用户已经确认的事实。

## 2. 查能力、数据和算子

```powershell
python -m research_pipeline capabilities --format json
python -m research_pipeline operator list --format json
python -m research_pipeline artifact describe data.columnar-bundle.v1 --format json
python -m research_pipeline recipe list --format json
python -m research_pipeline catalog dataset search daily --catalog-lock <持久Catalog-Lock目录> --format json
```

`planned` 只能发现，不能运行。注册表已有同语义算子时直接复用。保存搜索结果中的
`catalog_context.lock_reference`；dataset 命中项的 `bindings` 会列出 `source_profile`、
`environment` 和物理对象。后续按 `next_commands` 填入每个 profile 的只读数据库路径，不猜路径。

## 3. 创建当前 ResearchPackage

当前公共 Recipe 清单为空，从中性声明草稿开始：

```powershell
python -m research_pipeline package init work/my_research --json
```

生成的四份 YAML 还不是正式 ResearchPackage。填写研究身份、来源与市场适配、指标与结论上限、数据请求、时点、图节点和结果表之后再准入；未填的草稿可以运行 `lint` 查看当前阻断字段，不能 `admit`。项目扩展也走同一套 lint、admit、run 和 verify。

## 4. 一次 lint 看完本地缺口

```powershell
python -m research_pipeline package lint --package work/my_research --catalog-lock <搜索结果.catalog_context.lock_reference> --json
```

完整包的 lint 会编译 QueryIR 和 typed operator graph，检查 ResultSpec、指标可达性、来源声明，并报告 OperatorDefinition 已声明的资源预算；草稿 lint 会指出当前阻断的声明字段。它不会打开研究数据库，也不会授予运行资格。

如果需要项目算子，先创建一个不存在的新目录：

```powershell
python -m research_pipeline operator scaffold --output work/my_operator --project-id my-research --operator-id project.my-operator --format json
```

按返回的 `next_commands` 完成 `operator validate/build`，再在 lint 和 admit 中提供 bundle；admit
后 bundle 闭包进入不可变计划。scaffold 是 typed identity 示例，必须替换为真实算法并补贴身测试，
不能把示例输出当成研究证据。

## 5. 用显式只读数据源正式准入

```powershell
python -m research_pipeline package admit `
  --package work/my_research `
  --catalog-lock <搜索结果.catalog_context.lock_reference> `
  --data-db <source:prod只读DuckDB> `
  --output work/plan `
  --json
```

admit 自动从 package requests 和 Catalog Lock 解析 binding，观察当前 schema，形成漂移与 PIT 闭包，并核对数据库 size/mtime 前后不变。失败时按结构化缺失项修 package、Catalog 或数据源，不手写内部 hash。

## 6. 运行和恢复

```powershell
python -m research_pipeline run --plan work/plan --data-db <只读DuckDB> --artifact-root work/artifacts --handoff-out work/handoff.json --run-root work/run --result-store work/results --clock 2026-01-01T00:00:00+08:00 --root-seed 0 --json
python -m research_pipeline inspect --run-root work/run --json
```

- 进程中断：`resume --run-root work/run --json`
- 可重试节点失败：`retry-node --run-root work/run --node <node> --json`
- 终态 run 从某节点形成新子 run：`rerun-from --run-root work/run --output-run-root work/child-run --node <node> --json`
- package、Catalog、数据修订、实现、clock 或 seed 变化：重新 admit，再新建 run。

## 7. 生成和消费可信结果

Runtime 成功后发布自包含 Result。随后独立验证：

```powershell
python -m research_pipeline verify --result <Result目录> --result-store work/results --output work/verification-result.json --json
python -m research_pipeline report --verification-result work/verification-result.json --result-store work/results --json
```

`compare`、`export-result` 和 Dashboard 同样只消费 VerificationResult 与 ResultStore。`export-result` 不重新执行研究。删除 run-root 后，这些消费仍应成立。

## 常见失败怎么改

| 失败 | 含义 | 下一步 |
| --- | --- | --- |
| package schema 或算子端口错 | 声明不闭合 | 修改 package 后重新 lint |
| Catalog/binding/漂移/PIT 失败 | 数据在决定时点不可证明可用 | 修 Catalog、来源或查询，再 admit |
| bundle 身份变化 | 实现已经变化 | 重新 build、lint、admit，新 run |
| checkpoint 身份变化 | 旧结果不可安全复用 | 新 run，不能手改状态 |
| verifier gate 失败 | 当前结论上限不成立 | 修对应数据/算法或降低 claim 后新 run |


## 从完整研究起点开始

| 研究目的 | 起点 | 需要明确的研究事实 |
| --- | --- | --- |
| 按材料复现固定公式 | [公开公式示例](../integrations/rdagent/examples/volume_concentration/README.md) | 公式、输入字段、窗口、缺失规则和独立参考；真实材料由研究者确认 |
| 让 RD-Agent 执行参数研究 | [研究包循环](../integrations/rdagent/examples/package_campaign/README.md) | 基包、允许变体、开发范围、评价字段、预算与停止条件 |
| 日频特征与 Qlib 模型 | [Qlib 研究起点](../examples/qlib_portfolio/README.md) | 特征和标签可见时间、候选模型、时间切分与研究用途 |
| 预测到组合、成交和成本 | [Qlib 组合起点](../examples/qlib_portfolio/README.md)的 `portfolio` 模式 | 固定 test 预测排名规则、目标权重、次会话开盘成交、费用、TCA 与独立金融复核 |

可运行的公式、参数循环与模型起点自带公开合成输入。先在仓库外输出目录得到正式结果，再改动自己的研究参数；换成真实数据时重新声明数据可见性、来源和适用市场。合成例的验证结论仅证明声明的计算关系，不代表真实市场效果。

公式复现与探索研究分别使用对应入口：固定公式根据独立参考判断是否复现正确；探索研究可以改变获准参数，但必须保存每个候选及失败记录。新增算法仍放项目扩展，不能通过修改框架核心把某个项目写成所有研究的默认规则。
