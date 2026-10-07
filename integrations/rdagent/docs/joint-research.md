# 因子与模型联合研究

`joint-run` 将已确认因子基线、新因子表达式与生成式模型放入同一个有预算的开发会话。固定版本 RD-Agent 的 LoopBase 调度各轮，动作模板选择因子或模型方向；两类提案、正式 Workspace 评价和反思沿用已有组件。

## 请求与运行

请求合同为 `rd-joint-research-v1`，必须声明 campaign_id、session_root、factor_request、model_request、budget、proposer。factor_request 与 model_request 沿用各自公开请求合同。因子 confirmed_spec 支持 synthetic 或 confirmed_formula；模型 confirmed_spec 指向已确认研究定义文本。两类请求必须共用 package_template 的 source、development 和 objective，支持真实冻结归档，不连接文件数据库。因子基准复现仍须与原文确认公式一致。

统一 budget 包含 rounds、evaluations、model_calls、output_tokens、max_output_tokens_per_call。两分支共用 proposer、调用收据及 token 预约预算；模型 repairs 在 model_request 中声明，修复调用消耗统一预算。evaluations 在正式评价启动前逐轮预约，失败执行也占用次数；恢复同一实验复用预约。

三轮“基线→新因子→模型”无修复时需要9次调用。合成示例预约12次、单次2048 token、总预约24576 token；固定响应按统一实际调用顺序提供，不调用付费模型。真实项目应按提案与结构编码的输出需求显式设置调用上限和总预约量，单次最多8192 token；例如12次、单次8192 token的总预约量为98304 token。live运行沿用项目的单独批准边界。

~~~bash
python integrations/rdagent/examples/joint_research/prepare.py --output /mnt/i/research/joint-demo
python -m quantwitness_rdagent joint-run --request /mnt/i/research/joint-demo/request.json
python -m quantwitness_rdagent joint-inspect --request /mnt/i/research/joint-demo/request.json
python -m quantwitness_rdagent joint-resume --request /mnt/i/research/joint-demo/request.json
~~~

## 方向、特征与知识

第一轮执行已确认因子基线；后续方向选择明确返回 factor、model 或 stop，并引用实际采用的 record_id。因子方向为收盘动量、均价偏离、相对波动和区间位置；模型方向为受支持的生成式前馈网络。方向、假设和反思只接收独立验证通过的开发指标、定义及已有反思。

模型使用当前已验证因子中开发指标最好的表达式，绑定既有 historical_return 特征槽。模型提案保存 factor_record_id 与 factor_expression；正式变体设计、研究身份、Result 和独立验证均覆盖该绑定。标签、处理器、训练设置、开发范围保持冻结。指标相同时保留较早因子。共同评价资格从正式样本、标签时点和 validation 成员逐条绑定到基线；冻结特征缺失或评价成员/标签变化的候选保留正式实验，不进入金融反馈和最佳因子选择。metrics.rows 是指标行数，实际样本数保存于 population-check.json。

rounds/ 保存每轮方向、引用、提案、评价、反思与记录；branches/factor 和 branches/model 保存原分支正式实验。calls/ 保存统一调用预约与响应，已完成请求恢复时复用。原生JSON请求在正文末尾列出本阶段任务约束、字段及只返回JSON对象的要求；不合格式的响应保留原文并停止，不自动增加付费调用。已完成实验沿用原 Workspace 和 Result，未知调用结果停止等待核对。模型编码经验继续使用 model_request.coding_knowledge 来源。


## 正式执行与替代执行恢复

每个候选的 lint、admit、run、verify、report 默认由独立轻量 Python 进程完成。研究进程保存方向、调用账本和阶段状态；计算进程仅接收包、归档输入、Catalog、bundle 和运行预算，不接收模型环境文件或凭据。计算进程沿用正式 Workspace 和 Runtime 的诊断、ResultStore 与独立验证，失败不会生成成功反馈。

候选目录的 `.package-execution.lock` 使用核心已有的进程身份锁。研究父进程中断后，仍存活的计算子进程继续持有它；恢复返回 `wait`，须等待原计算结束再恢复。计算退出后释放锁，异常退出留下的失效锁由同一核心规则处理。

Runtime 推荐 `resume` 或 `retry-node` 时，恢复继续同一 execution。推荐 `readmit-new-run` 时，保留旧执行，通过公共 `workspace execute` 为同一候选重新准入并创建新 execution，使用独立标签，例如 `factor_0001-readmit-1`；包、冻结来源、Catalog、bundle、时钟、种子和预算沿用该实验的声明。

~~~bash
python -m research_pipeline workspace execute --workspace <候选目录>/workspace --label factor_0001-readmit-1 \
  --catalog-lock <冻结Catalog> --source-archive-root <来源归档> \
  --input-snapshot-manifest <冻结输入清单> --verifier-bundle <冻结Verifier> \
  --extension-bundle <冻结算子bundle> --json
~~~

重复传入实验声明中的所有 extension-bundle，并提供原 runtime_options 与验证资源参数。命令完成后，用公开 Python 入口采用该 execution。以联合会话中的单候选 PackageCampaign 为例：

~~~python
import json
from pathlib import Path
from quantwitness_rdagent.package_execution import adopt_package_execution

experiment = Path("<联合会话>/branches/factor/experiments/factor_0001")
request = json.loads((experiment / "request.json").read_text(encoding="utf-8"))
candidate_id = "factor_0001"
source = request["source"]
adoption = adopt_package_execution(
    execution_id="<workspace execute 返回的新execution_id>",
    root=experiment / "candidates" / candidate_id,
    workspace_id=request["campaign_id"] + "-" + candidate_id,
    allocation_label=candidate_id,
    base_package=str(experiment / "packages" / ("candidate_" + candidate_id)),
    source_archive_root=source["source_archive_root"],
    input_snapshot_manifest=source["input_snapshot_manifest"],
    verifier_bundle=source["verifier_bundle"],
    binding=source,
)
~~~

采用入口只接受同一候选 Workspace 中已经生成正式 Result 且独立验证为 pass 的替代 execution。它核对当前冻结包、归档输入、Catalog、算子与 Verifier bundle、时钟、种子、原正式 Plan 和运行资源；身份不符时保留原关联。

成功采用后，`execution-adoption.json` 保存明确的新 ID 与正式 Result 身份，`execution-ref.json` 指向新执行。原引用、诊断及此前采用记录保留在 `execution-history/<旧execution_id>/`，旧 Workspace execution 保持原位。之后执行 `joint-resume`，评价按采用 ID 复用该 Result 及 VerificationResult，不再按原标签选回失败执行；重复采用同一 execution 沿用同一记录。

## 已知限制

价格合同的因子范围是四类已复核的收盘表达式；模型限于生成式前馈网络。联合入口不扩展原文公式解释器和金融口径。每轮仅使用 development 的 validation 指标，最终 test/holdout 不进入方向、假设或反思。固定文本验收证明调度、特征绑定、正式评价及恢复；自主提案质量由真实 live 研究单独评价。

## 已确认日级因子合同

待著而救日级研究显式声明 `factor_contract="dai-daily-v1"`，使用 `$dai` 基线、受限Ref/Mean派生和 `dai_following` 槽；请求字段、方向、共同资格与正式证据见[日级研究指南](dai-daily-research.md)。既有收盘价示例继续使用原合同。

## 固定三轮计划

联合请求可显式声明 `research_schedule=["baseline","factor","model"]`，并设置 `budget.rounds=3`。方向调用分别在已批准的后继因子、模型分支内选择方向和知识引用；不符合计划的响应被拒绝。默认仍自由选择，预算、调用预约与恢复方式不变。计划随请求冻结，恢复不能更换。
