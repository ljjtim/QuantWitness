# 研究系列与最终留出集

最终留出集用于一次确认。同一研究问题调整窗口、候选、seed、特征、标签期限或数据版本时，沿用同一个研究系列。系列账本记录已经打开的最终评价区间；新方案与既往区间重叠时，框架在读取目标值前拒绝。

## 声明与目录

正式 `research.model.locked-holdout` 节点必须提供 `research_series_json` 字符串，包含完整 JSON 声明：

```json
{
  "series_id": "etf-return-prediction",
  "root": "I:/research/holdout-series",
  "purpose": "确认ETF价格特征对下一会话收益的预测能力",
  "legacy_ledger_roots": []
}
```

`series_id` 使用字母或数字开头的简短目录标识，可包含点、下划线与连字符。`root` 和所有旧账本根使用绝对路径。持久目录为 `root/series_id/`，其中 `registration.json` 保存完整声明及带时区的实际登记时间，`ledgers/` 保存每次最终评价的四阶段消费记录与系列证据。登记时间是系统操作时间，与历史研究的 `fixed_clock` 分别解释。

Root 应放在需要长期保留的项目记录目录中。不同 output/run 共享该 Root 与系列 ID；恢复沿用原计划声明。已登记的研究问题和旧账本列表不能通过后续调用替换。研究包的 lint/admit 检查声明，运行时登记和打开账本。具体变体仍有独立包、计划、模型及样本身份。

公开 Qlib 示例使用以下参数进入最终评价：

```powershell
python examples/qlib_portfolio/run.py --mode model --output I:/research/etf-final --research-series-id etf-return-prediction --research-series-root I:/research/holdout-series --research-series-purpose "确认ETF价格特征对下一会话收益的预测能力"
```

开发研究使用 `--mode development`，不打开最终留出账本。多窗口、多候选和多期限比较在开发区完成，选定方案后进行一次最终确认。

## 打开与消费

冻结最终评价计划时，框架读取本系列和显式登记旧账本中的已打开区间，将真实暴露写入 `exposed_intervals`。原冻结暴露先随账本持久保存，再发布计划。拟合阶段中断后恢复会复用原计划和原快照；期间新增的不重叠暴露仅追加到打开证据，不改变原计划身份。实际打开前，在系列级进程锁内重读这些事实、检查与本次留出集的重叠，再原子记录 `opened`。锁释放后才能读取留出集值。不同变体并发时，共享同一把锁。

重叠按日期闭区间判定，共享一个边界日期也拒绝。既往暴露不因证券子集、数据版本或变体身份变化而清零。完全不重叠的未来区间可以进行下一次确认。

`frozen` 和 `prepared` 未打开数据，不计为暴露，但其原计划与恢复限制继续有效。`opened` 即消耗资格，即使读取前进程退出也不返还；读取或计算失败进入 `consumed_failed`。已有成功节点通过原 checkpoint 恢复或复用，不能重新打开相同留出集。

## 旧账本与恢复

已有研究在首次登记时，通过 `legacy_ledger_roots` 列出对应旧账本目录；公开示例可以重复传入 `--research-series-legacy-root`。框架只读核对其计划和状态链，并将所有已打开区间纳入系列。缺失、损坏或声明漂移会阻止最终评价。

历史 Result 和账本内容保持原样。旧账本没有系列字段时仍可按其原合同复核；新的正式模型最终评价必须有完整系列声明与证据。旧 checkpoint 不因登记系列而自动获得新实现的恢复许可，已消费节点不能通过新运行再次执行。

Workspace 的原 execution 接续使用原冻结包、计划和系列声明。修改系列参数会改变执行身份，不能在原执行上更换消费边界。系列目录与研究交付物都应长期保存。

## 独立验证

Result 除 plan/prepared/opened/terminal 外，封存 `holdout-ledger/series.json`。它包含系列声明、登记事实、冻结时和打开前实际观察到的旧账本及其状态链。打开前可新增不重叠暴露；冻结时已存在的打开事实不能丢失，已确认终态不能改变。

独立 Verifier 从这些封存文件重建父研究目的身份、消费记录、日期区间和重叠结论，不读取当前工作目录，也不调用生产端系列判断。验证算法为 `verifier.model-search-holdout.v3`。文件完整性、PIT、原始样本与模型金融验证继续执行。

## 已知边界

系列约束作用于声明的持久工作区。另建目录、漏列历史账本或把同一问题人为另起名称，不能由本地框架自动识别。原始数据提供给操作者后，框架也无法撤销已经获知的信息。跨证券、期限和数据版本仍按日期隔离，是对最终评价信息复用采取的保守取舍。
