# 项目扩展

项目 extension 用于承载不属于通用框架的 Python 算法。它通过薄声明冻结身份、端口、依赖和资源约束，再由 `operator validate` 与 `operator build` 生成当前 ResearchPackage 使用的 bundle。

```powershell
python -m research_pipeline operator validate --spec <声明.yaml> --source <源码目录> --format json
python -m research_pipeline operator build --spec <声明.yaml> --source <源码目录> --output <bundle父目录> --format json
```

扩展源码目录是唯一源码闭包，不扫描相邻项目，不自动安装依赖，也不能导入 `research_pipeline` 或其他仓库根包。扩展只返回声明允许的输出，正式 Feature/Label 的时间事实由核心根据已准入输入附加。

项目算法只有在至少两个异构真实项目无需修改源码即可复用，并具备独立 oracle、前视/标签/边界攻击测试和人工批准时，才有资格提取最小通用原语进入框架核心。

普通 `operator list/describe` 和安装后运行不需要私有研究目录。晋级时由审查者向专用审查入口显式提供项目 ID 到目录的映射及测试证据根；审查会编译正式声明、核对不同拓扑与目标算子的消费，并执行引用的 pytest 节点。目录可在公开仓库外，私有项目真实性和测试覆盖的金融语义须由人工线下核实。运行可能写库的证据测试前须另行获得写库批准，不能以晋级审查代替批准。当前没有新增公共算子的批准晋级记录。

刷新整改基线时使用 `python research_pipeline/tools/capability_containment_gate.py --mode snapshot --promotion-project <项目ID>=<项目目录> --promotion-project <另一项目ID>=<另一项目目录> --promotion-evidence-root <测试目录>`。只有在存在 approved 晋级记录时才要求这些显式参数；普通 `check` 不加载项目证据，也不接受晋级证据参数。该命令只输出快照，不自动发布或修改基线文件。

完整 ABI、输出 writer、state 和 `causal_plan` 约束见 [项目扩展合同](project_extensions/README.md)。
