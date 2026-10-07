"""Linux RD 到 Windows RP 的串行参数数组桥接。"""
import json
import subprocess
from pathlib import Path
from .contracts import write_json
from .feedback import project_feedback


class RPExecutionBridge:
    def __init__(self, request):
        self.request = request
        request.freeze()

    def candidate(self, source_text):
        root = self.request.session_root / "candidates"
        root.mkdir(exist_ok=True)
        for entry in sorted(root.glob("candidate-*")):
            source = entry / "compute.py"
            if source.is_file() and source.read_text(encoding="utf-8") == source_text:
                if not (entry / "candidate.json").exists():
                    self._write_candidate(entry)
                return entry.name
        index = len(tuple(root.glob("candidate-*")))
        if index >= self.request.payload["budget"]["coder_attempts"]:
            raise RuntimeError("持久候选次数已耗尽")
        entry = root / f"candidate-{index:04d}"
        entry.mkdir()
        (entry / "compute.py").write_text(source_text, encoding="utf-8")
        self._write_candidate(entry)
        return entry.name

    def _write_candidate(self, entry):
        write_json(entry / "candidate.json", {"candidate_id": entry.name, "allocation_label":
                   f"{self.request.payload['request_id']}:{entry.name}", "evaluation_scope": self.request.payload["development_scope"]})

    def evaluate(self, candidate_id):
        self.request.freeze()
        binding = self.request.payload["runtime_binding"]
        # 固定入口只接收请求与候选，不接受生成代码定义执行命令。
        bootstrap = ("import os,runpy,sys;from pathlib import Path;root=Path(sys.argv[1]);"
                     "root=root if (root/'src/research_pipeline').is_dir() else root/'research_pipeline';"
                     "paths=[str(root/'src'),str(root/'integrations/rdagent/src')];sys.path[:0]=paths;"
                     "os.environ['PYTHONPATH']=os.pathsep.join(paths);os.environ['PYTHONUTF8']='1';"
                     "os.makedirs(sys.argv[2],exist_ok=True);os.environ['TEMP']=sys.argv[2];os.environ['TMP']=sys.argv[2];"
                     "sys.argv=['quantwitness_rdagent.worker'];runpy.run_module('quantwitness_rdagent.worker',run_name='__main__')")
        python = binding["windows_python"]
        if len(python) > 2 and python[1] == ":":
            python = "/mnt/" + python[0].lower() + python[2:].replace("\\", "/")
        repo = binding["windows_repo"].replace("\\", "/").rstrip("/")
        argv = [python, "-X", "utf8", "-c", bootstrap, repo,
                binding["windows_session_root"].replace("\\", "/") + "/scratch"]
        result = subprocess.run(argv, input=json.dumps({"request": self.request.payload, "candidate_id": candidate_id}),
                                text=True, encoding="utf-8", capture_output=True, check=False)
        if result.returncode != 0:
            raise RuntimeError(f"RP 执行桥中断，退出码 {result.returncode}: {result.stderr[-2000:]}")
        receipt = self.request.session_root / "candidates" / candidate_id / "feedback.json"
        if not receipt.is_file():
            raise RuntimeError(f"RP 执行桥未留下反馈，退出码 {result.returncode}: {result.stderr[-2000:]}")
        evidence = json.loads(receipt.read_text(encoding="utf-8"))
        return project_feedback(self.request, candidate_id, evidence)
