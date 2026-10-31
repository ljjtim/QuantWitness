"""分钟数据与复权测试使用的独立观察链夹具。"""

from pathlib import Path

import yaml


def prepare_observation_package(root: Path) -> None:
    spec_path = root / "spec/research.yaml"
    payload = yaml.safe_load(spec_path.read_text(encoding="utf-8"))
    nodes = payload["graph"]["nodes"]
    removed = {node["node_id"] for node in nodes
               if node["operator_id"].startswith("finance.simulation.")}
    while True:
        dependent = {node["node_id"] for node in nodes
                     if any(port["source_node_id"] in removed
                            for port in node.get("inputs", []))}
        if dependent <= removed:
            break
        removed |= dependent
    payload["graph"]["nodes"] = [node for node in nodes if node["node_id"] not in removed]
    payload["result"]["tables"] = [table for table in payload["result"]["tables"]
                                     if table["source_node_id"] not in removed]
    if not any(table["role"] == "primary" for table in payload["result"]["tables"]):
        payload["graph"]["nodes"].append({
            "node_id": "observe_stock", "operator_id": "research.observation.minute-bars",
            "operator_version": "1.0.0", "strategies": [],
            "inputs": [{"input_port": "bars", "source_node_id": "bars_1m",
                        "source_output_port": "bars"}],
            "parameters": {"request_id": "stock_minute"},
        })
        payload["result"]["tables"].append({
            "table_id": "stock_observation", "role": "primary",
            "source_node_id": "observe_stock", "source_port": "observation",
            "artifact_type": "research.minute-observation.v1",
            "schema_id": "research.minute-observation.metrics.v1", "path_prefix": "observation",
        })
        package_path = root / "package.yaml"
        package = yaml.safe_load(package_path.read_text(encoding="utf-8"))
        package["metric_contract"]["metrics"] = ["minute.row_count@1.0.0"]
        package["metric_contract"]["semantics"] = {"minute.row_count@1.0.0": "已验证分钟分区行数"}
        package_path.write_text(yaml.safe_dump(package, allow_unicode=True, sort_keys=False), encoding="utf-8")
    spec_path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")
