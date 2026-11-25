"""只读核对正式Result八表与固定手算预期，不执行策略或撮合。"""
import argparse
from datetime import datetime
from decimal import Decimal
import json
from pathlib import Path
import pyarrow.parquet as pq
import yaml
from research_pipeline.platform.lean_contract import LEAN_SCHEMA_IDS
HERE=Path(__file__).resolve().parent
DAYS=["2024-01-08","2024-01-09","2024-01-10","2024-01-11"]


def require(condition,message):
    if not condition:
        raise ValueError(message)


def number(value):
    return Decimal(str(value))


def payloads(root,manifest,name):
    selected=[item for item in manifest["tables"] if item["schema_id"]==LEAN_SCHEMA_IDS[name]]
    require(len(selected)==1,"Result须恰好包含一张"+name+"表")
    result=[]
    for relative in sorted(selected[0]["files"]):
        result.extend(pq.read_table(root/relative).to_pylist())
    require(bool(result) or name in {"intents", "fills", "positions", "account_events", "lifecycle"},name+"表为空")
    return result


def verify_history(root, manifest, expected):
    support_files = {item["source_path"]: item["relative_path"] for item in manifest["support_files"]}
    for observation in expected:
        day = observation["at"][:10]
        filename = f"lean/storage/quickstart/history-{day}.json"
        require(filename in support_files, f"缺少 {day} 的 History 记录")
        actual = json.loads((root / support_files[filename]).read_text(encoding="utf-8"))
        require(actual["at"] == observation["at"], f"{day} 的 History 请求时点不匹配")
        require(list(map(number, actual["close"])) == list(map(number, observation["close"])),
                f"{day} 的 History 收盘价不匹配")


def verify(root,frequency):
    root=Path(root).resolve()
    require((root/"COMMITTED").is_file(),"缺少正式Result提交标记")
    manifest=json.loads((root/"result.json").read_text(encoding="utf-8"))
    expected=yaml.safe_load((HERE/"expected.yaml").read_text(encoding="utf-8"))
    case=expected["frequencies"][frequency]
    tables={name:payloads(root,manifest,name) for name in LEAN_SCHEMA_IDS}
    require(not tables["intents"], "原生算法不能生成虚构策略意图")
    fills=[row for row in tables["orders"] if number(row["quantity"])!=0]
    require(len(fills)==2,"必须恰好买入、卖出各一次")
    for actual,wanted in zip(fills,case["fills"]):
        require(actual["instrument_id"]==expected["instrument_id"],"成交证券不匹配")
        for key in ("submitted_at","at"):
            require(actual[key]==datetime.fromisoformat(wanted[key]),"成交时点不匹配："+key)
        for key in ("quantity","price","fee","cash"):
            require(number(actual[key])==number(wanted[key]),"成交金额不匹配："+key)
    require(len({row["order_id"] for row in tables["orders"]})==2,"订单数必须为2")
    require(sum(number(row["fee"]) for row in fills)==number(case["fee_total"]),"总费用不匹配")
    daily=tables["daily"]
    require([row["date"].isoformat() for row in daily]==DAYS,"日终会话不完整")
    require([number(row["cash"]) for row in daily]==list(map(number,case["daily_cash"])),"日终现金不匹配")
    require(number(daily[-1]["equity"])==number(case["final_equity"]),"期末权益不匹配")
    require(not daily[-1]["positions"],"期末持仓必须为空")
    require(all(row["active_orders"]==0 for row in daily),"日终存在未完成订单")
    requests = [row["payload"] for row in tables["trace"] if row["kind"] == "order_request"
                and row["payload"]["request_type"] == "Submit"]
    submitted = {row["order_id"]: row for row in requests}
    require(len(submitted) == 2, "轨迹须保留两笔原生提交请求")
    require(sorted(row["quantity"] for row in submitted.values()) == [-100, 100], "原生申报数量不匹配")
    require(len(tables["fills"]) == 2, "实际成交表必须保存两笔成交")
    require(bool(tables["account_events"]), "缺少真实账户事件")
    verify_history(root, manifest, case["history"])
    return dict(result_id=manifest["result_id"],frequency=frequency,fill_count=2,fee_total=case["fee_total"],final_equity=case["final_equity"])

if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--result",required=True,type=Path)
    parser.add_argument("--frequency",required=True,choices=("1d","1m"))
    args=parser.parse_args()
    print(json.dumps(verify(args.result,args.frequency),ensure_ascii=False,indent=2))
