"""只读核对正式Result四表与固定手算预期，不执行策略或撮合。"""
import argparse
from datetime import datetime
from decimal import Decimal
import json
from pathlib import Path
import pyarrow.parquet as pq
import yaml
HERE=Path(__file__).resolve().parent
DAYS=["2024-01-08","2024-01-09","2024-01-10","2024-01-11"]


def require(condition,message):
    if not condition:
        raise ValueError(message)


def number(value):
    return Decimal(str(value))


def payloads(root,manifest,name):
    selected=[item for item in manifest["tables"] if item["schema_id"]=="research.lean."+name+".v1"]
    require(len(selected)==1,"Result须恰好包含一张"+name+"表")
    result=[]
    for relative in sorted(selected[0]["files"]):
        table=pq.ParquetFile(root/relative).read(columns=["payload_json"])
        result.extend(json.loads(value) for value in table["payload_json"].to_pylist())
    require(bool(result),name+"表为空")
    return result


def verify(root,frequency):
    root=Path(root).resolve()
    require((root/"COMMITTED").is_file(),"缺少正式Result提交标记")
    manifest=json.loads((root/"result.json").read_text(encoding="utf-8"))
    expected=yaml.safe_load((HERE/"expected.yaml").read_text(encoding="utf-8"))
    case=expected["frequencies"][frequency]
    tables={name:payloads(root,manifest,name) for name in ("orders","intents","daily","trace")}
    fills=[row for row in tables["orders"] if number(row["quantity"])!=0]
    require(len(fills)==2,"必须恰好买入、卖出各一次")
    for actual,wanted in zip(fills,case["fills"]):
        require(actual["instrument_id"]==expected["instrument_id"],"成交证券不匹配")
        for key in ("submitted_at","at"):
            require(datetime.fromisoformat(actual[key])==datetime.fromisoformat(wanted[key]),"成交时点不匹配："+key)
        for key in ("quantity","price","fee","cash"):
            require(number(actual[key])==number(wanted[key]),"成交金额不匹配："+key)
    require(len({row["order_id"] for row in tables["orders"]})==2,"订单数必须为2")
    require(sum(number(row["fee"]) for row in fills)==number(case["fee_total"]),"总费用不匹配")
    daily=tables["daily"]
    require([row["date"] for row in daily]==DAYS,"日终会话不完整")
    require([number(row["cash"]) for row in daily]==list(map(number,case["daily_cash"])),"日终现金不匹配")
    require(number(daily[-1]["equity"])==number(case["final_equity"]),"期末权益不匹配")
    require(not daily[-1]["positions"],"期末持仓必须为空")
    require(all(row["active_orders"]==0 for row in daily),"日终存在未完成订单")
    intents=tables["intents"]
    require(len(intents)==2,"意图数必须为2")
    require([row["intent"]["side"] for row in intents]==["buy","sell"],"买卖意图顺序不匹配")
    require(all(row["intent"]["quantity"]==100 for row in intents),"意图数量不匹配")
    decisions=[row for row in tables["trace"] if row["response"]["intents"]]
    require(len(decisions)==2,"轨迹须恰好包含两次下单决定")
    for row,wanted in zip(decisions,case["fills"]):
        event=row["event"]
        require(event["kind"]=="bar","下单必须来自bar事件")
        require(datetime.fromisoformat(event["decision_time"])==datetime.fromisoformat(wanted["submitted_at"]),"轨迹决定时点不匹配")
        code=expected["instrument_id"]
        require(datetime.fromisoformat(row["quotes"][code]["available_at"])<=datetime.fromisoformat(event["decision_time"]),"决定使用未来报价")
    sell_positions=decisions[1]["event"]["account"]["positions"]
    require(any(row["instrument"]["instrument_id"]==expected["instrument_id"] and row["sellable_quantity"]>=100 for row in sell_positions),"卖出时T+1可卖量不足")
    return dict(result_id=manifest["result_id"],frequency=frequency,fill_count=2,fee_total=case["fee_total"],final_equity=case["final_equity"])

if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--result",required=True,type=Path)
    parser.add_argument("--frequency",required=True,choices=("1d","1m"))
    args=parser.parse_args()
    print(json.dumps(verify(args.result,args.frequency),ensure_ascii=False,indent=2))
