"""Approved local-only continuation of the 20-interval bracket staging job."""
from decimal import Decimal
from pathlib import Path
import json

from futures_rebuild.tier1_bracket_source_publisher import stage_indexed_bracket_market_year

ROOT = Path(__file__).resolve().parents[2]
INDEX = "a9656ec52912cd45bab11d7209e2a9ebc6728b9a4749af0006975b863459ff7d"
AUDIT = "efb8943fccb79b12b43f603e5b2078b847a38d868aa79fd6e1ab15fa35a638d5"
SIGNAL = "dfd98a3a427191694c967c06a62f76e780b61a28bfbb301bd90671fdb1da70cb"
CAUSAL = {
    ("ES", 2019): "8435e869afa40b5ba9173bd6cbf3512a3747fe633b15554c09f6fc9fbc03c5fc",
    ("ES", 2020): "f77cdf691ca4c0c9ea93bb58ac8b6c54de23d2042a20d1529066026f349ac264",
    ("ES", 2021): "9c0b5c38eb20d8addbf2a4f934af04a75e9556acd0db11f3724a59124b75bfad",
    ("ES", 2022): "418715a95666c0a07bf736f554828d90846775b0a661ddc2871da9635c947868",
    ("CL", 2019): "478f4fd8d87a986e9ec3c1d19298d0772eef3c760f851c8ea593d72b257ff3ed",
    ("CL", 2020): "0fab1f8fd79cdbd23af70a6482c72c2b42939076d0031468a1d25f4915b02881",
    ("CL", 2021): "04ebdd803002427cab9256ac134715237d05f35d5014ad1e670d333bccf6dcdc",
    ("CL", 2022): "e4dcbcb4eb2948551cc4db18a377740b73036b75b0fb70ced134429fc94c8261",
    ("ZN", 2018): "88562721cd83687e9d3a7a70af74ea63ab716a45c87a3d1641b21fd44240a01f",
    ("ZN", 2019): "0200afd5af8c9ec0bfce9c20a6834be741e5b0fe45b7e873fef72322652a3903",
    ("ZN", 2020): "2cc21e1a442684a194c3a2a289535bd19e1e529e48c6b56804b61950faae8e67",
    ("ZN", 2021): "d4d929750b8f17f41660b92dd25978366e4c9868e1a6f6a9f3c03e32b118e930",
    ("ZN", 2022): "15f0fe11d695819227d04170a5546395a9570498307c1ca54f0e56fbebfd0955",
    ("6E", 2018): "3fa8f15e3d9de7e95ef642ec71727bc83f0d0761df6d53b18fc36e118e5971a1",
    ("6E", 2019): "f1816c54d590b3371736135f3b060949ab223531d0abcbb2afe564f835052ab2",
    ("6E", 2020): "bfde5aab012eff610a34809476c0dc5883bb93502c70bf48abeaba6959899284",
    ("6E", 2021): "ae3eed49a48f70383fcdac7b2b25d666bf42fc4900182e1ea17eb1c9b4666d0a",
    ("6E", 2022): "208477f422daa163111c3b8c8fc0616746d0f1616229a67af992ecf20d2ebb21",
}
COST = {"ES": Decimal("53.10"), "CL": Decimal("83.34"), "ZN": Decimal("67.50"), "6E": Decimal("28.54")}
LOG = ROOT / "state" / "tier1_bracket_staging" / "remaining_progress.jsonl"

for (market, year), causal_id in CAUSAL.items():
    result = stage_indexed_bracket_market_year(
        root=ROOT, phase8_index_release_id=INDEX, audit_receipt_id=AUDIT,
        causal_release_id=causal_id, signal_contract_id=SIGNAL,
        stress_round_trip_cost_usd=COST[market],
    )
    checkpoint = result["checkpoint_payload"]
    if not checkpoint["complete"] or checkpoint["input_rows"] != checkpoint["output_rows"]:
        raise RuntimeError(f"checkpoint did not complete: {market} {year}")
    with LOG.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps({"market": market, "year": year, "input_rows": checkpoint["input_rows"], "output_rows": checkpoint["output_rows"]}, sort_keys=True) + "\n")
