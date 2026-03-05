from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Optional

import pandas as pd


@dataclass(frozen=True)
class BillLineItem:
    project: str
    source_column: str
    multiplier: float = 1.0


def _sum_column(df: pd.DataFrame, col: str) -> float:
    if df.empty or col not in df.columns:
        return 0.0
    series = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return float(series.sum())


def build_summary_bill(
    result_df: pd.DataFrame,
    opening_balance: float,
    line_items: Iterable[BillLineItem],
    prepaid_amount: float,
    opening_project: str = "期初余额",
    prepaid_project: str = "本月预充",
    total_project: str = "总计",
) -> pd.DataFrame:
    """Build a summary bill with columns: 序号, 项目, 金额, 余额.

    Rules:
    - First row is opening balance: 金额=0, 余额=期初余额
    - Middle rows are selected sums: 余额=上一行余额 + 本行金额
    - Prepaid row uses prepaid_amount
    - Total row: 金额=（除期初外所有金额之和）, 余额=预充行余额
    """

    opening_balance = float(opening_balance or 0)
    prepaid_amount = float(prepaid_amount or 0)

    rows: List[dict] = []

    balance = opening_balance
    rows.append({"序号": 1, "项目": opening_project, "金额": 0.0, "余额": balance})

    idx = 1
    transaction_sum = 0.0
    for item in line_items:
        idx += 1
        raw_sum = _sum_column(result_df, item.source_column)
        amount = float(raw_sum) * float(item.multiplier or 0)
        transaction_sum += amount
        balance = balance + amount
        rows.append({"序号": idx, "项目": str(item.project), "金额": amount, "余额": balance})

    idx += 1
    transaction_sum += prepaid_amount
    balance = balance + prepaid_amount
    rows.append({"序号": idx, "项目": prepaid_project, "金额": prepaid_amount, "余额": balance})

    idx += 1
    rows.append({"序号": idx, "项目": total_project, "金额": transaction_sum, "余额": balance})

    out = pd.DataFrame(rows)

    for c in ["金额", "余额"]:
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0).round(2)
    out["序号"] = pd.to_numeric(out["序号"], errors="coerce").fillna(0).astype(int)
    out["项目"] = out["项目"].astype(str)

    return out
