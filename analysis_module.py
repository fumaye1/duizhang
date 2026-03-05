from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class PriceRule:
    weight_upper: float
    price: float


def parse_price_rules(config_df: pd.DataFrame) -> List[PriceRule]:
    """Parse ConfigPanel input into sorted price rules.

    Expected columns:
    - 重量上限(kg)
    - 运营交割价(元)

    Returns a list sorted by weight_upper ascending.
    """

    if config_df is None or config_df.empty:
        return []

    df = config_df.copy()
    required = ["重量上限(kg)", "运营交割价(元)"]
    for col in required:
        if col not in df.columns:
            return []

    df["重量上限(kg)"] = pd.to_numeric(df["重量上限(kg)"], errors="coerce")
    df["运营交割价(元)"] = pd.to_numeric(df["运营交割价(元)"], errors="coerce")
    df = df.dropna(subset=["重量上限(kg)", "运营交割价(元)"])
    if df.empty:
        return []

    df = df.sort_values("重量上限(kg)").drop_duplicates(subset=["重量上限(kg)"], keep="last")
    rules = [
        PriceRule(weight_upper=float(r["重量上限(kg)"]), price=float(r["运营交割价(元)"]))
        for _, r in df.iterrows()
    ]
    return rules


def lookup_price(weight: float, rules: Sequence[PriceRule]) -> Optional[float]:
    if not rules or weight is None or pd.isna(weight):
        return None
    w = float(weight)
    for r in rules:
        if w <= r.weight_upper:
            return r.price
    return rules[-1].price


def compute_province_pivot(
    df: pd.DataFrame,
    province_col: str,
    weight_col: str,
    order_col: str,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    work = df.copy()
    work[province_col] = work[province_col].astype(str)
    work[weight_col] = pd.to_numeric(work[weight_col], errors="coerce")

    pivot_count = pd.pivot_table(
        work,
        index=province_col,
        columns=weight_col,
        values=order_col,
        aggfunc="count",
        fill_value=0,
    )
    pivot_count = pivot_count.sort_index(axis=1)
    pivot_count["合计"] = pivot_count.sum(axis=1)

    total = float(pivot_count["合计"].sum()) if not pivot_count.empty else 0.0
    if total <= 0:
        pivot_share = pivot_count.copy()
        pivot_share.loc[:, :] = 0.0
    else:
        pivot_share = pivot_count.div(total)

    pivot_count = pivot_count.reset_index()
    pivot_share = pivot_share.reset_index()
    return pivot_count, pivot_share


def compute_top_skus(
    df: pd.DataFrame,
    sku_col: str,
    weight_col: str,
    fee_col: str,
    order_col: str,
    rules: Sequence[PriceRule],
    top_n: int = 50,
) -> pd.DataFrame:
    work = df.copy()
    work[sku_col] = work[sku_col].astype(str)
    work[order_col] = work[order_col].astype(str)
    work[weight_col] = pd.to_numeric(work[weight_col], errors="coerce")
    work[fee_col] = pd.to_numeric(work[fee_col], errors="coerce").fillna(0)

    grouped = (
        work.groupby(sku_col, dropna=False)
        .agg(
            单量=(order_col, "count"),
            快递费总额=(fee_col, "sum"),
            最常见重量=(weight_col, lambda s: s.mode().iloc[0] if not s.mode().empty else np.nan),
        )
        .reset_index()
    )
    grouped["平均单价"] = grouped["快递费总额"] / grouped["单量"].replace(0, np.nan)

    if rules:
        grouped["运营交割价"] = grouped["最常见重量"].apply(lambda w: lookup_price(w, rules))
        grouped["单价差(平均-交割)"] = grouped["平均单价"] - pd.to_numeric(grouped["运营交割价"], errors="coerce")
        grouped["总差(平均-交割)"] = grouped["快递费总额"] - pd.to_numeric(grouped["运营交割价"], errors="coerce") * grouped["单量"]
    else:
        grouped["运营交割价"] = np.nan
        grouped["单价差(平均-交割)"] = np.nan
        grouped["总差(平均-交割)"] = np.nan

    grouped = grouped.sort_values(["快递费总额", "单量"], ascending=[False, False]).head(int(top_n))
    return grouped


def compute_weight_price_table(
    df: pd.DataFrame,
    weight_col: str,
    fee_col: str,
    order_col: str,
    rules: Sequence[PriceRule],
) -> pd.DataFrame:
    work = df.copy()
    work[order_col] = work[order_col].astype(str)
    work[weight_col] = pd.to_numeric(work[weight_col], errors="coerce")
    work[fee_col] = pd.to_numeric(work[fee_col], errors="coerce").fillna(0)

    grouped = (
        work.groupby(weight_col, dropna=False)
        .agg(
            单量=(order_col, "count"),
            快递费汇总=(fee_col, "sum"),
        )
        .reset_index()
        .rename(columns={weight_col: "结算重量"})
    )
    grouped["平均单价"] = grouped["快递费汇总"] / grouped["单量"].replace(0, np.nan)

    if rules:
        grouped["运营交割价"] = grouped["结算重量"].apply(lambda w: lookup_price(w, rules))
        grouped["单价差(平均-交割)"] = grouped["平均单价"] - pd.to_numeric(grouped["运营交割价"], errors="coerce")
        grouped["总差(平均-交割)"] = grouped["快递费汇总"] - pd.to_numeric(grouped["运营交割价"], errors="coerce") * grouped["单量"]
    else:
        grouped["运营交割价"] = np.nan
        grouped["单价差(平均-交割)"] = np.nan
        grouped["总差(平均-交割)"] = np.nan

    grouped = grouped.sort_values("结算重量", ascending=True)
    return grouped
