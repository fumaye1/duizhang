from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd


@dataclass
class ReconcileConfig:
    yuncang: str
    erp_type: str
    use_actual_weight: bool
    enable_deductions: bool
    enable_consumables: bool
    clean_province: bool


REQUIRED_COLUMNS = {
    "wdt": ["物流单号", "商家编码", "数量", "收货省份", "发货时间", "快递公司"],
    "yubao": ["物流单号", "货品名称", "数量", "收货地址", "发货时间", "快递公司"],
    "yubao_map": ["货品名称", "商家编码"],
    "maozhong": ["商家编码", "毛重(g)", "箱规"],
    "weight_segments": ["云仓", "重量段结束(kg)"],
    "tariff": [
        "云仓",
        "快递公司",
        "重量段结束(kg)",
        "是否打包品",
        "省份",
        "首重(kg)",
        "首费(元)",
        "续重(kg)",
        "续费(元)",
        "生效开始日期",
        "生效结束日期",
    ],
    "bill": ["物流单号", "计费重量(kg)", "快递费(元)", "云仓"],
    "consumables": ["商家编码", "价格(元)"],
    "tear": ["物流单号"],
    "aftersale": ["物流单号", "赔付金额(元)"],
}


def normalize_province(value: str) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    if not text:
        return ""

    # If a cell accidentally contains multiple addresses separated by '/', keep the first.
    for sep in ["/", "\\", "／"]:
        if sep in text:
            text = text.split(sep, 1)[0].strip()

    # Remove whitespace to improve pattern matching: "北京 北京市 昌平区" -> "北京北京市昌平区"
    text_compact = "".join(str(text).split())

    # Direct-controlled municipalities should not keep the suffix "市".
    municipalities = ["北京", "上海", "天津", "重庆"]
    for name in municipalities:
        if text_compact.startswith(name):
            return name
        if name + "市" in text_compact:
            return name

    text = text_compact
    for suffix in ["自治区", "省", "市"]:
        if suffix in text:
            idx = text.find(suffix)
            return text[: idx + len(suffix)]
    return text


def parse_ship_province(df: pd.DataFrame, erp_type: str) -> pd.Series:
    # Prefer a pre-normalized province column if present.
    if "收货省份" in df.columns:
        return df["收货省份"].astype(str)
    if "收货地址" in df.columns:
        return df["收货地址"].astype(str)
    return pd.Series([""] * len(df))


def safe_to_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def validate_columns(df: pd.DataFrame, required: Iterable[str]) -> List[str]:
    missing = [col for col in required if col not in df.columns]
    return missing


def ensure_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def build_sku_mapping(yubao_map: Optional[pd.DataFrame]) -> Dict[str, str]:
    if yubao_map is None:
        return {}
    return (
        yubao_map.dropna(subset=["货品名称", "商家编码"])
        .set_index("货品名称")["商家编码"]
        .astype(str)
        .to_dict()
    )


def map_yubao_sku(df: pd.DataFrame, sku_map: Dict[str, str]) -> pd.DataFrame:
    df = df.copy()
    df["商家编码"] = df["货品名称"].map(sku_map)
    return df


def compute_maozhong_lookup(df: pd.DataFrame) -> Dict[str, Dict[str, float]]:
    lookup = {}
    for _, row in df.iterrows():
        sku = str(row["商家编码"]).strip()
        lookup[sku] = {
            "毛重(g)": float(row.get("毛重(g)", 0) or 0),
            "箱规": float(row.get("箱规", 0) or 0),
        }
    return lookup


def compute_order_weights(
    detail_df: pd.DataFrame,
    maozhong_lookup: Dict[str, Dict[str, float]],
    use_actual_weight: bool,
) -> pd.DataFrame:
    df = detail_df.copy()
    df["数量"] = ensure_numeric(df["数量"]).fillna(0)
    df["毛重(g)"] = df["商家编码"].map(lambda x: maozhong_lookup.get(str(x), {}).get("毛重(g)", np.nan))
    df["箱规"] = df["商家编码"].map(lambda x: maozhong_lookup.get(str(x), {}).get("箱规", np.nan))

    df["预估重量(kg)"] = df["数量"] * df["毛重(g)"] / 1000

    if use_actual_weight and "实际重量" in df.columns:
        df["实际重量"] = ensure_numeric(df["实际重量"]).fillna(0)
        df["计费重量原始"] = df[["实际重量", "预估重量(kg)"]].max(axis=1)
    else:
        df["计费重量原始"] = df["预估重量(kg)"]

    df["是否打包品"] = df.apply(
        lambda row: is_packed_order(row.get("数量"), row.get("箱规")), axis=1
    )
    return df


def is_packed_order(quantity: float, carton_size: float) -> str:
    if carton_size in [0, np.nan, None]:
        return "打包品"
    if pd.isna(carton_size) or pd.isna(quantity):
        return "打包品"
    if carton_size == 0:
        return "打包品"
    return "打包品" if quantity % carton_size != 0 else "非打包品"


def compute_settlement_weight(
    weight: float, yuncang: str, weight_segments_df: pd.DataFrame
) -> float:
    segments = (
        weight_segments_df[weight_segments_df["云仓"] == yuncang]["重量段结束(kg)"]
        .dropna()
        .sort_values()
        .tolist()
    )
    if not segments:
        return float("nan")
    for seg in segments:
        if weight <= seg:
            return seg
    # If actual weight exceeds the maximum segment, keep the original weight.
    # This enables overweight pricing rules (重量段结束(kg)=0) to kick in.
    return float(weight)


def _coerce_tariff_schema(tariff_df: pd.DataFrame) -> pd.DataFrame:
    """Normalize tariff schema to new fields.

    New schema expects: 首重(kg), 首费(元), 续重(kg), 续费(元).
    For backward compatibility, if 首费(元) is missing but 快递费(元)/快递费 exists,
    use it as 首费(元), and default the rest to 0.
    """

    df = tariff_df.copy()

    if "首费(元)" not in df.columns:
        if "快递费(元)" in df.columns:
            df["首费(元)"] = df["快递费(元)"]
        elif "快递费" in df.columns:
            df["首费(元)"] = df["快递费"]
        elif "首重价格" in df.columns:
            df["首费(元)"] = df["首重价格"]

    for col in ["首重(kg)", "续重(kg)", "续费(元)"]:
        if col not in df.columns:
            df[col] = 0

    for col in ["重量段结束(kg)", "首重(kg)", "首费(元)", "续重(kg)", "续费(元)"]:
        if col in df.columns:
            df[col] = ensure_numeric(df[col])

    return df


def match_tariff(order: pd.Series, tariff_df: pd.DataFrame) -> Optional[pd.Series]:
    """Match tariff by (云仓/快递/是否打包品/省份/生效期) and weight rule.

    Weight logic:
    - Prefer non-zero segments: choose the smallest 重量段结束(kg) >= w
    - If no non-zero segment covers w, fall back to 重量段结束(kg)=0 (overweight rule)
    """

    df = tariff_df
    w = float(order.get("结算重量(取整)") or 0)

    candidates = df[
        (df["云仓"] == order["云仓"])
        & (df["快递公司"] == order["快递公司"])
        & ((df["省份"] == order["收货省份"]) | (df["省份"] == "*"))
        & ((df["是否打包品"] == order["是否打包品"]) | (df["是否打包品"] == "全包"))
        & (df["生效开始日期"] <= order["发货时间"])
        & (df["生效结束日期"] >= order["发货时间"])
    ].copy()

    if candidates.empty:
        return None

    candidates["_province_exact"] = candidates["省份"] == order["收货省份"]
    candidates["_pack_exact"] = candidates["是否打包品"] == order["是否打包品"]
    candidates["_weight_end"] = ensure_numeric(candidates["重量段结束(kg)"]).fillna(0)

    # Try covered segment first (weight_end > 0 and >= w).
    covered = candidates[(candidates["_weight_end"] > 0) & (candidates["_weight_end"] >= w)]
    if not covered.empty:
        min_end = float(covered["_weight_end"].min())
        covered = covered[covered["_weight_end"] == min_end]
        covered = covered.sort_values(
            ["_province_exact", "_pack_exact", "生效开始日期"],
            ascending=[False, False, False],
        )
        return covered.iloc[0]

    # Fall back to overweight rule (weight_end == 0).
    overweight = candidates[candidates["_weight_end"] == 0]
    if overweight.empty:
        return None
    overweight = overweight.sort_values(
        ["_province_exact", "_pack_exact", "生效开始日期"],
        ascending=[False, False, False],
    )
    return overweight.iloc[0]


def calculate_tariff_fee(order: pd.Series, matched: Optional[pd.Series]) -> Tuple[float, str]:
    if matched is None:
        return 0.0, "未匹配资费"

    # New schema.
    if "首费(元)" in matched.index and pd.notna(matched.get("首费(元)")):
        weight_end = float(matched.get("重量段结束(kg)") or 0)
        base_fee = float(matched.get("首费(元)") or 0)
        if weight_end and weight_end > 0:
            return base_fee, ""

        w = float(order.get("结算重量(取整)") or 0)
        base_weight = float(matched.get("首重(kg)") or 0)
        step_weight = float(matched.get("续重(kg)") or 0)
        step_fee = float(matched.get("续费(元)") or 0)

        if w <= base_weight or step_weight <= 0 or step_fee == 0:
            return base_fee, ""

        extra_units = int(np.ceil(max(0.0, w - base_weight) / step_weight))
        return base_fee + extra_units * step_fee, ""

    # Legacy fallback.
    if "快递费(元)" in matched.index and pd.notna(matched.get("快递费(元)")):
        return float(matched.get("快递费(元)")), ""
    if "快递费" in matched.index and pd.notna(matched.get("快递费")):
        return float(matched.get("快递费")), ""
    if "首重价格" in matched.index and "续重价格" in matched.index:
        base = float(matched.get("首重价格") or 0)
        extra = float(matched.get("续重价格") or 0)
        weight = float(order.get("结算重量(取整)") or 0)
        extra_units = max(0, int(np.ceil(weight - 1)))
        return base + extra_units * extra, ""
    return 0.0, "资费缺少价格字段"


def summarize_by(columns: List[str], df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    summary = (
        df.groupby(columns)
        .agg(
            单量=("物流单号", "count"),
            理论运费=("快递费(核算后)", "sum"),
            账单运费=("账单快递费", "sum"),
            差异=("差异金额", "sum"),
        )
        .reset_index()
    )
    return summary


def _ensure_columns(df: pd.DataFrame, cols: Iterable[str], fill_value=np.nan) -> pd.DataFrame:
    df = df.copy()
    for col in cols:
        if col not in df.columns:
            df[col] = fill_value
    return df


def aggregate_bill_df(bill_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate cloud-warehouse bills so multiple files / split bills can be merged safely.

    Strategy:
    - Group by (物流单号, 云仓)
    - Sum fees (快递费(元), 包装费(元))
    - Take max of 计费重量(kg)
    """
    if bill_df is None or bill_df.empty:
        return pd.DataFrame(columns=["物流单号", "云仓", "计费重量(kg)", "快递费(元)", "包装费(元)"])

    df = bill_df.copy()
    df = _ensure_columns(df, ["包装费(元)"])
    df["物流单号"] = df["物流单号"].astype(str)
    df["云仓"] = df["云仓"].astype(str)
    df["计费重量(kg)"] = ensure_numeric(df["计费重量(kg)"])
    df["快递费(元)"] = ensure_numeric(df["快递费(元)"])
    df["包装费(元)"] = ensure_numeric(df["包装费(元)"])

    agg = (
        df.groupby(["物流单号", "云仓"], dropna=False)
        .agg(
            **{
                "计费重量(kg)": ("计费重量(kg)", "max"),
                "快递费(元)": ("快递费(元)", "sum"),
                "包装费(元)": ("包装费(元)", "sum"),
            }
        )
        .reset_index()
    )
    return agg


def reconcile_main(
    detail_df: Optional[pd.DataFrame],
    maozhong_df: Optional[pd.DataFrame],
    weight_segments_df: Optional[pd.DataFrame],
    tariff_df: Optional[pd.DataFrame],
    bill_df: pd.DataFrame,
    config: ReconcileConfig,
    yubao_map_df: Optional[pd.DataFrame] = None,
    consumables_df: Optional[pd.DataFrame] = None,
    tear_df: Optional[pd.DataFrame] = None,
    aftersale_df: Optional[pd.DataFrame] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    bill_agg_df = aggregate_bill_df(bill_df)

    # If there is no detail, still surface bill rows in outputs.
    if detail_df is None or detail_df.empty:
        bill_only = bill_agg_df.copy()
        bill_only["快递公司"] = ""
        bill_only["账单计费重量"] = bill_only["计费重量(kg)"]
        bill_only["账单快递费"] = bill_only["快递费(元)"]
        bill_only["账单包装费"] = bill_only["包装费(元)"]
        bill_only["快递费(核算后)"] = 0.0
        bill_only["差异金额"] = bill_only["账单快递费"].fillna(0)
        bill_only["售后赔付"] = 0.0
        bill_only["应付金额"] = 0.0
        bill_only["备注"] = "发货明细缺失"

        output_columns = [
            "物流单号",
            "商家编码",
            "收货省份",
            "数量",
            "店铺名称",
            "计费重量原始",
            "毛重(g)",
            "结算重量(取整)",
            "快递费(核算后)",
            "耗材费",
            "撕单",
            "售后赔付",
            "账单快递费",
            "差异金额",
            "应付金额",
            "备注",
            "云仓",
            "快递公司",
        ]
        bill_only = _ensure_columns(bill_only, output_columns)
        result_df = bill_only.loc[:, output_columns].copy()
        result_df = result_df.rename(columns={"计费重量原始": "预估重量"})

        exception_df = pd.DataFrame(
            [{"物流单号": str(x), "原因": "发货明细缺失"} for x in result_df["物流单号"].astype(str)]
        )
        summary_df = summarize_by(["云仓", "快递公司"], result_df)
        return result_df, summary_df, exception_df

    if maozhong_df is None or weight_segments_df is None or tariff_df is None:
        raise ValueError("存在发货明细时，毛重表/重量段定义表/资费表为必填")

    df = detail_df.copy()
    df["发货时间"] = safe_to_datetime(df["发货时间"])
    df["快递公司"] = df["快递公司"].astype(str)
    df["物流单号"] = df["物流单号"].astype(str)

    if config.erp_type == "yubao":
        sku_map = build_sku_mapping(yubao_map_df)
        df = map_yubao_sku(df, sku_map)

    df["收货省份"] = parse_ship_province(df, config.erp_type)
    if config.clean_province:
        df["收货省份"] = df["收货省份"].map(normalize_province)

    maozhong_lookup = compute_maozhong_lookup(maozhong_df)
    df = compute_order_weights(df, maozhong_lookup, config.use_actual_weight)
    df["云仓"] = config.yuncang

    df["结算重量(取整)"] = df["计费重量原始"].apply(
        lambda x: compute_settlement_weight(x, config.yuncang, weight_segments_df)
    )

    tariff_df = tariff_df.copy()
    tariff_df = _coerce_tariff_schema(tariff_df)
    tariff_df["生效开始日期"] = safe_to_datetime(tariff_df["生效开始日期"])
    tariff_df["生效结束日期"] = safe_to_datetime(tariff_df["生效结束日期"])

    results = []
    notes = []
    for _, row in df.iterrows():
        matched = match_tariff(row, tariff_df)
        fee, note = calculate_tariff_fee(row, matched)
        results.append(fee)
        notes.append(note)

    df["快递费(核算后)"] = results
    df["备注"] = notes

    if consumables_df is not None and config.enable_consumables:
        cons_map = (
            consumables_df.dropna(subset=["商家编码", "价格(元)"])
            .set_index("商家编码")["价格(元)"]
            .astype(float)
            .to_dict()
        )
        df["耗材费"] = df["商家编码"].map(lambda x: cons_map.get(str(x), 0))
    else:
        df["耗材费"] = 0.0

    df["撕单"] = "否"
    if tear_df is not None and config.enable_deductions:
        tear_set = set(tear_df["物流单号"].astype(str))
        df.loc[df["物流单号"].isin(tear_set), "撕单"] = "是"
        df.loc[df["物流单号"].isin(tear_set), "快递费(核算后)"] = 0.0
        df.loc[df["物流单号"].isin(tear_set), "备注"] = df.loc[
            df["物流单号"].isin(tear_set), "备注"
        ].replace("", "撕单扣减")

    df["售后赔付"] = 0.0
    if aftersale_df is not None and config.enable_deductions:
        pay_map = (
            aftersale_df.dropna(subset=["物流单号", "赔付金额(元)"])
            .set_index("物流单号")["赔付金额(元)"]
            .astype(float)
            .to_dict()
        )
        df["售后赔付"] = df["物流单号"].map(lambda x: pay_map.get(str(x), 0.0))

    # Merge bills onto detail; bills are pre-aggregated to avoid duplicates when multiple files exist.
    df = df.merge(
        bill_agg_df[["物流单号", "计费重量(kg)", "快递费(元)", "包装费(元)", "云仓"]],
        on=["物流单号", "云仓"],
        how="left",
        suffixes=("", "_账单"),
    )
    df.rename(
        columns={
            "计费重量(kg)": "账单计费重量",
            "快递费(元)": "账单快递费",
            "包装费(元)": "账单包装费",
        },
        inplace=True,
    )

    # Append bill rows that have no matching detail.
    detail_keys = df[["物流单号", "云仓"]].drop_duplicates()
    missing_detail_bill = bill_agg_df.merge(
        detail_keys,
        on=["物流单号", "云仓"],
        how="left",
        indicator=True,
    )
    missing_detail_bill = missing_detail_bill[missing_detail_bill["_merge"] == "left_only"].drop(
        columns=["_merge"]
    )
    if not missing_detail_bill.empty:
        extra = missing_detail_bill.copy()
        extra["快递公司"] = ""
        extra["账单计费重量"] = extra["计费重量(kg)"]
        extra["账单快递费"] = extra["快递费(元)"]
        extra["账单包装费"] = extra["包装费(元)"]
        extra["快递费(核算后)"] = 0.0
        extra["备注"] = "发货明细缺失"

        # Keep consistent columns for downstream calculations.
        df = pd.concat([df, extra], ignore_index=True, sort=False)

    df["差异金额"] = df["账单快递费"].fillna(0) - df["快递费(核算后)"].fillna(0)
    df["应付金额"] = df["快递费(核算后)"].fillna(0) - df["售后赔付"].fillna(0)

    exception_rows = []
    if "" in df["备注"].values:
        pass
    for _, row in df.iterrows():
        if row.get("备注"):
            exception_rows.append({"物流单号": row["物流单号"], "原因": row["备注"]})
        if pd.isna(row.get("结算重量(取整)")):
            exception_rows.append({"物流单号": row["物流单号"], "原因": "缺少重量段定义"})
        if pd.isna(row.get("账单快递费")):
            exception_rows.append({"物流单号": row["物流单号"], "原因": "账单缺失"})

    exception_df = pd.DataFrame(exception_rows)

    summary_df = summarize_by(["云仓", "快递公司"], df)

    output_columns = [
        "云仓",
        "物流单号",
        "快递公司",
        "商家编码",
        "收货省份",
        "数量",
        "店铺名称",
        "计费重量原始",
        "毛重(g)",
        "结算重量(取整)",
        "快递费(核算后)",
        "耗材费",
        "撕单",
        "售后赔付",
        "账单计费重量",
        "账单快递费",
        "账单包装费",
        "差异金额",
        "应付金额",
        "备注",
    ]
    for col in output_columns:
        if col not in df.columns:
            df[col] = np.nan

    result_df = df.loc[:, output_columns].copy()
    result_df = result_df.rename(columns={"计费重量原始": "预估重量"})

    return result_df, summary_df, exception_df
