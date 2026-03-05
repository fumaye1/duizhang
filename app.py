from __future__ import annotations

import io
import json
from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

from reconcile import (
    REQUIRED_COLUMNS,
    ReconcileConfig,
    build_sku_mapping,
    map_yubao_sku,
    reconcile_main,
    validate_columns,
)

from analysis_module import (
    compute_province_pivot,
    compute_top_skus,
    compute_weight_price_table,
    parse_price_rules,
)

from bill_summary_module import BillLineItem, build_summary_bill
from wps_http_client import HttpMetricConfig, fetch_metric


st.set_page_config(page_title="多云仓自动对账工具", layout="wide")


@st.cache_data(show_spinner=False)
def load_excel(file_bytes: bytes, sheet_name: str) -> pd.DataFrame:
    return pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet_name)


def file_uploader_block(label: str, key: str) -> Tuple[Optional[bytes], Optional[str], List[str]]:
    uploaded = st.file_uploader(label, type=["xlsx", "xls"], key=key)
    if not uploaded:
        return None, None, []
    xls = pd.ExcelFile(uploaded)
    sheet = st.selectbox(f"选择Sheet - {label}", xls.sheet_names, key=f"sheet_{key}")
    data = load_excel(uploaded.getvalue(), sheet)
    return uploaded.getvalue(), sheet, list(data.columns)


def file_uploader_multi_block(
    label: str, key: str
) -> Tuple[List[Tuple[bytes, str]], List[str]]:
    uploaded_files = st.file_uploader(
        label,
        type=["xlsx", "xls"],
        key=key,
        accept_multiple_files=True,
    )
    if not uploaded_files:
        return [], []

    items: List[Tuple[bytes, str]] = []
    all_cols: set[str] = set()
    with st.expander(f"{label}（已上传 {len(uploaded_files)} 个文件）", expanded=True):
        for i, uploaded in enumerate(uploaded_files):
            xls = pd.ExcelFile(uploaded)
            sheet = st.selectbox(
                f"选择Sheet - {uploaded.name}",
                xls.sheet_names,
                key=f"sheet_{key}_{i}",
            )
            df_preview = load_excel(uploaded.getvalue(), sheet)
            all_cols.update(list(df_preview.columns))
            items.append((uploaded.getvalue(), sheet))

    return items, sorted(all_cols)


def read_df(file_bytes: Optional[bytes], sheet: Optional[str]) -> Optional[pd.DataFrame]:
    if not file_bytes or not sheet:
        return None
    return load_excel(file_bytes, sheet)


def read_and_map_multi(
    items: List[Tuple[bytes, str]], mapping: Dict[str, str]
) -> Optional[pd.DataFrame]:
    if not items:
        return None
    dfs: List[pd.DataFrame] = []
    for file_bytes, sheet in items:
        raw = load_excel(file_bytes, sheet)
        dfs.append(apply_mapping(raw, mapping))
    if not dfs:
        return None
    return pd.concat(dfs, ignore_index=True)


def read_multi_excel(items: List[Tuple[bytes, str]]) -> Optional[pd.DataFrame]:
    if not items:
        return None
    dfs: List[pd.DataFrame] = []
    for file_bytes, sheet in items:
        dfs.append(load_excel(file_bytes, sheet))
    if not dfs:
        return None
    return pd.concat(dfs, ignore_index=True)


def ensure_columns(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    df = df.copy()
    for col in cols:
        if col not in df.columns:
            df[col] = pd.NA
    return df


def aggregate_by_key_sum(df: pd.DataFrame, key_col: str, value_cols: List[str]) -> pd.DataFrame:
    if df.empty:
        return df
    work = df.copy()
    work[key_col] = work[key_col].astype(str)
    for col in value_cols:
        work[col] = pd.to_numeric(work[col], errors="coerce").fillna(0)
    out = work.groupby([key_col], dropna=False)[value_cols].sum().reset_index()
    return out


def excel_safe_sheet_name(name: str) -> str:
    text = str(name or "").strip() or "Sheet"
    for ch in [":", "\\", "/", "?", "*", "[", "]"]:
        text = text.replace(ch, "_")
    return text[:31]


def split_by_warehouse_and_threshold(
    df: pd.DataFrame,
    warehouse_col: str,
    max_rows: int,
) -> List[Tuple[str, pd.DataFrame]]:
    if df.empty:
        return [("多仓汇总", df)]

    if max_rows <= 0:
        max_rows = 800_000

    if len(df) <= max_rows:
        return [("多仓汇总", df)]

    if warehouse_col not in df.columns:
        raise ValueError(f"缺少列：{warehouse_col}，无法按仓分页")

    sheets: List[Tuple[str, pd.DataFrame]] = []
    for wh_value, wh_df in df.groupby(warehouse_col, dropna=False, sort=False):
        wh_name = excel_safe_sheet_name(str(wh_value) if pd.notna(wh_value) else "未知仓")
        wh_df = wh_df.copy()
        if len(wh_df) <= max_rows:
            sheets.append((wh_name, wh_df))
            continue

        # Chunk within a warehouse if it still exceeds the threshold.
        num_chunks = int((len(wh_df) + max_rows - 1) / max_rows)
        for idx in range(num_chunks):
            start = idx * max_rows
            end = min((idx + 1) * max_rows, len(wh_df))
            chunk = wh_df.iloc[start:end].copy()
            sheet_name = excel_safe_sheet_name(f"{wh_name}_{idx + 1}")
            sheets.append((sheet_name, chunk))

    # Ensure sheet names are unique.
    used: set[str] = set()
    unique_sheets: List[Tuple[str, pd.DataFrame]] = []
    for name, sdf in sheets:
        base = name
        suffix = 1
        while name in used:
            suffix += 1
            name = excel_safe_sheet_name(f"{base}_{suffix}")
        used.add(name)
        unique_sheets.append((name, sdf))
    return unique_sheets


def show_required_fields(title: str, key: str) -> None:
    with st.expander(f"{title}字段要求", expanded=False):
        cols = REQUIRED_COLUMNS.get(key, [])
        st.write("、".join(cols) if cols else "无")


def mapping_ui(
    title: str,
    required_cols: List[str],
    optional_cols: List[str],
    available_cols: List[str],
    key_prefix: str,
    preset: Optional[Dict[str, str]] = None,
) -> Tuple[Dict[str, str], List[str]]:
    if not available_cols:
        return {}, []
    mapping: Dict[str, str] = {}
    missing: List[str] = []
    with st.expander(f"{title}字段映射", expanded=False):
        st.markdown("**必填字段**")
        for col in required_cols:
            options = ["--请选择--"] + available_cols
            preset_value = (preset or {}).get(col)
            if preset_value in options:
                default_index = options.index(preset_value)
            else:
                default_index = options.index(col) if col in available_cols else 0
            selected = st.selectbox(
                f"{col}", options, index=default_index, key=f"{key_prefix}_{col}"
            )
            if selected == "--请选择--":
                missing.append(col)
            else:
                mapping[col] = selected

        if optional_cols:
            st.markdown("**可选字段**")
            for col in optional_cols:
                options = ["--不使用--"] + available_cols
                preset_value = (preset or {}).get(col)
                if preset_value in options:
                    default_index = options.index(preset_value)
                else:
                    default_index = options.index(col) if col in available_cols else 0
                selected = st.selectbox(
                    f"{col}", options, index=default_index, key=f"{key_prefix}_opt_{col}"
                )
                if selected != "--不使用--":
                    mapping[col] = selected

    return mapping, missing


def apply_mapping(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    rename_map = {source: target for target, source in mapping.items() if source}
    return df.rename(columns=rename_map)


def mapping_duplicates(mapping: Dict[str, str]) -> List[str]:
    values = [val for val in mapping.values() if val]
    return sorted({val for val in values if values.count(val) > 1})


def build_template(
    mappings: Dict[str, Dict[str, str]],
    erp_type: str,
) -> Dict[str, Dict[str, str]]:
    return {
        "version": "v1",
        "erp_type": erp_type,
        "mappings": mappings,
    }


def load_template(file_bytes: bytes) -> Dict[str, Dict[str, str]]:
    payload = json.loads(file_bytes.decode("utf-8"))
    return payload


def main():
    st.title("多云仓自动对账工具 - 主对账 MVP")
    st.caption("固定字段名版本（MVP），按PRD主流程实现")

    tab_reconcile, tab_multi, tab_analysis, tab_bill = st.tabs(
        ["📋 对账主流程", "📦 多仓汇总与回冲", "📊 快递费分析", "🧾 汇总账单"]
    )

    with tab_reconcile:
        st.subheader("字段映射模板")
        template_col1, template_col2 = st.columns([1, 1])
        with template_col1:
            template_upload = st.file_uploader(
                "加载模板（JSON）", type=["json"], key="mapping_template"
            )
        template_data: Dict[str, Dict[str, str]] = {}
        if template_upload is not None:
            try:
                template_data = load_template(template_upload.getvalue())
                st.success("模板已加载")
            except json.JSONDecodeError:
                st.error("模板文件格式不正确")
            except Exception:
                st.error("模板解析失败")

        st.subheader("1. 发货明细（支持多文件汇总）")
        st.caption("可同时上传旺店通与云宝发货明细；系统会先汇总再对账。若仅需把云仓账单出结果，可不上传发货明细。")

        clean_province = st.checkbox("清洗收货省份/市", value=True)

        st.markdown("#### 旺店通发货明细（可选）")
        wdt_items, wdt_cols = file_uploader_multi_block("旺店通发货明细文件", "detail_wdt")
        show_required_fields("旺店通发货明细", "wdt")
        wdt_mapping, wdt_missing = mapping_ui(
            "旺店通发货明细",
            REQUIRED_COLUMNS["wdt"],
            ["实际重量", "店铺名称"],
            wdt_cols,
            "detail_wdt",
            preset=template_data.get("mappings", {}).get("detail_wdt")
            or template_data.get("mappings", {}).get("detail"),
        )

        st.markdown("#### 云宝发货明细（可选）")
        yubao_items, yubao_cols = file_uploader_multi_block("云宝发货明细文件", "detail_yubao")
        show_required_fields("云宝发货明细", "yubao")
        yubao_detail_mapping, yubao_detail_missing = mapping_ui(
            "云宝发货明细",
            REQUIRED_COLUMNS["yubao"],
            ["实际重量", "店铺名称"],
            yubao_cols,
            "detail_yubao",
            preset=template_data.get("mappings", {}).get("detail_yubao"),
        )

        yubao_bytes = yubao_sheet = None
        yubao_mapping, yubao_missing = {}, []
        if yubao_items:
            st.subheader("2. 云宝名称货品表（云宝明细必填）")
            yubao_bytes, yubao_sheet, yubao_map_cols = file_uploader_block(
                "云宝名称货品表", "yubao_map"
            )
            show_required_fields("云宝名称货品表", "yubao_map")
            yubao_mapping, yubao_missing = mapping_ui(
                "云宝名称货品表",
                REQUIRED_COLUMNS["yubao_map"],
                [],
                yubao_map_cols,
                "yubao_map",
                preset=template_data.get("mappings", {}).get("yubao_map"),
            )

        st.subheader("3. 毛重表")
        maozhong_bytes, maozhong_sheet, maozhong_cols = file_uploader_block("毛重表", "maozhong")
        show_required_fields("毛重表", "maozhong")
        maozhong_mapping, maozhong_missing = mapping_ui(
            "毛重表",
            REQUIRED_COLUMNS["maozhong"],
            [],
            maozhong_cols,
            "maozhong",
            preset=template_data.get("mappings", {}).get("maozhong"),
        )

        st.subheader("4. 重量段定义表")
        weight_bytes, weight_sheet, weight_cols = file_uploader_block("重量段定义表", "weight_segments")
        show_required_fields("重量段定义表", "weight_segments")
        weight_mapping, weight_missing = mapping_ui(
            "重量段定义表",
            REQUIRED_COLUMNS["weight_segments"],
            [],
            weight_cols,
            "weight_segments",
            preset=template_data.get("mappings", {}).get("weight_segments"),
        )

        st.subheader("5. 多条件资费表")
        tariff_bytes, tariff_sheet, tariff_cols = file_uploader_block("多条件资费表", "tariff")
        show_required_fields("多条件资费表", "tariff")
        tariff_mapping, tariff_missing = mapping_ui(
            "多条件资费表",
            REQUIRED_COLUMNS["tariff"],
            ["快递费(元)", "快递费", "首重价格", "续重价格"],
            tariff_cols,
            "tariff",
            preset=template_data.get("mappings", {}).get("tariff"),
        )

        st.subheader("6. 辅助数据源")
        consumable_bytes, consumable_sheet, consumable_cols = file_uploader_block("耗材表（可选）", "consumables")
        consumable_mapping, _ = mapping_ui(
            "耗材表",
            [],
            REQUIRED_COLUMNS["consumables"],
            consumable_cols,
            "consumables",
            preset=template_data.get("mappings", {}).get("consumables"),
        )

        tear_bytes, tear_sheet, tear_cols = file_uploader_block("撕单表（可选）", "tear")
        tear_mapping, _ = mapping_ui(
            "撕单表",
            REQUIRED_COLUMNS["tear"],
            [],
            tear_cols,
            "tear",
            preset=template_data.get("mappings", {}).get("tear"),
        )

        after_bytes, after_sheet, after_cols = file_uploader_block("售后赔付表（可选）", "aftersale")
        after_mapping, _ = mapping_ui(
            "售后赔付表",
            REQUIRED_COLUMNS["aftersale"],
            [],
            after_cols,
            "aftersale",
            preset=template_data.get("mappings", {}).get("aftersale"),
        )

        st.subheader("7. 云仓账单（支持多文件汇总）")
        bill_items, bill_cols = file_uploader_multi_block("云仓账单文件", "bill")
        show_required_fields("云仓账单", "bill")
        bill_mapping, bill_missing = mapping_ui(
            "云仓账单",
            REQUIRED_COLUMNS["bill"],
            ["包装费(元)"],
            bill_cols,
            "bill",
            preset=template_data.get("mappings", {}).get("bill"),
        )

        template_payload = build_template(
            mappings={
                "detail": wdt_mapping,
                "detail_wdt": wdt_mapping,
                "detail_yubao": yubao_detail_mapping,
                "yubao_map": yubao_mapping,
                "maozhong": maozhong_mapping,
                "weight_segments": weight_mapping,
                "tariff": tariff_mapping,
                "consumables": consumable_mapping,
                "tear": tear_mapping,
                "aftersale": after_mapping,
                "bill": bill_mapping,
            },
            erp_type="mixed",
        )
        template_json = json.dumps(template_payload, ensure_ascii=False, indent=2)
        with template_col2:
            st.download_button(
                "保存模板（JSON）",
                data=template_json.encode("utf-8"),
                file_name="字段映射模板_mixed.json",
                mime="application/json",
            )

        st.subheader("8. 对账参数")
        yuncang = st.text_input("云仓名称", value="华东仓")
        use_actual_weight = st.checkbox("优先使用实际重量（若存在）", value=True)
        enable_deductions = st.checkbox("启用撕单/售后扣款", value=True)
        enable_consumables = st.checkbox("启用耗材计算", value=False)

        st.divider()

        if st.button("开始对账", type="primary"):
            has_detail = bool(wdt_items) or bool(yubao_items)
            missing_files = []
            if not bill_items:
                missing_files.append("云仓账单")
            if has_detail:
                if not maozhong_bytes:
                    missing_files.append("毛重表")
                if not weight_bytes:
                    missing_files.append("重量段定义表")
                if not tariff_bytes:
                    missing_files.append("多条件资费表")
                if yubao_items and not yubao_bytes:
                    missing_files.append("云宝名称货品表")

            if missing_files:
                st.error(f"缺少必选文件：{'、'.join(missing_files)}")
                return

            mapping_missing = bill_missing
            if has_detail:
                mapping_missing += maozhong_missing + weight_missing + tariff_missing
                if wdt_items:
                    mapping_missing += wdt_missing
                if yubao_items:
                    mapping_missing += yubao_detail_missing + yubao_missing
            if mapping_missing:
                st.error(f"请完成必填字段映射：{'、'.join(sorted(set(mapping_missing)))}")
                return

            mapping_sets = [
                ("云仓账单", bill_mapping),
            ]
            if has_detail:
                mapping_sets += [
                    ("毛重表", maozhong_mapping),
                    ("重量段定义表", weight_mapping),
                    ("多条件资费表", tariff_mapping),
                ]
                if wdt_items:
                    mapping_sets.append(("旺店通发货明细", wdt_mapping))
                if yubao_items:
                    mapping_sets.append(("云宝发货明细", yubao_detail_mapping))
                    mapping_sets.append(("云宝名称货品表", yubao_mapping))

            for title, mapping in mapping_sets:
                duplicates = mapping_duplicates(mapping)
                if duplicates:
                    st.error(f"{title}映射存在重复列：{'、'.join(duplicates)}")
                    return

            bill_df = read_and_map_multi(bill_items, bill_mapping)
            wdt_df = read_and_map_multi(wdt_items, wdt_mapping)
            yubao_detail_df = read_and_map_multi(yubao_items, yubao_detail_mapping)
            maozhong_df = (
                apply_mapping(read_df(maozhong_bytes, maozhong_sheet), maozhong_mapping)
                if maozhong_bytes
                else None
            )
            weight_df = (
                apply_mapping(read_df(weight_bytes, weight_sheet), weight_mapping) if weight_bytes else None
            )
            tariff_df = (
                apply_mapping(read_df(tariff_bytes, tariff_sheet), tariff_mapping) if tariff_bytes else None
            )
            yubao_map_df = (
                apply_mapping(read_df(yubao_bytes, yubao_sheet), yubao_mapping) if yubao_bytes else None
            )
            consumable_df = (
                apply_mapping(read_df(consumable_bytes, consumable_sheet), consumable_mapping)
                if consumable_bytes
                else None
            )
            tear_df = apply_mapping(read_df(tear_bytes, tear_sheet), tear_mapping) if tear_bytes else None
            after_df = apply_mapping(read_df(after_bytes, after_sheet), after_mapping) if after_bytes else None

            if bill_df is None or bill_df.empty:
                st.error("云仓账单为空，无法对账")
                return

            detail_df: Optional[pd.DataFrame]
            if not has_detail:
                detail_df = None
            else:
                detail_parts: List[pd.DataFrame] = []
                if wdt_df is not None and not wdt_df.empty:
                    detail_parts.append(wdt_df)
                if yubao_detail_df is not None and not yubao_detail_df.empty:
                    if yubao_map_df is None or yubao_map_df.empty:
                        st.error("已上传云宝发货明细，但云宝名称货品表为空")
                        return
                    sku_map = build_sku_mapping(yubao_map_df)
                    yubao_detail_df = map_yubao_sku(yubao_detail_df, sku_map)
                    # Normalize province field for reconciliation.
                    yubao_detail_df["收货省份"] = yubao_detail_df["收货地址"].astype(str)
                    detail_parts.append(yubao_detail_df)
                detail_df = pd.concat(detail_parts, ignore_index=True) if detail_parts else None

            missing = []
            missing += validate_columns(bill_df, REQUIRED_COLUMNS["bill"])
            if has_detail:
                if detail_df is None or detail_df.empty:
                    missing.append("发货明细")
                if maozhong_df is None:
                    missing += REQUIRED_COLUMNS["maozhong"]
                else:
                    missing += validate_columns(maozhong_df, REQUIRED_COLUMNS["maozhong"])
                if weight_df is None:
                    missing += REQUIRED_COLUMNS["weight_segments"]
                else:
                    missing += validate_columns(weight_df, REQUIRED_COLUMNS["weight_segments"])
                if tariff_df is None:
                    missing += REQUIRED_COLUMNS["tariff"]
                else:
                    missing += validate_columns(tariff_df, REQUIRED_COLUMNS["tariff"])
                if wdt_items and wdt_df is not None:
                    missing += validate_columns(wdt_df, REQUIRED_COLUMNS["wdt"])
                if yubao_items and yubao_detail_df is not None:
                    missing += validate_columns(yubao_detail_df, REQUIRED_COLUMNS["yubao"])
                if yubao_items and yubao_map_df is not None:
                    missing += validate_columns(yubao_map_df, REQUIRED_COLUMNS["yubao_map"])

            if missing:
                st.error(f"发现缺失字段：{'、'.join(sorted(set(missing)))}")
                return

            config = ReconcileConfig(
                yuncang=yuncang,
                erp_type="wdt",
                use_actual_weight=use_actual_weight,
                enable_deductions=enable_deductions,
                enable_consumables=enable_consumables,
                clean_province=clean_province,
            )

            with st.spinner("正在对账计算..."):
                result_df, summary_df, exception_df = reconcile_main(
                    detail_df=detail_df,
                    maozhong_df=maozhong_df,
                    weight_segments_df=weight_df,
                    tariff_df=tariff_df,
                    bill_df=bill_df,
                    config=config,
                    yubao_map_df=yubao_map_df,
                    consumables_df=consumable_df,
                    tear_df=tear_df,
                    aftersale_df=after_df,
                )

            st.success("对账完成")
            st.subheader("对账结果预览")
            st.dataframe(result_df.head(200), use_container_width=True)

            st.subheader("汇总预览")
            st.dataframe(summary_df, use_container_width=True)

            st.subheader("异常清单")
            st.dataframe(exception_df, use_container_width=True)

            output = io.BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                result_df.to_excel(writer, sheet_name="对账结果", index=False)
                summary_df.to_excel(writer, sheet_name="汇总", index=False)
                exception_df.to_excel(writer, sheet_name="异常清单", index=False)
            st.download_button(
                label="下载对账结果Excel",
                data=output.getvalue(),
                file_name="对账结果.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

    with tab_multi:
        st.subheader("1) 多仓快递费汇总（基于多个对账结果表）")
        st.caption("上传多个对账结果文件，选择字段后导出汇总表。")

        reconcile_items, reconcile_cols = file_uploader_multi_block(
            "对账结果文件（可多选）",
            "mw_reconcile_files",
        )

        default_fields = [
            "云仓",
            "物流单号",
            "店铺名称",
            "收货省份",
            "结算重量(取整)",
            "账单快递费",
        ]
        selected_fields = st.multiselect(
            "选择导出字段（默认已预选 6 个字段）",
            options=reconcile_cols,
            default=[c for c in default_fields if c in reconcile_cols],
            key="mw_summary_fields",
        )

        max_rows_per_sheet = st.number_input(
            "分页阈值（行数，超过则按云仓分页导出）",
            min_value=1,
            value=800_000,
            step=50_000,
            key="mw_summary_max_rows",
        )

        if st.button("生成多仓汇总表", type="primary", key="mw_build_summary"):
            if not reconcile_items:
                st.error("请先上传至少一个对账结果文件")
                return
            if not selected_fields:
                st.error("请至少选择 1 个导出字段")
                return
            df_all = read_multi_excel(reconcile_items)
            if df_all is None or df_all.empty:
                st.error("读取结果为空，请检查所选 Sheet")
                return

            df_all = ensure_columns(df_all, selected_fields)
            export_df = df_all.loc[:, selected_fields].copy()

            st.subheader("多仓汇总预览")
            st.dataframe(export_df.head(200), use_container_width=True)

            output = io.BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                sheets = split_by_warehouse_and_threshold(
                    export_df,
                    warehouse_col="云仓",
                    max_rows=int(max_rows_per_sheet),
                )
                for sheet_name, sheet_df in sheets:
                    sheet_df.to_excel(writer, sheet_name=sheet_name, index=False)
            st.download_button(
                label="下载多仓汇总Excel",
                data=output.getvalue(),
                file_name="多仓快递费汇总.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="mw_download_summary",
            )

        st.divider()

        st.subheader("2) 回冲差异分析（新复核结果 vs 旧汇总表）")
        st.caption("按物流单号汇总快递费等字段，对比新旧差异，导出回冲差异表。")

        new_items, new_cols = file_uploader_multi_block(
            "新复核结果表（可多选）",
            "mw_recharge_new",
        )
        old_bytes, old_sheet, old_cols = file_uploader_block(
            "旧汇总表（手动上传）",
            "mw_recharge_old",
        )

        compare_candidates = [
            "账单快递费",
            "快递费(核算后)",
            "应付金额",
            "差异金额",
            "理论运费",
            "账单运费",
        ]
        compare_options = sorted(set(new_cols) | set(old_cols))
        default_compare = [c for c in compare_candidates if c in compare_options]
        compare_fields = st.multiselect(
            "选择需要对比的字段（可多选）",
            options=compare_options,
            default=default_compare[:1] if default_compare else [],
            key="mw_recharge_compare_fields",
        )

        if st.button("生成回冲差异表", type="primary", key="mw_build_recharge"):
            if not new_items:
                st.error("请上传新复核结果表")
                return
            if not old_bytes or not old_sheet:
                st.error("请上传旧汇总表")
                return
            if not compare_fields:
                st.error("请至少选择 1 个对比字段")
                return

            new_df = read_multi_excel(new_items)
            old_df = read_df(old_bytes, old_sheet)
            if new_df is None or new_df.empty:
                st.error("新复核结果读取为空，请检查所选 Sheet")
                return
            if old_df is None or old_df.empty:
                st.error("旧汇总表读取为空，请检查所选 Sheet")
                return

            key_col = "物流单号"
            if key_col not in new_df.columns or key_col not in old_df.columns:
                st.error("新旧表均需包含列：物流单号")
                return

            new_df = ensure_columns(new_df, [key_col] + compare_fields)
            old_df = ensure_columns(old_df, [key_col] + compare_fields)

            new_agg = aggregate_by_key_sum(new_df, key_col=key_col, value_cols=compare_fields)
            old_agg = aggregate_by_key_sum(old_df, key_col=key_col, value_cols=compare_fields)

            diff = old_agg.merge(new_agg, on=[key_col], how="outer", suffixes=("_旧", "_新"))
            for col in compare_fields:
                old_col = f"{col}_旧"
                new_col = f"{col}_新"
                if old_col not in diff.columns:
                    diff[old_col] = 0
                if new_col not in diff.columns:
                    diff[new_col] = 0
                diff[f"{col}_差异(新-旧)"] = pd.to_numeric(diff[new_col], errors="coerce").fillna(0) - pd.to_numeric(
                    diff[old_col], errors="coerce"
                ).fillna(0)

            show_cols: List[str] = [key_col]
            for col in compare_fields:
                show_cols += [f"{col}_旧", f"{col}_新", f"{col}_差异(新-旧)"]
            diff = ensure_columns(diff, show_cols).loc[:, show_cols].copy()

            st.subheader("回冲差异预览")
            st.dataframe(diff.head(200), use_container_width=True)

            output = io.BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                diff.to_excel(writer, sheet_name="回冲差异", index=False)
            st.download_button(
                label="下载回冲差异Excel",
                data=output.getvalue(),
                file_name="回冲差异表.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="mw_download_recharge",
            )

    with tab_analysis:
        st.subheader("快递费分析")
        st.caption("上传单个云仓的对账结果表，生成省份透视、TOP单品、重量单价表，并导出Excel。")

        analysis_bytes, analysis_sheet, analysis_cols = file_uploader_block(
            "对账结果表（单文件）",
            "analysis_file",
        )
        if not analysis_bytes or not analysis_sheet:
            st.info("请先上传对账结果表")
            analysis_df = None
        else:
            analysis_df = read_df(analysis_bytes, analysis_sheet)

        if analysis_df is None or analysis_df.empty:
            if analysis_bytes and analysis_sheet:
                st.error("对账结果表为空，请检查所选 Sheet")
        else:
            st.subheader("字段选择")
            col_left, col_right = st.columns([1, 1])
            with col_left:
                order_col = st.selectbox(
                    "订单唯一标识列",
                    options=analysis_cols,
                    index=analysis_cols.index("物流单号") if "物流单号" in analysis_cols else 0,
                    key="analysis_order_col",
                )
                province_col = st.selectbox(
                    "省份列",
                    options=analysis_cols,
                    index=analysis_cols.index("收货省份") if "收货省份" in analysis_cols else 0,
                    key="analysis_province_col",
                )
                sku_col = st.selectbox(
                    "单品列",
                    options=analysis_cols,
                    index=analysis_cols.index("商家编码") if "商家编码" in analysis_cols else 0,
                    key="analysis_sku_col",
                )
            with col_right:
                weight_col = st.selectbox(
                    "重量列",
                    options=analysis_cols,
                    index=analysis_cols.index("结算重量(取整)") if "结算重量(取整)" in analysis_cols else 0,
                    key="analysis_weight_col",
                )
                fee_candidates = ["账单快递费", "快递费(核算后)", "应付金额", "账单运费", "理论运费"]
                fee_default = next((c for c in fee_candidates if c in analysis_cols), analysis_cols[0])
                fee_col = st.selectbox(
                    "快递费列（用于统计）",
                    options=analysis_cols,
                    index=analysis_cols.index(fee_default) if fee_default in analysis_cols else 0,
                    key="analysis_fee_col",
                )

            st.subheader("运营交割价配置")
            st.caption("填写“重量上限(kg) -> 运营交割价(元)”。未配置时将不计算差价相关字段。")
            config_init = pd.DataFrame(
                [
                    {"重量上限(kg)": pd.NA, "运营交割价(元)": pd.NA},
                ]
            )
            config_df = st.data_editor(
                config_init,
                num_rows="dynamic",
                use_container_width=True,
                key="analysis_price_config",
            )
            rules = parse_price_rules(config_df)

            top_n = st.number_input(
                "TOP 单品数量",
                min_value=1,
                value=50,
                step=10,
                key="analysis_top_n",
            )

            if st.button("生成分析", type="primary", key="analysis_run"):
                missing = [
                    c
                    for c in [order_col, province_col, sku_col, weight_col, fee_col]
                    if c not in analysis_df.columns
                ]
                if missing:
                    st.error(f"对账结果表缺少列：{'、'.join(missing)}")
                else:
                    with st.spinner("正在生成分析结果..."):
                        pivot_count, pivot_share = compute_province_pivot(
                            analysis_df,
                            province_col=province_col,
                            weight_col=weight_col,
                            order_col=order_col,
                        )
                        top_sku_df = compute_top_skus(
                            analysis_df,
                            sku_col=sku_col,
                            weight_col=weight_col,
                            fee_col=fee_col,
                            order_col=order_col,
                            rules=rules,
                            top_n=int(top_n),
                        )
                        weight_price_df = compute_weight_price_table(
                            analysis_df,
                            weight_col=weight_col,
                            fee_col=fee_col,
                            order_col=order_col,
                            rules=rules,
                        )

                    st.subheader("省份透视（单量）")
                    st.dataframe(pivot_count, use_container_width=True)
                    st.subheader("省份透视（占比）")
                    st.dataframe(pivot_share, use_container_width=True)
                    st.subheader("TOP 单品")
                    st.dataframe(top_sku_df, use_container_width=True)
                    st.subheader("重量单价表")
                    st.dataframe(weight_price_df, use_container_width=True)

                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine="openpyxl") as writer:
                        pivot_count.to_excel(writer, sheet_name="省份透视_单量", index=False)
                        pivot_share.to_excel(writer, sheet_name="省份透视_占比", index=False)
                        top_sku_df.to_excel(writer, sheet_name="TOP单品", index=False)
                        weight_price_df.to_excel(writer, sheet_name="重量单价", index=False)

                    st.download_button(
                        label="下载分析Excel",
                        data=output.getvalue(),
                        file_name="快递费分析.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="analysis_download",
                    )

    with tab_bill:
        st.subheader("汇总账单")
        st.caption(
            "根据结果表生成汇总账单（序号/项目/金额/余额）。期初余额与本月预充支持手动输入或从WPS多维表API获取。"
        )

        bill_bytes, bill_sheet, bill_cols = file_uploader_block(
            "结果表（单文件，可选任意Sheet）",
            "bill_summary_file",
        )
        if not bill_bytes or not bill_sheet:
            st.info("请先上传结果表")
            return

        result_df = read_df(bill_bytes, bill_sheet)
        if result_df is None or result_df.empty:
            st.error("结果表为空，请检查所选 Sheet")
            return

        st.subheader("1) 期初余额")
        opening_mode = st.radio(
            "期初余额来源",
            options=["手动输入", "从WPS多维表API获取"],
            horizontal=True,
            key="bill_opening_mode",
        )
        opening_balance: float = 0.0

        if opening_mode == "手动输入":
            opening_balance = float(
                st.number_input(
                    "期初余额",
                    value=float(st.session_state.get("bill_opening_balance", 0.0)),
                    step=100.0,
                    key="bill_opening_balance",
                )
            )
        else:
            st.caption("说明：这里提供通用HTTP取数配置。将WPS多维表接口的请求参数粘贴进来即可。")
            col1, col2 = st.columns([1, 1])
            with col1:
                opening_url = st.text_input("请求URL", key="bill_opening_url")
                opening_method = st.selectbox("请求方法", options=["GET", "POST"], key="bill_opening_method")
                opening_value_path = st.text_input(
                    "取值路径 value_path（例：data.items[0].amount）",
                    value="value",
                    key="bill_opening_value_path",
                )
            with col2:
                opening_headers = st.text_area(
                    "请求Headers(JSON对象)",
                    value=st.session_state.get("bill_opening_headers", "{}"),
                    height=120,
                    key="bill_opening_headers",
                )
                opening_body = st.text_area(
                    "请求参数/Body(JSON对象；GET为query参数，POST为json body)",
                    value=st.session_state.get("bill_opening_body", "{}"),
                    height=120,
                    key="bill_opening_body",
                )

            if st.button("获取期初余额", type="secondary", key="bill_fetch_opening"):
                try:
                    cfg = HttpMetricConfig(
                        method=str(opening_method),
                        url=str(opening_url),
                        headers_json=str(opening_headers),
                        body_json=str(opening_body),
                        value_path=str(opening_value_path),
                    )
                    value = fetch_metric(cfg)
                    st.session_state["bill_opening_balance"] = float(value)
                    st.success(f"已获取期初余额：{value}")
                except Exception as e:
                    st.error(f"获取期初余额失败：{e}")

            opening_balance = float(st.session_state.get("bill_opening_balance", 0.0))

        st.subheader("2) 选择项目（从结果表字段合计）")
        sum_candidates = [
            "账单快递费",
            "快递费(核算后)",
            "耗材费",
            "加收费",
            "应付金额",
            "差异金额",
        ]
        default_sum_cols = [c for c in sum_candidates if c in bill_cols]
        selected_sum_cols = st.multiselect(
            "选择需要合计的字段（可多选）",
            options=bill_cols,
            default=default_sum_cols,
            key="bill_sum_cols",
        )

        items_init = pd.DataFrame(
            [
                {
                    "项目": col,
                    "来源列": col,
                    "系数": 1.0,
                }
                for col in selected_sum_cols
            ]
        )
        items_df = st.data_editor(
            items_init,
            use_container_width=True,
            hide_index=True,
            disabled=["来源列"],
            key="bill_items_editor",
        )

        st.subheader("3) 本月预充")
        prepaid_mode = st.radio(
            "本月预充来源",
            options=["手动输入", "从WPS多维表API获取"],
            horizontal=True,
            key="bill_prepaid_mode",
        )
        prepaid_amount: float = 0.0
        if prepaid_mode == "手动输入":
            prepaid_amount_input = float(
                st.number_input(
                    "本月预充金额（输入正数，系统按 -1 系数计入）",
                    value=float(st.session_state.get("bill_prepaid_amount", 0.0)),
                    step=100.0,
                    key="bill_prepaid_amount",
                )
            )
            prepaid_amount = -abs(prepaid_amount_input)
        else:
            st.caption("说明：同上，使用通用HTTP取数配置拉取本月预充金额。")
            col1, col2 = st.columns([1, 1])
            with col1:
                prepaid_url = st.text_input("请求URL", key="bill_prepaid_url")
                prepaid_method = st.selectbox("请求方法", options=["GET", "POST"], key="bill_prepaid_method")
                prepaid_value_path = st.text_input(
                    "取值路径 value_path（例：data.items[0].amount）",
                    value="value",
                    key="bill_prepaid_value_path",
                )
            with col2:
                prepaid_headers = st.text_area(
                    "请求Headers(JSON对象)",
                    value=st.session_state.get("bill_prepaid_headers", "{}"),
                    height=120,
                    key="bill_prepaid_headers",
                )
                prepaid_body = st.text_area(
                    "请求参数/Body(JSON对象；GET为query参数，POST为json body)",
                    value=st.session_state.get("bill_prepaid_body", "{}"),
                    height=120,
                    key="bill_prepaid_body",
                )

            if st.button("获取本月预充金额", type="secondary", key="bill_fetch_prepaid"):
                try:
                    cfg = HttpMetricConfig(
                        method=str(prepaid_method),
                        url=str(prepaid_url),
                        headers_json=str(prepaid_headers),
                        body_json=str(prepaid_body),
                        value_path=str(prepaid_value_path),
                    )
                    value = fetch_metric(cfg)
                    st.session_state["bill_prepaid_amount"] = float(value)
                    st.success(f"已获取本月预充金额：{value}")
                except Exception as e:
                    st.error(f"获取本月预充金额失败：{e}")

            prepaid_amount = -abs(float(st.session_state.get("bill_prepaid_amount", 0.0)))

        st.divider()

        if st.button("生成汇总账单", type="primary", key="bill_build"):
            if not isinstance(items_df, pd.DataFrame) or items_df.empty:
                st.error("请先选择至少 1 个需要合计的字段")
                return
            if "来源列" not in items_df.columns or "项目" not in items_df.columns or "系数" not in items_df.columns:
                st.error("项目配置表结构异常，请重新选择字段")
                return

            line_items: List[BillLineItem] = []
            for _, row in items_df.iterrows():
                project = str(row.get("项目") or "").strip()
                source_col = str(row.get("来源列") or "").strip()
                multiplier = row.get("系数")
                if not project or not source_col:
                    continue
                try:
                    multiplier_f = float(multiplier)
                except Exception:
                    multiplier_f = 1.0
                line_items.append(
                    BillLineItem(project=project, source_column=source_col, multiplier=multiplier_f)
                )

            if not line_items:
                st.error("请至少保留 1 行有效的项目配置")
                return

            bill_df = build_summary_bill(
                result_df=result_df,
                opening_balance=float(opening_balance),
                line_items=line_items,
                prepaid_amount=float(prepaid_amount),
            )

            st.subheader("汇总账单预览")
            st.dataframe(bill_df, use_container_width=True)

            output = io.BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                bill_df.to_excel(writer, sheet_name="汇总账单", index=False)

            st.download_button(
                label="下载汇总账单Excel",
                data=output.getvalue(),
                file_name="汇总账单.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="bill_download",
            )


if __name__ == "__main__":
    main()
