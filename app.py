from __future__ import annotations

import io
import json
from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

from reconcile import (
    REQUIRED_COLUMNS,
    ReconcileConfig,
    reconcile_main,
    validate_columns,
)


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


def read_df(file_bytes: Optional[bytes], sheet: Optional[str]) -> Optional[pd.DataFrame]:
    if not file_bytes or not sheet:
        return None
    return load_excel(file_bytes, sheet)


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

    tab = st.tabs(["📋 对账主流程"])[0]

    with tab:
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

        st.subheader("1. 发货明细")
        erp_type = st.radio("ERP类型", ["旺店通", "云宝"], horizontal=True)
        erp_key = "wdt" if erp_type == "旺店通" else "yubao"
        detail_bytes, detail_sheet, detail_cols = file_uploader_block("发货明细文件", "detail")
        show_required_fields("发货明细", erp_key)
        clean_province = st.checkbox("清洗收货省份/市", value=True)

        detail_mapping, detail_missing = mapping_ui(
            "发货明细",
            REQUIRED_COLUMNS[erp_key],
            ["实际重量", "店铺名称"],
            detail_cols,
            "detail",
            preset=template_data.get("mappings", {}).get("detail"),
        )

        yubao_bytes = yubao_sheet = None
        if erp_type == "云宝":
            st.subheader("2. 云宝名称货品表")
            yubao_bytes, yubao_sheet, yubao_cols = file_uploader_block("云宝名称货品表", "yubao_map")
            show_required_fields("云宝名称货品表", "yubao_map")
            yubao_mapping, yubao_missing = mapping_ui(
                "云宝名称货品表",
                REQUIRED_COLUMNS["yubao_map"],
                [],
                yubao_cols,
                "yubao_map",
                preset=template_data.get("mappings", {}).get("yubao_map"),
            )
        else:
            yubao_mapping, yubao_missing = {}, []

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

        st.subheader("7. 云仓账单")
        bill_bytes, bill_sheet, bill_cols = file_uploader_block("云仓账单", "bill")
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
                "detail": detail_mapping,
                "yubao_map": yubao_mapping,
                "maozhong": maozhong_mapping,
                "weight_segments": weight_mapping,
                "tariff": tariff_mapping,
                "consumables": consumable_mapping,
                "tear": tear_mapping,
                "aftersale": after_mapping,
                "bill": bill_mapping,
            },
            erp_type=erp_key,
        )
        template_json = json.dumps(template_payload, ensure_ascii=False, indent=2)
        with template_col2:
            st.download_button(
                "保存模板（JSON）",
                data=template_json.encode("utf-8"),
                file_name=f"字段映射模板_{erp_key}.json",
                mime="application/json",
            )

        st.subheader("8. 对账参数")
        yuncang = st.text_input("云仓名称", value="华东仓")
        use_actual_weight = st.checkbox("优先使用实际重量（若存在）", value=True)
        enable_deductions = st.checkbox("启用撕单/售后扣款", value=True)
        enable_consumables = st.checkbox("启用耗材计算", value=False)

        st.divider()

        if st.button("开始对账", type="primary"):
            missing_files = []
            if not detail_bytes:
                missing_files.append("发货明细")
            if not maozhong_bytes:
                missing_files.append("毛重表")
            if not weight_bytes:
                missing_files.append("重量段定义表")
            if not tariff_bytes:
                missing_files.append("多条件资费表")
            if not bill_bytes:
                missing_files.append("云仓账单")
            if erp_type == "云宝" and not yubao_bytes:
                missing_files.append("云宝名称货品表")

            if missing_files:
                st.error(f"缺少必选文件：{'、'.join(missing_files)}")
                return

            mapping_missing = detail_missing + maozhong_missing + weight_missing + tariff_missing + bill_missing
            if erp_type == "云宝":
                mapping_missing += yubao_missing
            if mapping_missing:
                st.error(f"请完成必填字段映射：{'、'.join(sorted(set(mapping_missing)))}")
                return

            mapping_sets = [
                ("发货明细", detail_mapping),
                ("毛重表", maozhong_mapping),
                ("重量段定义表", weight_mapping),
                ("多条件资费表", tariff_mapping),
                ("云仓账单", bill_mapping),
            ]
            if erp_type == "云宝":
                mapping_sets.append(("云宝名称货品表", yubao_mapping))

            for title, mapping in mapping_sets:
                duplicates = mapping_duplicates(mapping)
                if duplicates:
                    st.error(f"{title}映射存在重复列：{'、'.join(duplicates)}")
                    return

            detail_df = apply_mapping(read_df(detail_bytes, detail_sheet), detail_mapping)
            maozhong_df = apply_mapping(read_df(maozhong_bytes, maozhong_sheet), maozhong_mapping)
            weight_df = apply_mapping(read_df(weight_bytes, weight_sheet), weight_mapping)
            tariff_df = apply_mapping(read_df(tariff_bytes, tariff_sheet), tariff_mapping)
            bill_df = apply_mapping(read_df(bill_bytes, bill_sheet), bill_mapping)
            yubao_df = apply_mapping(read_df(yubao_bytes, yubao_sheet), yubao_mapping) if yubao_bytes else None
            consumable_df = (
                apply_mapping(read_df(consumable_bytes, consumable_sheet), consumable_mapping)
                if consumable_bytes
                else None
            )
            tear_df = apply_mapping(read_df(tear_bytes, tear_sheet), tear_mapping) if tear_bytes else None
            after_df = apply_mapping(read_df(after_bytes, after_sheet), after_mapping) if after_bytes else None

            missing = []
            missing += validate_columns(detail_df, REQUIRED_COLUMNS[erp_key])
            missing += validate_columns(maozhong_df, REQUIRED_COLUMNS["maozhong"])
            missing += validate_columns(weight_df, REQUIRED_COLUMNS["weight_segments"])
            missing += validate_columns(tariff_df, REQUIRED_COLUMNS["tariff"])
            missing += validate_columns(bill_df, REQUIRED_COLUMNS["bill"])
            if erp_type == "云宝" and yubao_df is not None:
                missing += validate_columns(yubao_df, REQUIRED_COLUMNS["yubao_map"])

            if missing:
                st.error(f"发现缺失字段：{'、'.join(sorted(set(missing)))}")
                return

            config = ReconcileConfig(
                yuncang=yuncang,
                erp_type=erp_key,
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
                    yubao_map_df=yubao_df,
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


if __name__ == "__main__":
    main()
