from __future__ import annotations

import io
import json
from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

from reconcile import (
    REQUIRED_COLUMNS,
    ReconcileConfig,
    WEIGHT_SOURCE_DETAIL_ESTIMATED,
    WEIGHT_SOURCE_MAOZHONG_CALC,
    PACK_RULE_FIXED_NON_PACKED,
    PACK_RULE_FIXED_PACKED,
    PACK_RULE_IGNORE,
    PACK_RULE_MATCH,
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


MULTI_STANDARD_FIELDS: List[str] = [
    "云仓",
    "物流单号",
    "商家编码",
    "数量",
    "货品数量",
    "实际重量",
    "结算重量(取整)",
    "收货省份",
    "店铺名称",
    "账单快递费",
    "快递费(核算后)",
    "应付金额",
    "差异金额",
    "理论运费",
    "账单运费",
]


ExcelSheetItem = Tuple[bytes, str, str, str, List[str]]


def _collect_excel_items_from_workbooks(
    *,
    label: str,
    key: str,
    workbooks: List[Tuple[bytes, str, str, List[str]]],
) -> Tuple[List[ExcelSheetItem], List[str]]:
    items: List[ExcelSheetItem] = []
    all_cols: set[str] = set()

    total_sheets = sum(len(sheets) for _b, _n, _e, sheets in workbooks)
    if total_sheets <= 0:
        return [], []

    progress = st.progress(0)
    status_text = st.empty()
    done = 0
    for file_bytes, name, engine, sheets in workbooks:
        status_text.write(f"字段检查（仅读表头）：{name}（{len(sheets)} 个Sheet）")
        for sheet in sheets:
            cols = load_excel_columns(file_bytes, sheet, engine=engine)
            all_cols.update(cols)
            items.append((file_bytes, sheet, name, engine, cols))
            done += 1
            progress.progress(min(1.0, done / max(1, total_sheets)))
    status_text.empty()
    return items, sorted(all_cols)


def ordered_multiselect(
    label: str,
    options: List[str],
    default: Optional[List[str]],
    key: str,
    help_text: Optional[str] = None,
) -> List[str]:
    """A multiselect that preserves selection order in session_state.

    Streamlit's multiselect returns values in option order; we maintain an explicit order list.
    """

    selected = st.multiselect(label, options=options, default=default or [], key=key, help=help_text)
    order_key = f"{key}__order"
    prev_order: List[str] = list(st.session_state.get(order_key, []))

    # Remove deselected
    selected_set = set(selected)
    new_order = [x for x in prev_order if x in selected_set]

    # Append newly selected
    for x in selected:
        if x not in new_order:
            new_order.append(x)

    st.session_state[order_key] = new_order
    return new_order


def file_uploader_multi_block_named(
    label: str, key: str
) -> Tuple[List[Tuple[bytes, str, str, str, List[str]]], List[str]]:
    uploaded_files = st.file_uploader(
        label,
        type=["xlsx", "xls"],
        key=key,
        accept_multiple_files=True,
    )
    if not uploaded_files:
        return [], []

    with st.expander(f"{label}（已上传 {len(uploaded_files)} 个文件）", expanded=True):
        st.caption("可为每个工作簿多选 Sheet（选中的每个 Sheet 会参与后续生成）。")

        st.caption("性能优化：上传阶段仅解析 Sheet 名；字段检查仅读取表头；生成时才读取明细数据。")

        workbooks: List[Tuple[bytes, str, str, List[str]]] = []
        for i, uploaded in enumerate(uploaded_files):
            file_bytes = uploaded.getvalue()
            xls, engine = _safe_excel_file(file_bytes, uploaded.name, f"{label}:{uploaded.name}")
            if xls is None or engine is None:
                st.warning(f"已跳过文件：{uploaded.name}")
                continue

            selected_sheets = st.multiselect(
                f"选择Sheet（可多选）- {uploaded.name}",
                options=xls.sheet_names,
                default=[xls.sheet_names[0]] if xls.sheet_names else [],
                key=f"sheets_{key}_{i}",
            )
            if not selected_sheets:
                st.warning(f"未选择任何Sheet，已跳过：{uploaded.name}")
                continue

            workbooks.append((file_bytes, uploaded.name, engine, list(selected_sheets)))

        items, all_cols = _collect_excel_items_from_workbooks(label=label, key=key, workbooks=workbooks)
    return items, all_cols


def file_uploader_block_named(label: str, key: str) -> Tuple[List[ExcelSheetItem], List[str]]:
    uploaded = st.file_uploader(label, type=["xlsx", "xls"], key=key)
    if not uploaded:
        return [], []

    file_bytes = uploaded.getvalue()
    xls, engine = _safe_excel_file(file_bytes, uploaded.name, label)
    if xls is None or engine is None:
        return [], []

    with st.expander(f"{label}（已上传 1 个文件）", expanded=True):
        st.caption("可多选 Sheet（选中的每个 Sheet 会参与后续计算/生成）。")
        st.caption("性能优化：上传阶段仅解析 Sheet 名；字段检查仅读取表头；需要时才读取明细数据。")

        selected_sheets = st.multiselect(
            f"选择Sheet（可多选）- {uploaded.name}",
            options=xls.sheet_names,
            default=[xls.sheet_names[0]] if xls.sheet_names else [],
            key=f"sheets_{key}",
        )
        if not selected_sheets:
            st.warning("未选择任何Sheet")
            return [], []

        workbooks = [(file_bytes, uploaded.name, engine, list(selected_sheets))]
        return _collect_excel_items_from_workbooks(label=label, key=key, workbooks=workbooks)


def build_per_file_hit_status_ui(
    *,
    file_name: str,
    file_cols: List[str],
    standard_fields_ordered: List[str],
) -> List[str]:
    if not standard_fields_ordered:
        return []

    missing = [f for f in standard_fields_ordered if f not in set(file_cols)]
    with st.expander(f"字段命中检查 - {file_name}", expanded=True):
        if not missing:
            st.success("全部命中")
        else:
            st.error("未命中字段：" + "、".join(missing))
            st.info("请在源表中把列名手动改为规范字段名后重新上传（本工具不会自动清洗/改列名）。")
    return missing


@st.cache_data(show_spinner=False)
def load_excel(file_bytes: bytes, sheet_name: str, engine: Optional[str] = None) -> pd.DataFrame:
    return pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet_name, engine=engine)


@st.cache_data(show_spinner=False)
def load_excel_columns(file_bytes: bytes, sheet_name: str, engine: Optional[str] = None) -> List[str]:
    """Load only header row (nrows=0) to get columns quickly."""
    df0 = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet_name, engine=engine, nrows=0)
    return list(df0.columns)


@st.cache_data(show_spinner=False)
def load_excel_usecols(
    file_bytes: bytes,
    sheet_name: str,
    engine: Optional[str],
    usecols: Tuple[str, ...],
) -> pd.DataFrame:
    """Load only selected columns to speed up large sheets."""
    return pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet_name, engine=engine, usecols=list(usecols))


def _detect_excel_engine(file_bytes: bytes) -> Optional[str]:
    if not file_bytes:
        return None

    # xlsx is a zip file
    if file_bytes[:2] == b"PK":
        return "openpyxl"

    # xls is an OLE2 compound document
    if file_bytes[:8] == b"\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1":
        return "xlrd"

    return None


def _safe_excel_file(file_bytes: bytes, filename: str, context_label: str) -> Tuple[Optional[pd.ExcelFile], Optional[str]]:
    engine = _detect_excel_engine(file_bytes)
    if engine is None:
        st.error(
            "上传文件看起来不是有效的Excel工作簿（无法识别为 xlsx/xls）。\n"
            "请确认文件未损坏，并优先使用 .xlsx 格式重新导出/另存后上传。\n\n"
            f"文件：{filename}\n"
            f"位置：{context_label}"
        )
        return None, None

    ext = str(filename).lower()
    if ext.endswith(".xls") and engine == "openpyxl":
        st.warning(
            f"文件扩展名为 .xls，但内容更像 .xlsx（ZIP）。建议将文件改为 .xlsx 后再上传：{filename}"
        )
    if ext.endswith(".xlsx") and engine == "xlrd":
        st.warning(
            f"文件扩展名为 .xlsx，但内容更像 .xls（OLE2）。建议将文件改为 .xls 或重新导出：{filename}"
        )

    try:
        xls = pd.ExcelFile(io.BytesIO(file_bytes), engine=engine)
        return xls, engine
    except ImportError as e:
        if engine == "xlrd":
            st.error(
                "检测到 .xls（旧版Excel）文件，但当前环境缺少读取依赖：xlrd。\n"
                "解决方式：\n"
                "1) 安装依赖：pip install xlrd>=2.0.1（推荐），或\n"
                "2) 将文件另存为 .xlsx 后重新上传。\n\n"
                f"文件：{filename}\n错误：{e}"
            )
        else:
            st.error(f"读取Excel失败：{filename}。错误：{e}")
        return None, None
    except Exception as e:
        # Common case: xlrd.biffh.XLRDError: Can't find workbook in OLE2 compound document
        err_name = type(e).__name__
        if err_name == "XLRDError":
            st.error(
                "读取 .xls 失败：文件内容不是有效的 xls 工作簿（可能是文件损坏，或扩展名与实际格式不一致）。\n"
                "建议：\n"
                "- 用Excel/WPS重新‘另存为’ .xlsx 后上传；或\n"
                "- 重新从源系统导出账单/结果表（避免中间拷贝/改后缀）。\n\n"
                f"文件：{filename}\n错误：{e}"
            )
        else:
            st.error(
                "读取Excel失败（可能是文件损坏或格式不兼容）。\n"
                "建议优先另存为 .xlsx 再上传。\n\n"
                f"文件：{filename}\n错误类型：{err_name}\n错误：{e}"
            )
        return None, None
def file_uploader_block(label: str, key: str) -> Tuple[List[ExcelSheetItem], List[str]]:
    """Single-file uploader with multi-sheet selection (unified behavior)."""
    return file_uploader_block_named(label, key)


def file_uploader_multi_block(
    label: str, key: str
) -> Tuple[List[ExcelSheetItem], List[str]]:
    # Backward-compatible wrapper: keep name but return the new named-item shape.
    return file_uploader_multi_block_named(label, key)


def read_df_items(items: List[ExcelSheetItem]) -> Optional[pd.DataFrame]:
    if not items:
        return None
    dfs: List[pd.DataFrame] = []
    for file_bytes, sheet, _name, engine, _cols in items:
        dfs.append(load_excel(file_bytes, sheet, engine=engine))
    if not dfs:
        return None
    return pd.concat(dfs, ignore_index=True)


def read_df_items_with_progress(
    *,
    items: List[ExcelSheetItem],
    title: str,
) -> Optional[pd.DataFrame]:
    if not items:
        return None

    total = len(items)
    progress = st.progress(0)
    status = st.empty()
    dfs: List[pd.DataFrame] = []
    for idx, (file_bytes, sheet, name, engine, _cols) in enumerate(items, start=1):
        status.write(f"正在读取{title}：{name}::{sheet}（{idx}/{total}）")
        dfs.append(load_excel(file_bytes, sheet, engine=engine))
        progress.progress(min(1.0, idx / max(1, total)))
    status.empty()
    if not dfs:
        return None
    return pd.concat(dfs, ignore_index=True)


def read_and_map_multi_with_progress(
    *,
    items: List[ExcelSheetItem],
    mapping: Dict[str, str],
    title: str,
) -> Optional[pd.DataFrame]:
    if not items:
        return None

    usecols_list = [v for v in mapping.values() if v]
    usecols = tuple(sorted(set(usecols_list)))

    total = len(items)
    progress = st.progress(0)
    status = st.empty()
    dfs: List[pd.DataFrame] = []
    for idx, (file_bytes, sheet, name, engine, _cols) in enumerate(items, start=1):
        status.write(f"正在读取{title}：{name}::{sheet}（{idx}/{total}）")
        if usecols:
            raw = load_excel_usecols(file_bytes, sheet, engine, usecols)
        else:
            raw = load_excel(file_bytes, sheet, engine=engine)
        dfs.append(apply_mapping(raw, mapping))
        progress.progress(min(1.0, idx / max(1, total)))
    status.empty()
    if not dfs:
        return None
    return pd.concat(dfs, ignore_index=True)


def read_and_map_multi(
    items: List[ExcelSheetItem],
    mapping: Dict[str, str],
) -> Optional[pd.DataFrame]:
    if not items:
        return None
    dfs: List[pd.DataFrame] = []
    for file_bytes, sheet, _name, engine, _cols in items:
        raw = load_excel(file_bytes, sheet, engine=engine)
        dfs.append(apply_mapping(raw, mapping))
    if not dfs:
        return None
    return pd.concat(dfs, ignore_index=True)


def read_multi_excel(items: List[ExcelSheetItem]) -> Optional[pd.DataFrame]:
    if not items:
        return None
    dfs: List[pd.DataFrame] = []
    for file_bytes, sheet, _name, engine, _cols in items:
        dfs.append(load_excel(file_bytes, sheet, engine=engine))
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


def _parse_csv_fields(text: str) -> List[str]:
    if not text:
        return []
    parts = [p.strip() for p in str(text).replace("\n", ",").split(",")]
    return [p for p in parts if p]


def _parse_lines(text: str) -> List[str]:
    if not text:
        return []
    return [line.strip() for line in str(text).splitlines() if line.strip()]


def _unique_keep_order(items: List[str]) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def bulk_mapping_ui(
    title: str,
    available_cols: List[str],
    base_standard_fields: List[str],
    key_prefix: str,
    preset: Optional[Dict[str, str]] = None,
) -> Tuple[Dict[str, str], List[str], List[str]]:
    """Fast mapping UI.

    Returns:
      mapping: {standard_field: source_col}
      missing: kept for API symmetry (currently always [])
      standard_fields: effective standard field list (base + user extras)
    """

    if not available_cols:
        return {}, [], base_standard_fields

    with st.expander(f"{title}字段映射（批量）", expanded=False):
        auto_same_name = st.checkbox(
            "自动匹配同名字段（规范字段名在源表中存在时自动映射）",
            value=True,
            key=f"{key_prefix}_auto_same",
        )

        extra_std = st.text_input(
            "新增规范字段（逗号或换行分隔，可选）",
            value="",
            key=f"{key_prefix}_extra_std",
            help="用于临时增加规范字段，例如：体积重量、商品名称等。",
        )
        extra_std_fields = _parse_csv_fields(extra_std)
        standard_fields = _unique_keep_order(list(base_standard_fields) + extra_std_fields)

        preset = preset or {}
        preset_targets = [k for k in standard_fields if k in preset]
        preset_sources = [preset.get(k, "") for k in preset_targets]

        st.markdown("**1) 选择需要映射的规范字段**")
        tcol1, tcol2 = st.columns([1, 1])
        with tcol1:
            target_selected = st.multiselect(
                "规范字段（可多选）",
                options=standard_fields,
                default=preset_targets,
                key=f"{key_prefix}_targets_ms",
            )
        with tcol2:
            target_text_default = "\n".join(preset_targets or target_selected)
            target_order_text = st.text_area(
                "已选规范字段（可调整顺序，一行一个）",
                value=target_text_default,
                height=160,
                key=f"{key_prefix}_targets_text",
            )
        targets_ordered = _unique_keep_order(_parse_lines(target_order_text) or target_selected)

        st.markdown("**2) 选择源表字段（与上方规范字段按顺序一一对应）**")
        scol1, scol2 = st.columns([1, 1])
        with scol1:
            source_selected = st.multiselect(
                "源表字段（可多选）",
                options=available_cols,
                default=[c for c in preset_sources if c in available_cols],
                key=f"{key_prefix}_sources_ms",
            )
        with scol2:
            source_text_default = "\n".join([c for c in preset_sources if c in available_cols] or source_selected)
            source_order_text = st.text_area(
                "已选源表字段（可调整顺序，一行一个）",
                value=source_text_default,
                height=160,
                key=f"{key_prefix}_sources_text",
            )
        sources_ordered_raw = _unique_keep_order(_parse_lines(source_order_text) or source_selected)

        invalid_sources = [c for c in sources_ordered_raw if c not in set(available_cols)]
        if invalid_sources:
            st.warning(f"以下源表字段不存在，将被忽略：{invalid_sources}")
        sources_ordered = [c for c in sources_ordered_raw if c in set(available_cols)]

        if targets_ordered and sources_ordered and len(targets_ordered) != len(sources_ordered):
            st.warning(
                f"规范字段数量({len(targets_ordered)})与源表字段数量({len(sources_ordered)})不一致，将按最短长度配对。"
            )

        mapping: Dict[str, str] = {}
        for t, s in zip(targets_ordered, sources_ordered):
            if not t or not s:
                continue
            mapping[t] = s

        if auto_same_name:
            for t in standard_fields:
                if t in mapping:
                    continue
                if t in available_cols:
                    mapping[t] = t

        dup_sources = mapping_duplicates(mapping)
        if dup_sources:
            st.warning(f"检测到重复映射（多个规范字段指向同一源字段）：{dup_sources}")

        if mapping:
            preview_rows = [{"规范字段": k, "源表字段": v} for k, v in mapping.items()]
            st.dataframe(pd.DataFrame(preview_rows), use_container_width=True, hide_index=True)
        else:
            st.info("尚未生成映射：请选择规范字段与源表字段，或启用同名字段自动匹配。")

    return mapping, [], standard_fields


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


def build_multi_template(mappings: Dict[str, Dict[str, str]]) -> Dict[str, object]:
    return {
        "version": "v1",
        "scope": "multi",
        "mappings": mappings,
    }


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
            ["预估重量(kg)", "实际重量", "店铺名称"],
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
            ["预估重量(kg)", "实际重量", "店铺名称"],
            yubao_cols,
            "detail_yubao",
            preset=template_data.get("mappings", {}).get("detail_yubao"),
        )

        yubao_map_items: List[ExcelSheetItem] = []
        yubao_mapping, yubao_missing = {}, []
        if yubao_items:
            st.subheader("2. 云宝名称货品表（云宝明细必填）")
            yubao_map_items, yubao_map_cols = file_uploader_block("云宝名称货品表", "yubao_map")
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
        maozhong_items, maozhong_cols = file_uploader_block("毛重表", "maozhong")
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
        weight_items, weight_cols = file_uploader_block("重量段定义表", "weight_segments")
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
        tariff_items, tariff_cols = file_uploader_block("多条件资费表", "tariff")
        show_required_fields("多条件资费表", "tariff")
        tariff_mapping, tariff_missing = mapping_ui(
            "多条件资费表",
            REQUIRED_COLUMNS["tariff"],
            [],
            tariff_cols,
            "tariff",
            preset=template_data.get("mappings", {}).get("tariff"),
        )

        st.subheader("6. 辅助数据源")
        consumable_items, consumable_cols = file_uploader_block("耗材表（可选）", "consumables")
        consumable_mapping, _ = mapping_ui(
            "耗材表",
            [],
            REQUIRED_COLUMNS["consumables"],
            consumable_cols,
            "consumables",
            preset=template_data.get("mappings", {}).get("consumables"),
        )

        tear_items, tear_cols = file_uploader_block("撕单表（可选）", "tear")
        tear_mapping, _ = mapping_ui(
            "撕单表",
            REQUIRED_COLUMNS["tear"],
            [],
            tear_cols,
            "tear",
            preset=template_data.get("mappings", {}).get("tear"),
        )

        after_items, after_cols = file_uploader_block("售后赔付表（可选）", "aftersale")
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

        weight_source_label = st.radio(
            "重量来源",
            options=[
                "使用毛重表核算重量（kg）",
                "使用发货明细预估重量（kg）",
            ],
            index=0,
            help=(
                "毛重表核算重量：按 数量×毛重(g)/1000 计算；\n"
                "发货明细预估重量：使用明细中的‘预估重量(kg)’（需在字段映射里选择/映射）。"
            ),
        )
        weight_source = (
            WEIGHT_SOURCE_MAOZHONG_CALC
            if weight_source_label.startswith("使用毛重表")
            else WEIGHT_SOURCE_DETAIL_ESTIMATED
        )

        wdt_estimated_weight_col: Optional[str] = None
        yubao_estimated_weight_col: Optional[str] = None
        if weight_source == WEIGHT_SOURCE_DETAIL_ESTIMATED:
            st.markdown("**发货明细预估重量列选择**")
            st.caption("启用‘使用发货明细预估重量’时，需要从发货明细表头中选择一列作为预估重量(kg)。")

            if wdt_items and wdt_cols:
                options = ["--请选择--"] + list(wdt_cols)
                preset_value = (wdt_mapping or {}).get("预估重量(kg)")
                default_index = options.index(preset_value) if preset_value in options else 0
                wdt_estimated_weight_col = st.selectbox(
                    "旺店通发货明细：预估重量列（kg）",
                    options=options,
                    index=default_index,
                    key="wdt_estimated_weight_col",
                )

            if yubao_items and yubao_cols:
                options = ["--请选择--"] + list(yubao_cols)
                preset_value = (yubao_detail_mapping or {}).get("预估重量(kg)")
                default_index = options.index(preset_value) if preset_value in options else 0
                yubao_estimated_weight_col = st.selectbox(
                    "云宝发货明细：预估重量列（kg）",
                    options=options,
                    index=default_index,
                    key="yubao_estimated_weight_col",
                )

        match_packed = st.checkbox(
            "按是否打包品匹配资费",
            value=True,
            help="勾选后会根据毛重表‘箱规’判断每行是否为打包品；不勾选则使用默认值或忽略该条件。",
        )
        if match_packed:
            pack_rule = PACK_RULE_MATCH
            st.caption("已启用打包品判断：需要上传毛重表（含箱规）。")
        else:
            packed_default = st.radio(
                "未启用打包品匹配时：默认按哪种口径计算？",
                options=[
                    "默认打包品",
                    "默认非打包品",
                    "不限（忽略是否打包品条件）",
                ],
                index=2,
            )
            if packed_default == "默认打包品":
                pack_rule = PACK_RULE_FIXED_PACKED
            elif packed_default == "默认非打包品":
                pack_rule = PACK_RULE_FIXED_NON_PACKED
            else:
                pack_rule = PACK_RULE_IGNORE

        enable_deductions = st.checkbox("启用撕单/售后扣款", value=True)
        enable_consumables = st.checkbox("启用耗材计算", value=False)

        st.divider()

        if st.button("开始对账", type="primary"):
            has_detail = bool(wdt_items) or bool(yubao_items)

            need_maozhong = has_detail and (
                (weight_source == WEIGHT_SOURCE_MAOZHONG_CALC) or (pack_rule == PACK_RULE_MATCH)
            )
            missing_files = []
            if not bill_items:
                missing_files.append("云仓账单")
            if has_detail:
                if need_maozhong and (not maozhong_items):
                    missing_files.append("毛重表")
                if not weight_items:
                    missing_files.append("重量段定义表")
                if not tariff_items:
                    missing_files.append("多条件资费表")
                if yubao_items and not yubao_map_items:
                    missing_files.append("云宝名称货品表")

            if missing_files:
                st.error(f"缺少必选文件：{'、'.join(missing_files)}")
                return

            mapping_missing = bill_missing
            if has_detail:
                if need_maozhong:
                    mapping_missing += maozhong_missing
                mapping_missing += weight_missing + tariff_missing
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
                    ("重量段定义表", weight_mapping),
                    ("多条件资费表", tariff_mapping),
                ]
                if need_maozhong:
                    mapping_sets.append(("毛重表", maozhong_mapping))
                if wdt_items:
                    mapping_sets.append(("旺店通发货明细", wdt_mapping))
                if yubao_items:
                    mapping_sets.append(("云宝发货明细", yubao_detail_mapping))
                    mapping_sets.append(("云宝名称货品表", yubao_mapping))

            # If using detail-estimated weight, inject the selected source column into mappings.
            # This ensures usecols + rename produce standard field '预估重量(kg)' for downstream logic.
            if has_detail and weight_source == WEIGHT_SOURCE_DETAIL_ESTIMATED:
                if wdt_items:
                    if not wdt_estimated_weight_col or wdt_estimated_weight_col == "--请选择--":
                        st.error("已选择‘使用发货明细预估重量’，请为【旺店通发货明细】选择预估重量列")
                        return
                    wdt_mapping["预估重量(kg)"] = wdt_estimated_weight_col
                if yubao_items:
                    if not yubao_estimated_weight_col or yubao_estimated_weight_col == "--请选择--":
                        st.error("已选择‘使用发货明细预估重量’，请为【云宝发货明细】选择预估重量列")
                        return
                    yubao_detail_mapping["预估重量(kg)"] = yubao_estimated_weight_col

            for title, mapping in mapping_sets:
                duplicates = mapping_duplicates(mapping)
                if duplicates:
                    st.error(f"{title}映射存在重复列：{'、'.join(duplicates)}")
                    return

            with st.spinner("正在读取并映射数据..."):
                bill_df = read_and_map_multi_with_progress(
                    items=bill_items,
                    mapping=bill_mapping,
                    title="云仓账单",
                )
                wdt_df = read_and_map_multi_with_progress(
                    items=wdt_items,
                    mapping=wdt_mapping,
                    title="旺店通发货明细",
                )
                yubao_detail_df = read_and_map_multi_with_progress(
                    items=yubao_items,
                    mapping=yubao_detail_mapping,
                    title="云宝发货明细",
                )
                maozhong_df = read_and_map_multi_with_progress(
                    items=maozhong_items,
                    mapping=maozhong_mapping,
                    title="毛重表",
                )
                weight_df = read_and_map_multi_with_progress(
                    items=weight_items,
                    mapping=weight_mapping,
                    title="重量段定义表",
                )
                tariff_df = read_and_map_multi_with_progress(
                    items=tariff_items,
                    mapping=tariff_mapping,
                    title="多条件资费表",
                )
                yubao_map_df = read_and_map_multi_with_progress(
                    items=yubao_map_items,
                    mapping=yubao_mapping,
                    title="云宝名称货品表",
                )
                consumable_df = read_and_map_multi_with_progress(
                    items=consumable_items,
                    mapping=consumable_mapping,
                    title="耗材表",
                )
                tear_df = read_and_map_multi_with_progress(
                    items=tear_items,
                    mapping=tear_mapping,
                    title="撕单表",
                )
                after_df = read_and_map_multi_with_progress(
                    items=after_items,
                    mapping=after_mapping,
                    title="售后赔付表",
                )

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
                if need_maozhong:
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
                weight_source=weight_source,
                pack_rule=pack_rule,
                enable_deductions=enable_deductions,
                enable_consumables=enable_consumables,
                clean_province=clean_province,
            )

            if has_detail and weight_source == WEIGHT_SOURCE_DETAIL_ESTIMATED:
                if detail_df is None or detail_df.empty:
                    st.error("选择了发货明细预估重量，但发货明细为空")
                    return
                if "预估重量(kg)" not in detail_df.columns:
                    st.error("选择了发货明细预估重量，但明细中未生成规范字段：预估重量(kg)。请在参数区选择预估重量列，或在字段映射中映射到‘预估重量(kg)’")
                    return

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

        st.markdown("**字段对账（简化版）**")
        st.caption(
            "上传前请先在源表中手动把列名规范化为标准字段名（本工具不会自动清洗/改列名）。"
            "上传后仅提示哪些字段未命中；全部命中后才能生成导出。"
        )

        mw_extra_std = st.text_input(
            "新增规范字段（逗号或换行分隔，可选）",
            value="",
            key="mw_std_extra",
            help="例如：体积重量、SKU名称等。",
        )
        mw_std_all = _unique_keep_order(MULTI_STANDARD_FIELDS + _parse_csv_fields(mw_extra_std))

        mw_selected_std = ordered_multiselect(
            "选择需要对账/导出的规范字段（按勾选先后顺序导出）",
            options=mw_std_all,
            default=[
                "云仓",
                "物流单号",
                "店铺名称",
                "收货省份",
                "结算重量(取整)",
                "账单快递费",
            ],
            key="mw_std_fields",
        )

        reconcile_items_named, reconcile_cols = file_uploader_multi_block_named(
            "对账结果文件（可多选）",
            "mw_reconcile_files",
        )

        reconcile_missing_by_file: Dict[str, List[str]] = {}
        if reconcile_items_named and mw_selected_std:
            for _file_bytes, sheet, name, _engine, cols in reconcile_items_named:
                display_name = f"{name}::{sheet}"
                missing = build_per_file_hit_status_ui(
                    file_name=display_name,
                    file_cols=cols,
                    standard_fields_ordered=mw_selected_std,
                )
                if missing:
                    reconcile_missing_by_file[display_name] = missing

        selected_fields = list(mw_selected_std)

        max_rows_per_sheet = st.number_input(
            "分页阈值（行数，超过则按云仓分页导出）",
            min_value=1,
            value=800_000,
            step=50_000,
            key="mw_summary_max_rows",
        )

        if st.button("生成多仓汇总表", type="primary", key="mw_build_summary"):
            if not reconcile_items_named:
                st.error("请先上传至少一个对账结果文件")
                return
            if not selected_fields:
                st.error("请至少选择 1 个导出字段")
                return

            if reconcile_missing_by_file:
                lines = [f"- {name}：{ '、'.join(miss) }" for name, miss in reconcile_missing_by_file.items()]
                st.error("以下文件字段未全部命中，请先修改源表列名后重新上传：\n" + "\n".join(lines))
                return

            try:
                progress = st.progress(0)
                status = st.empty()
                dfs: List[pd.DataFrame] = []
                total = len(reconcile_items_named)
                usecols = tuple(selected_fields)
                for idx, (b, s, n, e, _c) in enumerate(reconcile_items_named, start=1):
                    status.write(f"正在读取明细数据：{n}::{s}（{idx}/{total}）")
                    dfs.append(load_excel_usecols(b, s, e, usecols))
                    progress.progress(min(1.0, idx / max(1, total)))
                status.empty()
                df_all = pd.concat(dfs, ignore_index=True)
            except KeyError as e:
                st.error(f"生成失败：存在未命中的字段（请先修改源表列名后重新上传）。错误：{e}")
                return
            if df_all is None or df_all.empty:
                st.error("读取结果为空，请检查所选 Sheet")
                return

            export_df = df_all.copy()

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

        new_items_named, new_cols = file_uploader_multi_block_named(
            "新复核结果表（可多选）",
            "mw_recharge_new",
        )
        old_items_named, old_cols = file_uploader_block(
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

        # Hit-check for recharge (only show missing; no manual mapping)
        key_col = "物流单号"
        recharge_required = [key_col] + list(compare_fields)
        new_missing_by_file: Dict[str, List[str]] = {}
        if new_items_named and recharge_required:
            for _file_bytes, sheet, name, _engine, cols in new_items_named:
                display_name = f"{name}::{sheet}"
                missing = build_per_file_hit_status_ui(
                    file_name=display_name,
                    file_cols=cols,
                    standard_fields_ordered=recharge_required,
                )
                if missing:
                    new_missing_by_file[display_name] = missing

        old_missing_by_file: Dict[str, List[str]] = {}
        if old_items_named and recharge_required:
            for _file_bytes, sheet, name, _engine, cols in old_items_named:
                display_name = f"{name}::{sheet}"
                missing = build_per_file_hit_status_ui(
                    file_name=f"旧汇总/{display_name}",
                    file_cols=cols,
                    standard_fields_ordered=recharge_required,
                )
                if missing:
                    old_missing_by_file[display_name] = missing

        if st.button("生成回冲差异表", type="primary", key="mw_build_recharge"):
            if not new_items_named:
                st.error("请上传新复核结果表")
                return
            if not old_items_named:
                st.error("请上传旧汇总表")
                return
            if not compare_fields:
                st.error("请至少选择 1 个对比字段")
                return

            if new_missing_by_file or old_missing_by_file:
                lines: List[str] = []
                if new_missing_by_file:
                    lines += [f"- 新复核/{name}：{ '、'.join(miss) }" for name, miss in new_missing_by_file.items()]
                if old_missing_by_file:
                    lines += [f"- 旧汇总/{name}：{ '、'.join(miss) }" for name, miss in old_missing_by_file.items()]
                st.error("字段未全部命中，请先修改源表列名后重新上传：\n" + "\n".join(lines))
                return

            try:
                progress = st.progress(0)
                status = st.empty()
                dfs_new: List[pd.DataFrame] = []
                total = len(new_items_named)
                usecols = tuple(recharge_required)
                for idx, (b, s, n, e, _c) in enumerate(new_items_named, start=1):
                    status.write(f"正在读取新复核明细：{n}::{s}（{idx}/{total}）")
                    dfs_new.append(load_excel_usecols(b, s, e, usecols))
                    progress.progress(min(1.0, idx / max(1, total)))
                status.empty()
                new_df = pd.concat(dfs_new, ignore_index=True)
            except KeyError as e:
                st.error(f"新复核结果缺少必需字段（请先修改源表列名后重新上传）。错误：{e}")
                return

            try:
                progress_old = st.progress(0)
                status_old = st.empty()
                dfs_old: List[pd.DataFrame] = []
                total_old = len(old_items_named)
                usecols_old = tuple(recharge_required)
                for idx, (b, s, n, e, _c) in enumerate(old_items_named, start=1):
                    status_old.write(f"正在读取旧汇总明细：{n}::{s}（{idx}/{total_old}）")
                    dfs_old.append(load_excel_usecols(b, s, e, usecols_old))
                    progress_old.progress(min(1.0, idx / max(1, total_old)))
                status_old.empty()
                old_df = pd.concat(dfs_old, ignore_index=True)
            except Exception as e:
                st.error(f"旧汇总表读取失败（请确认文件未损坏，且列名已规范化）。错误：{e}")
                return
            if new_df is None or new_df.empty:
                st.error("新复核结果读取为空，请检查所选 Sheet")
                return
            if old_df is None or old_df.empty:
                st.error("旧汇总表读取为空，请检查所选 Sheet")
                return

            if key_col not in new_df.columns or key_col not in old_df.columns:
                st.error("新旧表均需包含列：物流单号。请在源表中把列名改为“物流单号”后重新上传。")
                return

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

        analysis_items, analysis_cols = file_uploader_block(
            "对账结果表（单文件，可多选Sheet）",
            "analysis_file",
        )
        if not analysis_items:
            st.info("请先上传对账结果表")
        elif not analysis_cols:
            st.error("读取表头失败：未获取到字段列名")
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
                if not analysis_items:
                    st.error("请先上传对账结果表")
                    return

                try:
                    if len(analysis_items) == 1:
                        with st.spinner("正在读取明细数据..."):
                            with st.spinner("正在读取对账结果表..."):
                                analysis_df = read_df_items_with_progress(items=analysis_items, title="对账结果表")
                    else:
                        progress = st.progress(0)
                        status = st.empty()
                        dfs: List[pd.DataFrame] = []
                        total = len(analysis_items)
                        for idx, (b, s, n, e, _c) in enumerate(analysis_items, start=1):
                            status.write(f"正在读取明细数据：{n}::{s}（{idx}/{total}）")
                            dfs.append(load_excel(b, s, engine=e))
                            progress.progress(min(1.0, idx / max(1, total)))
                        status.empty()
                        analysis_df = pd.concat(dfs, ignore_index=True) if dfs else None
                except Exception as e:
                    st.error(f"读取对账结果表失败：{e}")
                    return

                if analysis_df is None or analysis_df.empty:
                    st.error("对账结果表为空，请检查所选 Sheet")
                    return

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

        bill_items, bill_cols = file_uploader_block(
            "结果表（单文件，可多选Sheet）",
            "bill_summary_file",
        )
        if not bill_items:
            st.info("请先上传结果表")
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
            try:
                if len(bill_items) == 1:
                    with st.spinner("正在读取明细数据..."):
                        with st.spinner("正在读取结果表..."):
                            result_df = read_df_items_with_progress(items=bill_items, title="结果表")
                else:
                    progress = st.progress(0)
                    status = st.empty()
                    dfs: List[pd.DataFrame] = []
                    total = len(bill_items)
                    for idx, (b, s, n, e, _c) in enumerate(bill_items, start=1):
                        status.write(f"正在读取明细数据：{n}::{s}（{idx}/{total}）")
                        dfs.append(load_excel(b, s, engine=e))
                        progress.progress(min(1.0, idx / max(1, total)))
                    status.empty()
                    result_df = pd.concat(dfs, ignore_index=True) if dfs else None
            except Exception as e:
                st.error(f"读取结果表失败：{e}")
                return

            if result_df is None or result_df.empty:
                st.error("结果表为空，请检查所选 Sheet")
                return

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
