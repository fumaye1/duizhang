from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class HttpMetricConfig:
    """Generic HTTP metric fetch config.

    This is intentionally generic because WPS 'multi-dimensional table' API
    payloads vary by tenant/app configuration.

    - headers_json/body_json are JSON objects encoded as strings.
    - value_path supports a simple dotted path with optional [index], e.g.
      data.items[0].amount
    """

    method: str
    url: str
    headers_json: str = "{}"
    body_json: str = "{}"
    value_path: str = "value"
    timeout_seconds: int = 20


_PATH_TOKEN_RE = re.compile(r"(?P<key>[^.\[]+)(\[(?P<index>\d+)\])?")


def _parse_json_object(text: str) -> Dict[str, Any]:
    if not text or not str(text).strip():
        return {}
    obj = json.loads(text)
    if not isinstance(obj, dict):
        raise ValueError("headers/body 必须是JSON对象")
    return obj


def extract_by_path(payload: Any, path: str) -> Any:
    current = payload
    path = str(path or "").strip()
    if not path:
        return current

    for part in path.split("."):
        part = part.strip()
        if not part:
            continue

        match = _PATH_TOKEN_RE.fullmatch(part)
        if not match:
            raise ValueError(f"不支持的 value_path 片段：{part}")

        key = match.group("key")
        index_raw = match.group("index")

        if isinstance(current, dict):
            current = current.get(key)
        else:
            raise ValueError(f"无法从非对象取key={key}")

        if index_raw is not None:
            if not isinstance(current, list):
                raise ValueError(f"无法对非数组取下标：{key}[{index_raw}]")
            idx = int(index_raw)
            if idx < 0 or idx >= len(current):
                raise ValueError(f"数组下标越界：{key}[{idx}]")
            current = current[idx]

    return current


def fetch_metric(config: HttpMetricConfig) -> float:
    try:
        import requests  # type: ignore
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("缺少依赖 requests，请先安装") from exc

    headers = _parse_json_object(config.headers_json)
    body = _parse_json_object(config.body_json)

    method = str(config.method or "GET").upper()
    if method not in {"GET", "POST"}:
        raise ValueError("method 仅支持 GET / POST")

    if method == "GET":
        resp = requests.get(config.url, headers=headers, params=body, timeout=config.timeout_seconds)
    else:
        resp = requests.post(config.url, headers=headers, json=body, timeout=config.timeout_seconds)

    resp.raise_for_status()

    payload = resp.json()
    value = extract_by_path(payload, config.value_path)
    try:
        return float(value)
    except Exception as exc:
        raise ValueError(f"提取到的值无法转为数字：{value}") from exc
