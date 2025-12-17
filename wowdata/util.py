from __future__ import annotations

from pathlib import Path
from typing import Optional, Dict, Any

from wowdata.errors import WowDataUserError


def _is_probably_url(s: str) -> bool:
    return s.startswith("http://") or s.startswith("https://") or s.startswith("s3://")


def _norm_path(p: str, *, base_dir: Optional[Path]) -> str:
    """Normalize a URI/path relative to a base directory (when provided).

    - Leaves absolute paths and URLs unchanged.
    - If base_dir is provided and p is relative, returns an absolute resolved path.
    """
    if not isinstance(p, str) or not p:
        return p
    if _is_probably_url(p):
        return p
    try:
        pp = Path(p)
    except Exception:
        return p
    if pp.is_absolute():
        return str(pp)
    if base_dir is None:
        return p
    return str((base_dir / pp).resolve())


def _infer_type_from_uri(uri: str) -> Optional[str]:
    ext = Path(uri).suffix.lower()
    if ext == ".csv":
        return "csv"
    # todo: extend later: .xlsx, .parquet, db urls, s3://, http(s)://, etc.
    return None


FrictionlessSchema = Dict[str, Any]


def _normalize_inline_schema(schema: Dict[str, Any]) -> Dict[str, Any]:
    """
    Expect frictionless-ish schema:
      {"fields":[{"name":"age","type":"integer"}, ...]}
    We keep it permissive for now.
    """
    if not isinstance(schema, dict):
        raise WowDataUserError(
            "E_SCHEMA_INLINE_TYPE",
            "Inline schema must be a mapping (dict).",
            hint="Example: {'fields': [{'name': 'age', 'type': 'integer'}]}",
        )
    if "fields" not in schema:
        # allow passing full frictionless resource descriptor later; for now keep simple
        return {"fields": []}
    if not isinstance(schema["fields"], list):
        raise WowDataUserError(
            "E_SCHEMA_INLINE_FIELDS",
            "Inline schema['fields'] must be a list.",
            hint="Example: {'fields': [{'name': 'age', 'type': 'integer'}]}",
        )
    return schema
