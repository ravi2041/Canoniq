
import yaml
import re, uuid, json, ast
from pathlib import Path


def load_yaml_config(file_name: str):
    base = Path(__file__).resolve().parents[1] / "config"
    path = base / file_name
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# Load once at startup
ACTION_KEYWORDS = load_yaml_config("../config/actions.yaml")["actions"]
DIMENSION_KEYWORDS = load_yaml_config("dimensions.yaml")["dimensions"]


# -------- Patterns --------
_PCT = re.compile(r'(\d+(?:\.\d+)?)\s*%')
_MONEY = re.compile(r'[$€£]?\s*(\d{1,3}(?:[,\s]\d{3})*(?:\.\d+)?|\d+(?:\.\d+)?)')
_PLACE_ID = re.compile(r'(?:placement|ad\s?set|ad)\s+(\d{6,})', re.IGNORECASE)


def _parse_input(raw):
    """Accepts list[dict], dict, JSON string, or Python-literal string. Always returns a list of dicts."""
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        return [raw]
    if isinstance(raw, str):
        s = raw.strip()
        # Try JSON first
        try:
            val = json.loads(s)
            return val if isinstance(val, list) else [val]
        except Exception:
            # Fallback: Python literal
            try:
                val = ast.literal_eval(s)
                return val if isinstance(val, list) else [val]
            except Exception:
                return []
    return []


def _clean(s: str) -> str:
    if not s: return ""
    s = s.replace("′","'").replace("\u200b","")
    return re.sub(r'\s+', ' ', s).strip()

def _dir_from_text(s: str) -> str:
    s = s.lower()
    if any(kw in s for kw in ("↑", "up", "increase", "grow", "boost")):
        return "up"
    if any(kw in s for kw in ("↓", "down", "reduce", "decrease", "decline", "cut")):
        return "down"
    return "unknown"


def _mag_from_text(s: str) -> str:
    s = s.lower()
    if any(kw in s for kw in ("large", "high", "significant", "strong", "major")):
        return "large"
    if any(kw in s for kw in ("moderate", "medium", "average", "fair")):
        return "moderate"
    if any(kw in s for kw in ("small", "low", "slight", "minor", "minimal")):
        return "small"
    return "unknown"


def _dimension_from_text(s: str) -> str:
    s = s.lower()
    for kw, dim in DIMENSION_KEYWORDS.items():
        if kw in s:
            return dim
    return "ENGAGEMENT"


def _parse_action(a):
    if isinstance(a, dict):
        return a
    t = _clean(str(a)).lower()

    verb = "review"
    for kw, mapped in ACTION_KEYWORDS.items():
        if kw in t:
            verb = mapped
            break

    # Extract numbers, IDs
    m_pct = _PCT.search(t)
    delta_pct = int(float(m_pct.group(1))) if m_pct else None
    m_id = _PLACE_ID.search(t)
    entity_id = m_id.group(1) if m_id else None

    params = {
        "delta_pct": delta_pct,
        "entity_type": "placement" if entity_id else None,
        "entity_id": entity_id,
    }
    params = {k: v for k, v in params.items() if v not in (None, "")}

    return {"verb": verb, "params": params}


def _has_metric(collected, metric_label: str) -> bool:
    """Check if a metric with same label already exists (case-insensitive)."""
    return any(m.get("metric", "").lower() == metric_label.lower() for m in collected)


def _add_metric(collected, metric_label: str, formatted_value: str):
    """Add metric only if non-empty and not duplicate."""
    if formatted_value and not _has_metric(collected, metric_label):
        collected.append({"metric": metric_label.upper(), "formatted": formatted_value})


def _extract_first_pct(text: str):
    """Extract first percentage value from text, e.g., '48.7%'."""
    m = _PCT.search(text or "")
    return f"{m.group(1)}%" if m else None


def _extract_first_money(text: str):
    """Extract first currency/amount value from text, e.g., '$1.44'."""
    m = _MONEY.search(text or "")
    if not m:
        return None
    val = m.group(1).replace(",", "").strip()
    return f"${val}"  # default normalize to dollars (can extend with currency detection)

def _normalize_metrics(metrics_list, why_text):
    """
    Accepts metrics as a list of strings or dicts.
    Returns a deduped list with detected metrics extracted from both metrics and 'why',
    using DIMENSION_KEYWORDS from YAML config.
    """
    out = []
    lst = metrics_list or []

    # Collect snippets for regex scanning
    snippet_parts = []
    for item in lst:
        if isinstance(item, dict) and "metric" in item and "formatted" in item:
            _add_metric(out, item["metric"], _clean(item["formatted"]))
            snippet_parts.append(str(item.get("formatted", "")))
        else:
            txt = _clean(str(item))
            if txt:
                out.append({"metric": "snippet", "formatted": txt})
                snippet_parts.append(txt)

    blob = " | ".join(snippet_parts + [why_text or ""])

    # Dynamically check all dimensions from YAML config
    for kw, dim in DIMENSION_KEYWORDS.items():
        # percentage style metric
        if dim in ("CTR", "CVR", "ENGAGEMENT", "VIEW_RATE"):
            m = re.search(kw + r"[^0-9%]*" + _PCT.pattern, blob, re.IGNORECASE)
            if m:
                _add_metric(out, dim, f"{m.group(1)}%")
        # money style metric
        elif dim in ("CPC", "CPA", "CPM", "CPL", "ROAS", "REVENUE", "CPV"):
            m = re.search(kw + r"[^0-9$€£]*" + _MONEY.pattern, blob, re.IGNORECASE)
            if m:
                _add_metric(out, dim, f"${m.group(1).replace(',', '').strip()}")
        # plain numbers (like impressions, reach, frequency)
        elif dim in ("IMPRESSIONS", "REACH", "FREQUENCY"):
            m = re.search(kw + r"[^0-9]*([0-9,]+)", blob, re.IGNORECASE)
            if m:
                val = m.group(1).replace(",", "").strip()
                _add_metric(out, dim, val)

    return out


def format_recommendations(raw_recs):
    """Accepts str | list | dict and returns a normalized list ready for UI cards."""
    items = _parse_input(raw_recs)
    norm = []
    for r in items:
        if not isinstance(r, dict):
            continue
        title = _clean(r.get("title",""))
        why = _clean(r.get("why",""))

        # expected_impact can be dict or string
        ei_field = r.get("expected_impact")
        if isinstance(ei_field, dict):
            expected = {
                "dimension": (ei_field.get("dimension") or "ENGAGEMENT").upper(),
                "direction": (ei_field.get("direction") or "unknown").lower(),
                "magnitude": (ei_field.get("magnitude") or "unknown").lower(),
            }
        else:
            ei = _clean(str(ei_field or ""))
            expected = {
                "dimension": _dimension_from_text(ei),
                "direction": _dir_from_text(ei),
                "magnitude": _mag_from_text(ei)
            }

        action = _parse_action(r.get("action", ""))

        rec = {
            "id": r.get("id") or f"rec_{uuid.uuid4().hex[:8]}",
            "title": title,
            "action": action,
            "why": why,
            "expected_impact": expected,
            "confidence": (r.get("confidence") or "medium").lower(),
            "metrics_cited": _normalize_metrics(r.get("metrics_cited", []), why),
            "reasoning": r.get("reasoning", []),
            "entities": r.get("entities", [])
        }
        norm.append(rec)
    return norm


# ---- Example run ----
if __name__ == "__main__":
    sample = "[{'id': 'rec_199ea49c', 'title': 'Scale CM360 budget', 'action': {'verb': 'review', 'params': {}}, 'why': 'CM360’s high CVR of 48.7% and low CPA of 1.72 indicate strong conversion efficiency', 'expected_impact': {'dimension': 'ENGAGEMENT', 'direction': 'up', 'magnitude': 'large'}, 'confidence': 'high', 'metrics_cited': [{'metric': 'snippet', 'formatted': 'CM360 CVR 48.7%'}, {'metric': 'snippet', 'formatted': 'CPA 1.72'}], 'entities': []}, {'id': 'rec_400548d3', 'title': 'Expand TikTok spend and enable conversions', 'action': {'verb': 'review', 'params': {}}, 'why': 'TikTok delivered the highest social CTR (0.43%) and lowest CPC (0.76), but no conversions tracked', 'expected_impact': {'dimension': 'ENGAGEMENT', 'direction': 'up', 'magnitude': 'moderate'}, 'confidence': 'medium', 'metrics_cited': [{'metric': 'snippet', 'formatted': 'TikTok CTR 0.43%'}, {'metric': 'snippet', 'formatted': 'CPC 0.76'}, {'metric': 'CTR', 'formatted': '0.43%'}, {'metric': 'CPC', 'formatted': '0.76'}], 'entities': []}, {'id': 'rec_f4b30dc7', 'title': 'Optimize Facebook targeting and creatives', 'action': {'verb': 'review', 'params': {}}, 'why': 'Facebook’s moderate CTR of 0.38% and CPC of 1.44 suggest room for engagement improvement', 'expected_impact': {'dimension': 'CTR', 'direction': 'up', 'magnitude': 'small'}, 'confidence': 'medium', 'metrics_cited': [{'metric': 'snippet', 'formatted': 'Facebook CTR 0.38%'}, {'metric': 'snippet', 'formatted': 'CPC 1.44'}, {'metric': 'CTR', 'formatted': '0.38%'}, {'metric': 'CPC', 'formatted': '1.44'}], 'entities': []}, {'id': 'rec_e534ade8', 'title': 'Reallocate YouTube budget', 'action': {'verb': 'review', 'params': {'delta_pct': 20}}, 'why': 'YouTube has the lowest CTR (0.24%) and highest CPC (3.84), limiting efficiency', 'expected_impact': {'dimension': 'ENGAGEMENT', 'direction': 'down', 'magnitude': 'moderate'}, 'confidence': 'high', 'metrics_cited': [{'metric': 'snippet', 'formatted': 'YouTube CTR 0.24%'}, {'metric': 'snippet', 'formatted': 'CPC 3.84'}, {'metric': 'CTR', 'formatted': '0.24%'}, {'metric': 'CPC', 'formatted': '$3.84'}], 'entities': []}, {'id': 'rec_cb308ad2', 'title': 'Standardize campaign_name for cross-platform conversion', 'action': {'verb': 'review', 'params': {}}, 'why': 'Social platforms show zero conversions due to missing cross-platform attribution', 'expected_impact': {'dimension': 'CPA', 'direction': 'up', 'magnitude': 'small'}, 'confidence': 'high', 'metrics_cited': [{'metric': 'snippet', 'formatted': 'Conversions on social platforms = 0'}], 'entities': []}]"
    out = format_recommendations(sample)
    print("OUTPUT:\n", out)
