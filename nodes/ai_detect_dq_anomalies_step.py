# nodes/ai_detect_dq_anomalies_step.py
import os
import json
from collections import defaultdict
from datetime import datetime, timezone

from core.state import AgentState
from chains.ai_dq_pattern_chain import ai_dq_pattern_chain
from app.observability.observability import app_log

PATTERN_MEMORY_FILE = "dq_pattern_memory.json"


def _load_pattern_memory_from_file() -> dict:
    """
    Load persisted AI-learned patterns (e.g. known naming patterns, common anomalies)
    from disk so the AI can build on past runs.
    """
    if os.path.exists(PATTERN_MEMORY_FILE):
        try:
            with open(PATTERN_MEMORY_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def _save_pattern_memory_to_file(pattern_memory: dict) -> None:
    """
    Persist updated pattern memory back to disk.
    """
    try:
        with open(PATTERN_MEMORY_FILE, "w", encoding="utf-8") as f:
            json.dump(pattern_memory, f, indent=2)
    except Exception:
        # Don't kill the flow if we can't write; just log and move on.
        pass


def _sample_dq_dimensions(dq_dimensions: dict, max_per_dim: int = 300) -> dict:
    """
    Take a sample of dimension values per dimension to keep payload LLM-friendly.
    dq_dimensions structure is expected to be:

        {
          "campaign": [
            {
              "value": "...",
              "table": "...",
              "column": "...",
              "platform": "...",
              "entity": "...",
              "canonical_key": "..."
            },
            ...
          ],
          "creative": [...],
          ...
        }

    We'll keep up to `max_per_dim` entries per dimension, but try to retain
    diversity across platforms.
    """
    sampled = {}

    for dim, entries in dq_dimensions.items():
        if not isinstance(entries, list):
            continue

        # group per platform so we don't only sample from the largest one
        by_platform = defaultdict(list)
        for e in entries:
            platform = (e.get("platform") or "unknown").lower()
            by_platform[platform].append(e)

        dim_sample = []
        if not by_platform:
            sampled[dim] = []
            continue

        # distribute sampling across platforms
        platforms = list(by_platform.keys())
        per_platform_quota = max(1, max_per_dim // max(len(platforms), 1))

        for plat in platforms:
            plat_entries = by_platform[plat]
            # simple slice as sample; you can random.sample if you prefer
            dim_sample.extend(plat_entries[:per_platform_quota])
            if len(dim_sample) >= max_per_dim:
                break

        sampled[dim] = dim_sample[:max_per_dim]

    return sampled


def _build_dimension_context(dq_dimensions: dict) -> dict:
    """
    Build a compact, platform-aware / canonical-aware summary for the LLM.

    Output shape:

    {
      "campaign": [
        {
          "platform": "facebook_ads",
          "entity": "campaign",
          "canonical_key": "campaign_name",
          "unique_values": 123,
          "example_values": ["BF_SALE_2025", "BrandX_NZ_Install_202511"],
          "tables": ["facebook_ads_insights"],
          "columns": ["campaign_name"]
        },
        ...
      ],
      "creative": [...],
      "placement": [...],
      "site": [...]
    }
    """
    context = {}

    for dim, entries in dq_dimensions.items():
        if not isinstance(entries, list):
            context[dim] = []
            continue

        # group by (platform, entity, canonical_key)
        group_map = {}
        for e in entries:
            platform = (e.get("platform") or "unknown").lower()
            entity = (e.get("entity") or "").lower() or None
            canonical_key = e.get("canonical_key")

            key = (platform, entity, canonical_key)
            if key not in group_map:
                group_map[key] = {
                    "platform": platform,
                    "entity": entity,
                    "canonical_key": canonical_key,
                    "values": set(),
                    "tables": set(),
                    "columns": set(),
                }

            gm = group_map[key]
            gm["values"].add(str(e.get("value", "")).strip())
            if e.get("table"):
                gm["tables"].add(e["table"])
            if e.get("column"):
                gm["columns"].add(e["column"])

        dim_groups = []
        for (platform, entity, canonical_key), gm in group_map.items():
            values_list = [v for v in gm["values"] if v]  # remove empty strings
            example_values = values_list[:10]  # small sample for the prompt

            dim_groups.append({
                "platform": platform,
                "entity": entity,
                "canonical_key": canonical_key,
                "unique_values": len(values_list),
                "example_values": example_values,
                "tables": sorted(gm["tables"]),
                "columns": sorted(gm["columns"]),
            })

        context[dim] = dim_groups

    return context


def ai_detect_dq_anomalies_step(state: AgentState) -> dict:
    """
    Run AI-based pattern analysis over collected dimension values.

    Now leverages:
      - dq_dimensions enriched with platform/entity/canonical_key
      - a compact dimension_context for cross-platform & canonical analysis
      - pattern_memory to accumulate learned patterns over runs
    """
    run_id = state.get("run_id")
    dq_dimensions = state.get("dq_dimensions", {})

    if not dq_dimensions:
        app_log(
            "ai_detect_dq_anomalies_step_error",
            run_id=run_id,
            error="Missing dq_dimensions in state.",
        )
        state["dq_ai_findings"] = {
            "summary": "No dimensions found for analysis.",
            "findings": [],
        }
        return state

    # bring in the metadata created earlier
    dq_metadata = state.get("dq_metadata") or {}

    # 1) Load / initialise pattern memory
    pattern_memory = state.get("dq_pattern_memory")
    if pattern_memory is None:
        pattern_memory = _load_pattern_memory_from_file()

    # 2) Build LLM-friendly inputs
    sampled_dimensions = _sample_dq_dimensions(dq_dimensions, max_per_dim=300)
    dimension_context = _build_dimension_context(dq_dimensions)

    app_log("ai_detect_dq_anomalies_step_start", run_id=run_id)

    # 3) Call the LLM chain
    chain = ai_dq_pattern_chain()
    result = chain.invoke({
        # keep same key name as before for backward compatibility
        "dq_dimensions": json.dumps(sampled_dimensions, indent=2),
        "dimension_context": json.dumps(dimension_context, indent=2),
        "pattern_memory": json.dumps(pattern_memory, indent=2),
        "metadata": json.dumps(dq_metadata, indent=2),
        "include_table_info": True,  # instructs the chain to consider table/platform info
    })

    # 4) Persist updated pattern memory if the chain returns it
    updated_memory = result.get("updated_pattern_memory")
    if updated_memory:
        pattern_memory = updated_memory
        _save_pattern_memory_to_file(pattern_memory)

    # 5) Write back to state
    state["dq_ai_findings"] = result
    state["dq_last_checked"] = datetime.now(timezone.utc).isoformat()
    state["dq_pattern_memory"] = pattern_memory

    app_log(
        "ai_detect_dq_anomalies_step_done",
        run_id=run_id,
        summary=result.get("summary", "No summary"),
    )

    print("✅ AI Pattern Analysis Summary:")
    print(result.get("summary", "No summary available."))

    return state


if __name__ == "__main__":
    # simple manual test run
    test_state = AgentState(
        question="Run AI naming DQ",
        chain_type="data_quality",
        metadata={"marketing": {}, "shopify": {}},
    )
    # In real runs, dq_dimensions will be set by collect_dq_dimensions_step
    # Here we just ensure it fails gracefully.
    out_state = ai_detect_dq_anomalies_step(test_state)
    print(json.dumps(out_state.get("dq_ai_findings", {}), indent=2))
