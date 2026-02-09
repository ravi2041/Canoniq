# nodes/collect_dq_dimensions_step.py
import os
import json
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from datetime import datetime, timezone
from core.state import AgentState
from dotenv import load_dotenv

load_dotenv()

MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "localhost"),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", "poiuPOIU@098"),
    "database": os.getenv("MYSQL_DB", "marketing"),
}

# 🔐 Hardcoded mapping: logical platform -> physical table name
# Adjust these to match your actual DB tables.
PLATFORM_TABLE_MAP = {
    "facebook_ads": "facebook_data",
    "tiktok_data": "tiktok_data",
    "cm360": "cm360_data",
    "linkedin": "linkedin_data",          # if your table name is different, change here
    "trueview": "youtube_data",           # TrueView usually sits in YouTube export
    "the_trade_desk": "the_trade_desk_data",
    "snapchat": "snapchat_data",
}


def get_sqlalchemy_engine():
    user = quote_plus(MYSQL_CONFIG["user"])
    password = quote_plus(MYSQL_CONFIG["password"])
    host = MYSQL_CONFIG["host"]
    db = MYSQL_CONFIG["database"]
    uri = f"mysql+mysqlconnector://{user}:{password}@{host}/{db}"
    return create_engine(uri)


def load_metadata(metadata_path="D:/Analytics_AI/platform_naming_mapping.json"):
    abs_path = os.path.abspath(os.path.expanduser(metadata_path))
    print(f"🔍 Looking for metadata at: {abs_path}")
    print(f"📂 Current working directory: {os.getcwd()}")
    if not os.path.isfile(abs_path):
        raise FileNotFoundError(
            f"❌ Metadata file not found.\nChecked: {abs_path}\nWorking dir: {os.getcwd()}"
        )
    with open(abs_path, "r", encoding="utf-8") as f:
        return json.load(f)


def infer_table_for_column(engine, column_name, schema_name=None):
    """
    Try to infer the table name for a given column by scanning information_schema.
    Returns the table_name if exactly one match is found, otherwise None.

    NOTE: This is now a *fallback* if hardcoded PLATFORM_TABLE_MAP
    cannot resolve the table or the column doesn't exist in the mapped table.
    """
    schema_name = schema_name or MYSQL_CONFIG["database"]

    query = text(
        """
        SELECT table_name
        FROM information_schema.columns
        WHERE table_schema = :schema
          AND column_name  = :col
        """
    )

    try:
        with engine.connect() as conn:
            rows = conn.execute(
                query, {"schema": schema_name, "col": column_name}
            ).fetchall()
    except Exception as e:
        print(f"⚠️ Error inferring table for column '{column_name}': {e}")
        return None

    if len(rows) == 1:
        table_name = rows[0][0]
        print(
            f"✅ Fallback inference: column '{column_name}' uniquely found in table '{table_name}'"
        )
        return table_name

    if len(rows) == 0:
        print(
            f"⚠️ No table found with column '{column_name}' in schema '{schema_name}'"
        )
    else:
        table_list = ", ".join(r[0] for r in rows)
        print(
            f"⚠️ Multiple tables found with column '{column_name}' in schema '{schema_name}': "
            f"{table_list}. Cannot auto-infer uniquely."
        )
    return None


def column_exists_in_table(engine, table_name, column_name, schema_name=None) -> bool:
    """
    Check if a given column exists in a specific table.
    Used to validate the hardcoded platform -> table mapping.
    """
    schema_name = schema_name or MYSQL_CONFIG["database"]
    query = text(
        """
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_schema = :schema
          AND table_name   = :tbl
          AND column_name  = :col
        """
    )
    try:
        with engine.connect() as conn:
            count = conn.execute(
                query, {"schema": schema_name, "tbl": table_name, "col": column_name}
            ).scalar()
        return bool(count)
    except Exception as e:
        print(
            f"⚠️ Error checking column '{column_name}' in table '{table_name}': {e}"
        )
        return False


def normalize_platform_name(raw_platform: str, metadata: dict) -> str:
    """
    Map things like 'tiktok_ads' -> 'tiktok', 'trade_desk' -> 'the_trade_desk'
    using the `platforms` section + a small manual alias map.
    """
    if not raw_platform:
        return None

    raw = raw_platform.lower()

    # 1) Try exact match against platforms.name or aliases
    platforms_cfg = metadata.get("platforms", [])
    for p in platforms_cfg:
        name = (p.get("name") or "").lower()
        if raw == name:
            return p["name"]  # keep original casing

        aliases = [a.lower() for a in p.get("aliases", [])]
        if raw in aliases:
            return p["name"]

    # 2) Manual bridge aliases between old names and new canonical names
    extra_aliases = {
        "tiktok_ads": "tiktok",
        "trade_desk": "the_trade_desk",
        "tiktok": "tiktok",
        "trueview": "trueview",
        "facebook": "facebook_ads",
        "snapchat_ads": "snapchat",
    }

    if raw in extra_aliases:
        return extra_aliases[raw]

    # 3) Fallback – just return what we got
    return raw_platform


def extract_dimension_columns(metadata):
    """
    Parse metadata to get dimension columns.

    NOW:
    - Uses ONLY `canonical_entities.platform_mappings` to decide
      which columns to inspect per platform.
    - Does NOT read `datasets[].table_name` at all for DQ.
    - Table is left as None and will be resolved at runtime using
      PLATFORM_TABLE_MAP → column_exists_in_table → infer_table_for_column.
    """
    dimensions = {"campaign": [], "placement": [], "creative": [], "site": []}

    ENTITY_DIM_MAP = {
        "campaign": "campaign",
        "campaign_name": "campaign",
        "creative": "creative",
        "creative_name": "creative",
        "placement": "placement",
        "adgroup_name": "placement",
        "ad_group": "placement",
        "adset": "placement",
        "ad_set_name": "placement",
        "adset_name": "placement",
        "site": "site",
        "platform_name": "site",
        "publisher": "site",
    }

    # ------------------------------------
    # CASE 1: new format: canonical_entities
    # ------------------------------------
    if "canonical_entities" in metadata:
        for ent in metadata.get("canonical_entities", []):
            entity_name = (ent.get("entity_name") or "").lower()
            dim_key = ENTITY_DIM_MAP.get(entity_name)
            if not dim_key:
                print(f"⛔ Skipping unknown canonical_entity '{entity_name}'")
                continue

            canonical_name = ent.get("canonical_name")
            # canonical_name can be list or string in your JSON
            if isinstance(canonical_name, list) and canonical_name:
                canonical_key = canonical_name[0]
            else:
                canonical_key = canonical_name or None

            platform_mappings = ent.get("platform_mappings", {}) or {}

            print("🧩 Canonical entity:", entity_name, "– canonical:", canonical_name)

            for raw_platform, cols in platform_mappings.items():
                norm_platform = normalize_platform_name(raw_platform, metadata)

                # platform_mappings values in your JSON are lists of column names
                if not isinstance(cols, list):
                    cols = [cols]

                for col_name in cols:
                    if not col_name:
                        continue

                    dimensions[dim_key].append(
                        {
                            "table": None,           # resolved later
                            "column": col_name,
                            "platform": norm_platform,
                            "entity": entity_name,
                            "canonical_key": canonical_key,
                            "source_type": "database",
                        }
                    )

        return dimensions

    # ------------------------------------
    # CASE 2: old format: tables[]
    # ------------------------------------
    SEMANTIC_MAP = {
        "campaign_name": "campaign",
        "campaign": "campaign",
        "placement_name": "placement",
        "creative_name": "creative",
        "site_name": "site",
        "publisher": "site",
    }

    for table in metadata.get("tables", []):
        table_name = table.get("name")
        for col in table.get("columns", []):
            col_name = col.get("name", "")
            name_l = col_name.lower()
            semantic_type = (
                col.get("semantic_role") or col.get("semantic_type") or ""
            ).lower().strip()

            if name_l.endswith("_id") or name_l in (
                "campaign_id",
                "placement_id",
                "creative_id",
                "site_id",
                "advertiser_id",
            ):
                continue

            target_dim = None

            if semantic_type in SEMANTIC_MAP:
                target_dim = SEMANTIC_MAP[semantic_type]
            else:
                if "campaign" in name_l and not name_l.endswith("_id"):
                    target_dim = "campaign"
                elif "placement" in name_l and not name_l.endswith("_id"):
                    target_dim = "placement"
                elif "creative" in name_l and not name_l.endswith("_id"):
                    target_dim = "creative"
                elif name_l in ("site", "domain", "publisher") or "site_" in name_l:
                    target_dim = "site"

            if target_dim and target_dim in dimensions:
                dimensions[target_dim].append(
                    {
                        "table": table_name,
                        "column": col_name,
                        "platform": None,
                        "entity": None,
                        "canonical_key": None,
                        "source_type": "database",
                    }
                )

    return dimensions


def fetch_unique_values(
    engine,
    table,
    column,
    platform=None,
    entity=None,
    canonical_key=None,
    limit=5000,
):
    """Fetch unique values with their table/column + platform/entity context."""
    try:
        query = text(
            f"""
            SELECT DISTINCT `{column}` AS value
            FROM `{table}`
            WHERE `{column}` IS NOT NULL
              AND TRIM(`{column}`) != ''
              AND LOWER(`{column}`) NOT IN ('(not set)', 'na', 'null', 'none')
            LIMIT {limit}
        """
        )
        df = pd.read_sql(query, engine)
        return [
            {
                "value": str(v),
                "table": table,
                "column": column,
                "platform": platform,
                "entity": entity,
                "canonical_key": canonical_key,
                "source_type": "database",
            }
            for v in df["value"].dropna().unique()
        ]
    except Exception as e:
        print(f"⚠️ Error fetching {column} from {table}: {e}")
        return []


def collect_dq_dimensions_step(state: AgentState):
    engine = get_sqlalchemy_engine()
    metadata = load_metadata("D:/Analytics_AI/platform_naming_mapping.json")

    dimension_map = extract_dimension_columns(metadata)
    print("🧭 Dimension sources resolved:")
    print(json.dumps(dimension_map, indent=2))

    results = {k: [] for k in dimension_map.keys()}
    ingestion_logs = []

    for dim, sources in dimension_map.items():
        collected = []
        print(f"🔹 Dimension '{dim}' has {len(sources)} source(s).")

        for src in sources:
            platform = src.get("platform")
            entity = src.get("entity")
            canonical_key = src.get("canonical_key")
            column = src["column"]
            table = src.get("table")

            # 1️⃣ If table not specified in metadata, try hardcoded PLATFORM_TABLE_MAP
            if not table and platform:
                mapped_table = PLATFORM_TABLE_MAP.get(platform)
                if mapped_table:
                    # Validate that the column actually exists in that table
                    if column_exists_in_table(
                        engine,
                        table_name=mapped_table,
                        column_name=column,
                        schema_name=MYSQL_CONFIG["database"],
                    ):
                        print(
                            f"✅ Using mapped table '{mapped_table}' for platform='{platform}', column='{column}'"
                        )
                        table = mapped_table
                    else:
                        print(
                            f"⚠️ Column '{column}' not found in mapped table '{mapped_table}' "
                            f"for platform '{platform}'. Will try column-based inference."
                        )

            # 2️⃣ Fallback: use old infer_table_for_column based only on column
            if not table:
                table = infer_table_for_column(
                    engine,
                    column_name=column,
                    schema_name=MYSQL_CONFIG["database"],
                )
                if not table:
                    print(
                        f"⛔ Skipping {platform}.{column} for dim '{dim}': "
                        f"table could not be resolved (mapping + inference failed)."
                    )
                    continue

            values = fetch_unique_values(
                engine,
                table=table,
                column=column,
                platform=platform,
                entity=entity,
                canonical_key=canonical_key,
            )
            source_label = f"{table}.{column}"

            collected.extend(values)
            print(
                f"✅ {dim}: {len(values)} unique from database | {source_label}"
            )

        results[dim] = collected

        ingestion_logs.append(
            {
                "dimension": dim,
                "unique_count": len(collected),
                "source_columns": sources,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        )

    state["dq_dimensions"] = results
    state["dq_ingestion_anomalies"] = ingestion_logs

    # 🔹 NEW: carry the raw metadata forward for the AI step
    state["dq_metadata"] = metadata

    print("📊 Data Quality Dimensions Collected:")
    for k, v in results.items():
        print(f"  - {k}: {len(v)} unique values")

    return state


if __name__ == "__main__":
    state = AgentState(
        question="Run data quality collection",
        chain_type="data_quality",
        metadata={"marketing": {}, "shopify": {}},
    )
    output_state = collect_dq_dimensions_step(state)
    print(output_state)
