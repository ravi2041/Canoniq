# ai_metadata_builder.py
import os
import json
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from dotenv import load_dotenv
from urllib.parse import quote_plus
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage

load_dotenv()

# ---------- CONFIG ----------
MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "localhost"),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", "poiuPOIU@098"),
    "database": os.getenv("MYSQL_DB", "marketing"),
}

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Use a valid model
ai_model = ChatOpenAI(
    model="o4-mini-2025-04-16",
    temperature=1,
    model_kwargs={"response_format": {"type": "json_object"}},  # 👈 force JSON
)

# Path to your main metadata file (the big JSON you shared)
MAIN_METADATA_PATH = "D:/Analytics_AI/platform_naming_mapping.json"


# ---------------------- DB / UTILS ---------------------- #

def get_sqlalchemy_engine():
    user = quote_plus(MYSQL_CONFIG["user"])
    password = quote_plus(MYSQL_CONFIG["password"])
    host = MYSQL_CONFIG["host"]
    db = MYSQL_CONFIG["database"]
    uri = f"mysql+mysqlconnector://{user}:{password}@{host}/{db}"
    return create_engine(uri)


def introspect_schema(engine) -> dict:
    """
    Fetch all table names and their columns using pandas + SQLAlchemy.
    Returns a dict: {table_name: [column_info, ...], ...}
    """
    tables_df = pd.read_sql("SHOW TABLES", engine)
    schema = {}

    # first column in SHOW TABLES is the table name
    for tbl in tables_df.iloc[:, 0].tolist():
        cols_df = pd.read_sql(f"SHOW COLUMNS FROM `{tbl}`", engine)
        schema[tbl] = cols_df.to_dict(orient="records")

    return schema


def sample_rows(engine, table: str, n: int = 50):
    """
    Fetch small data sample for AI context.
    If the table is huge or fails, return [].
    """
    try:
        df = pd.read_sql(f"SELECT * FROM `{table}` LIMIT {n}", engine)
        return df.to_dict(orient="records")
    except Exception:
        return []


def make_json_safe(obj):
    """Recursively convert dates/decimals/etc. to strings so json.dumps works."""
    if isinstance(obj, (str, int, float)) or obj is None:
        return obj
    import datetime as _dt
    if isinstance(obj, (_dt.date, _dt.datetime)):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: make_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [make_json_safe(x) for x in obj]
    # fallback
    return str(obj)


# ---------------------- METADATA FILE HELPERS ---------------------- #

def load_main_metadata(path: str = MAIN_METADATA_PATH) -> dict:
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Metadata file not found at {path}")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_main_metadata(metadata: dict, path: str = MAIN_METADATA_PATH):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)
    print(f"✅ Updated metadata saved to {path}")


def get_known_tables_from_metadata(metadata: dict) -> set:
    """
    Look into metadata['datasets'][*]['tables'][*]['table_name'] and
    return a set of all known table names (physical DB table names).
    """
    known = set()
    for ds in metadata.get("datasets", []):
        for tbl in ds.get("tables", []):
            tname = tbl.get("table_name")
            if tname:
                known.add(tname)
    return known


def guess_source_platform_from_table(table_name: str, metadata: dict) -> str | None:
    """
    Very simple heuristics to guess which platform this table belongs to,
    based on your 'platforms' list and dataset naming patterns.
    You can improve this over time.
    """
    t = table_name.lower()

    # First: look for existing datasets using this table_name and reuse their source_platform
    for ds in metadata.get("datasets", []):
        for tbl in ds.get("tables", []):
            if tbl.get("table_name", "").lower() == t:
                return ds.get("source_platform")

    # Second: infer by name patterns (customise as needed)
    if "fb_" in t or "facebook" in t:
        return "facebook_ads"
    if "tiktok" in t:
        return "tiktok"
    if "snap" in t:
        return "snapchat"
    if "linkedin" in t or "li_" in t:
        return "linkedin"
    if "cm360" in t or "cm_" in t:
        return "cm360"
    if "trade_desk" in t or "tradedesk" in t or "ttd" in t:
        return "the_trade_desk"
    if "youtube" in t or "trueview" in t:
        return "trueview"
    if "google_search" in t or "gads" in t or "google_ads" in t:
        return "google_search_ads"
    if "shopify" in t:
        return "shopify"

    # Lastly: none – you can still have AI decide
    return None


# ---------------------- LLM: BUILD DATASET FOR ONE TABLE ---------------------- #

import json
from json import JSONDecodeError
from langchain_core.messages import HumanMessage

def ai_build_dataset_for_table(
    table_name: str,
    schema_for_table: list[dict],
    sample_rows_for_table: list[dict],
    db_name: str,
    main_metadata: dict,
) -> dict:
    """
    Ask the LLM to build a *dataset* block (like facebook_ads_raw, cm360_raw, etc)
    for a single table, consistent with your existing metadata structure.
    """

    safe_schema = make_json_safe(schema_for_table)
    safe_samples = make_json_safe(sample_rows_for_table)

    guessed_platform = guess_source_platform_from_table(table_name, main_metadata)
    existing_platforms = main_metadata.get("platforms", [])
    canonical_entities = main_metadata.get("canonical_entities", [])
    canonical_metrics = main_metadata.get("canonical_metrics", [])

    prompt = f"""
            You are an expert data modeler for marketing analytics.
            
            You are working inside an existing unified metadata file for a product called "Clarity AI".
            This metadata file already contains:
            
            1) A list of platforms (facebook_ads, cm360, tiktok, snapchat, etc.)
            2) Canonical entities (campaign, creative, placement, site) and their platform column mappings.
            3) Canonical metrics (impressions, clicks, spend, etc.) and their platform column mappings.
            4) A 'datasets' section where each dataset describes one or more physical tables.
            
            Your task now is:
            - Given ONE new database table schema and a few sample rows,
            - Generate ONE dataset block that should be appended to the existing 'datasets' array.
            
            Return ONLY a single valid JSON object. Do not include any explanation text.
            The JSON MUST have this shape (no extra top-level keys):
            
            {{
              "dataset_name": "<short_name_describing_source>",
              "source_platform": "<one of the known platform names>",
              "description": "<1-2 sentence description>",
              "templates": {{
                "column_defaults": {{
                  "standardization_rules": ["trim"]
                }},
                "metric_defaults": {{
                  "semantic_role": "metric",
                  "dq_rules": {{ "min_value": 0 }}
                }},
                "id_defaults": {{
                  "semantic_role": "id",
                  "dq_rules": {{ "not_null": true }}
                }},
                "time_defaults": {{
                  "semantic_role": "time",
                  "dq_rules": {{
                    "not_null": true,
                    "max_lag_days": 2
                  }}
                }}
              }},
              "alias_rules": {{
                "canonical_metrics": {{ }},
                "canonical_entities": {{ }}
              }},
              "tables": [
                {{
                  "table_name": "{table_name}",
                  "table_type": "fact" or "dimension",
                  "grain": "<ad_day | ad_hour | placement_day | campaign_day | etc>",
                  "primary_key": [ ],
                  "use_templates": true,
                  "columns_compact": [
                    {{
                      "column_name": "<col>",
                      "semantic_role": "<id | name | time | metric | category | flag>"
                    }}
                  ],
                  "derived_metrics": [ ]
                }}
              ]
            }}
            
            Use ONLY real columns from the provided schema.
            
            Known platform names (preferred values for source_platform):
            {json.dumps([p["name"] for p in existing_platforms], indent=2)}
            
            Database name: {db_name}
            Table name: {table_name}
            
            Table schema (from SHOW COLUMNS):
            {json.dumps(safe_schema, indent=2)}
            
            Sample rows (up to 50):
            {json.dumps(safe_samples, indent=2)}
            
            Existing canonical_entities (for reference):
            {json.dumps(canonical_entities, indent=2)}
            
            Existing canonical_metrics (for reference):
            {json.dumps(canonical_metrics, indent=2)}
            
            Guessed source platform for this table (you may override if data suggests otherwise):
            {guessed_platform}
            """

    msg = [HumanMessage(content=prompt)]
    response = ai_model.invoke(msg)

    # LangChain ChatOpenAI returns an AIMessage; content should be a JSON string
    raw = response.content
    if raw is None:
        raise ValueError(f"LLM returned None for table {table_name}")

    raw_str = raw.strip()
    if not raw_str:
        raise ValueError(f"LLM returned empty content for table {table_name}")

    try:
        dataset_obj = json.loads(raw_str)
    except JSONDecodeError:
        print("⚠️ Failed to parse AI response for table", table_name)
        print("Raw content from model:")
        print(raw_str)
        # Raise again so you see it clearly in the traceback
        raise

    if not isinstance(dataset_obj, dict):
        raise ValueError(
            f"Expected a JSON object (dict) for dataset, got {type(dataset_obj)}"
        )

    # Small sanity checks
    if "dataset_name" not in dataset_obj:
        raise ValueError(
            f"AI dataset for table {table_name} is missing 'dataset_name'"
        )
    if "tables" not in dataset_obj:
        raise ValueError(
            f"AI dataset for table {table_name} is missing 'tables' array"
        )

    return dataset_obj

# ---------------------- MERGE / UPSERT DATASET ---------------------- #

def upsert_dataset_in_metadata(metadata: dict, new_dataset: dict) -> dict:
    """
    If a dataset with the same dataset_name exists, replace it.
    Otherwise, append it.
    """
    datasets = metadata.get("datasets", [])
    ds_name = new_dataset.get("dataset_name")
    if not ds_name:
        raise ValueError("New dataset is missing 'dataset_name'")

    for i, ds in enumerate(datasets):
        if ds.get("dataset_name") == ds_name:
            print(f"🔁 Replacing existing dataset '{ds_name}' in metadata.")
            datasets[i] = new_dataset
            break
    else:
        print(f"➕ Appending new dataset '{ds_name}' to metadata.")
        datasets.append(new_dataset)

    metadata["datasets"] = datasets
    return metadata


# ---------------------- MAIN PIPELINE ---------------------- #

def generate_and_append_metadata(
    metadata_path: str = MAIN_METADATA_PATH,
    target_tables: list[str] | None = None,
):
    """
    1. Load existing main metadata (platform_naming_mapping.json)
    2. Connect to DB and introspect schema
    3. Decide which tables need new datasets (skip already-known table_name)
    4. For each new table, build dataset via AI and upsert into metadata
    5. Save updated metadata back to the same file
    """
    # 1) Load existing metadata
    main_md = load_main_metadata(metadata_path)
    db_name = MYSQL_CONFIG["database"]
    print(f"📂 Loaded existing metadata from {metadata_path}")

    # 2) Connect + introspect
    engine = get_sqlalchemy_engine()
    full_schema = introspect_schema(engine)
    print(f"🗂  Found {len(full_schema)} tables in DB '{db_name}'")

    # 3) Figure out which tables are "new"
    known_tables = get_known_tables_from_metadata(main_md)
    print(f"📚 Metadata already knows about {len(known_tables)} table(s): {sorted(known_tables)}")

    # If user provided a subset of tables, use that – otherwise all.
    all_tables = list(full_schema.keys())
    if target_tables:
        target_tables = [t for t in target_tables if t in all_tables]
    else:
        target_tables = all_tables

    tables_to_process = [t for t in target_tables if t not in known_tables]
    print(f"🎯 Tables to process for new datasets: {tables_to_process}")

    if not tables_to_process:
        print("✅ No new tables to describe. Nothing to do.")
        return main_md

    # 4) For each new table -> AI dataset -> upsert
    for tbl in tables_to_process:
        print(f"\n🔧 Building dataset metadata for table: {tbl}")
        schema_for_table = full_schema[tbl]
        sample_for_table = sample_rows(engine, tbl, n=50)

        new_dataset = ai_build_dataset_for_table(
            table_name=tbl,
            schema_for_table=schema_for_table,
            sample_rows_for_table=sample_for_table,
            db_name=db_name,
            main_metadata=main_md,
        )

        main_md = upsert_dataset_in_metadata(main_md, new_dataset)

    # 5) Optionally add/update a top-level timestamp
    main_md["generated_at"] = datetime.utcnow().isoformat()

    # 6) Save back
    save_main_metadata(main_md, metadata_path)

    return main_md


if __name__ == "__main__":
    # If you want to target specific tables, pass a list like:
    # generate_and_append_metadata(target_tables=["cm360_data", "linkedin_data"])
    generate_and_append_metadata()
