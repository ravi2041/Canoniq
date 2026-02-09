import mysql.connector
import json
from config import MYSQL_CONFIG_MARKETING
#


def infer_relationships(tables):
    relationships = []
    all_columns = {t["name"]: set(t["columns"]) for t in tables}

    for from_table in tables:
        for from_col in from_table["columns"]:
            if from_col.endswith("_id"):
                for to_table in tables:
                    if from_table["name"] != to_table["name"] and from_col in to_table["columns"]:
                        relationships.append({
                            "from_table": from_table["name"],
                            "from_column": from_col,
                            "to_table": to_table["name"],
                            "to_column": from_col
                        })
    return relationships


def generate_and_save_metadata(output_file: str = "metadata.json"):
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG_MARKETING)
        cursor = conn.cursor()

        # Get all tables in the database
        cursor.execute("SHOW TABLES")
        tables_raw = cursor.fetchall()
        table_names = [t[0] for t in tables_raw]

        tables = []
        for table_name in table_names:
            cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
            columns_raw = cursor.fetchall()
            columns = [col[0] for col in columns_raw]

            tables.append({
                "name": table_name,
                "columns": columns,
                "platform": infer_platform_from_name(table_name),
                "metrics_type": infer_metrics_type(columns),
                "notes": f"Table `{table_name}` is treated as a standalone platform dataset for aggregation before merging."
            })

        relationships = infer_relationships(tables)

        metadata = {
            "tables": tables,
            "relationships": relationships,
            "guidelines": [
                "When users ask for cross platform marketing questions, do not join source tables directly.",
                "Always group and aggregate data by platform first, then union results before comparisons.",
                "Always group and aggregate data first where table focuses on ad engagement metrics and tables that are focused on conversions.",
                "Avoid joining on non-matching columns like 'campaign name', 'ad name', 'placement name' between marketing platforms.",
                "Instead, provide different views for each platform and merge at the summary level only."
            ]
        }

        with open(output_file, "w") as f:
            json.dump(metadata, f, indent=2)

        print(f"✅ Metadata saved to {output_file}")
        cursor.close()
        conn.close()
        return metadata

    except mysql.connector.Error as e:
        print(f"❌ MySQL Error: {e}")
        return None


def infer_platform_from_name(table_name):
    """Rudimentary platform detection based on table name keywords."""
    name = table_name.lower()
    if "facebook" in name or "meta" in name:
        return "Facebook"
    elif "google" in name or "gads" in name or "dv360" in name:
        return "Google"
    elif "linkedin" in name:
        return "LinkedIn"
    elif "tt" in name or "tiktok" in name:
        return "TikTok"
    elif "cm360" in name:
        return "CM360"
    else:
        return "Unknown"


def infer_metrics_type(columns):
    """Tag if the table is mainly engagement or conversion focused."""
    columns = [c.lower() for c in columns]
    if any(k in columns for k in ["conversions", "total_conversions", "revenue"]):
        return "conversion"
    elif any(k in columns for k in ["clicks", "impressions", "video_25", "viewable_impression"]):
        return "engagement"
    else:
        return "mixed"

# Run
if __name__ == "__main__":
    generate_and_save_metadata()
