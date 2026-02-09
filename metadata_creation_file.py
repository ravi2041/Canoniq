import mysql.connector
import json
from config import MYSQL_CONFIG


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


def generate_and_save_metadata(output_file: str = "shopify_metadata.json"):
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
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
                "columns": columns
            })

        relationships = infer_relationships(tables)
        metadata = {
            "tables": tables,
            "relationships": relationships
        }

        # Save metadata to file
        with open(output_file, "w") as f:
            json.dump(metadata, f, indent=2)

        print(f"✅ Metadata saved to {output_file}")

        cursor.close()
        conn.close()

        return metadata

    except mysql.connector.Error as e:
        print(f"❌ MySQL Error: {e}")
        return None


if __name__ == "__main__":
    generate_and_save_metadata()
