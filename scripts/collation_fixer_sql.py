import os
import mysql.connector

# Replace with your actual DB credentials
conn = mysql.connector.connect(
    host=os.getenv("DB_HOST", "localhost"),
    user=os.getenv("DB_USER", "root"),
    password=os.getenv("DB_PASSWORD", ""),
    database=os.getenv("DB_NAME", "shopify")
)
cursor = conn.cursor()

# Target charset and collation
target_charset = "utf8mb4"
target_collation = "utf8mb4_general_ci"

# 1. Get all columns with char/text data types
cursor.execute(f"""
    SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = '{conn.database}'
      AND DATA_TYPE IN ('char', 'varchar', 'text', 'tinytext', 'mediumtext', 'longtext')
""")

rows = cursor.fetchall()

# 2. Group columns by table
from collections import defaultdict

table_columns = defaultdict(list)

for table, column, column_type in rows:
    table_columns[table].append((column, column_type))

# 3. Generate ALTER TABLE statements
for table, columns in table_columns.items():
    alters = []
    for col, col_type in columns:
        alters.append(f"MODIFY `{col}` {col_type} CHARACTER SET {target_charset} COLLATE {target_collation}")

    alter_sql = f"ALTER TABLE `{table}` " + ", ".join(alters) + ";"
    print(f"⚙️ Running:\n{alter_sql}")

    try:
        cursor.execute(alter_sql)
        conn.commit()
        print(f"✅ {table} updated.")
    except Exception as e:
        print(f"❌ Failed on {table}: {e}")

cursor.close()
conn.close()
