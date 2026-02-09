# ✅ sql_runner.py

import mysql.connector
from config import BASE_MYSQL_CONFIG

def run_sql_on_mysql(sql: str, database:str):
    try:
        config = BASE_MYSQL_CONFIG.copy()
        config["database"]= database

        conn = mysql.connector.connect(**config)
        print("✅ Connected to MySQL database")  # ✅ Confirm connection
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        print("columns:", columns)
        cursor.close()
        conn.close()
        return {
            "columns": columns,
            "rows": rows
        }
    except Exception as e:
        return {"error": str(e)}

# if __name__ == "__main__":
#     test_sql = "SELECT * from youtube_data limit 10 ;"  # safe test query
#     result = run_sql_on_mysql(test_sql)
#     print(result)