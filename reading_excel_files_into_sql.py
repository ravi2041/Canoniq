import os
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from config import MYSQL_CONFIG_MARKETING


# Encode the password to avoid breaking the connection string
encoded_password = quote_plus(MYSQL_CONFIG_MARKETING['password'])

# ====== File directory path ======
excel_folder_path = 'D:/2degree/2degree/'

# ====== Set up SQLAlchemy engine ======
engine = create_engine(
    f"mysql+mysqlconnector://{MYSQL_CONFIG_MARKETING['user']}:{encoded_password}@{MYSQL_CONFIG_MARKETING['host']}:{MYSQL_CONFIG_MARKETING['port']}/{MYSQL_CONFIG_MARKETING['database']}"
)

# ====== Process each Excel file ======
for file in os.listdir(excel_folder_path):
    if file.endswith('.xlsx') or file.endswith('.xls'):
        file_path = os.path.join(excel_folder_path, file)
        table_name = os.path.splitext(file)[0].lower()

        print(f"➡️ Loading {file} into table `{table_name}`...")

        try:
            df = pd.read_excel(file_path)
            df.columns = [col.strip().replace(" ", "_").lower() for col in df.columns]
            df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
            print(f"✅ Successfully inserted into `{table_name}`")
        except Exception as e:
            print(f"❌ Failed to load `{file}`: {e}")

print("🎉 All files processed.")
