import os
import subprocess

# --- Database connection info ---
DB_NAME = os.getenv("DB_NAME", "analytics_ai")
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")  # set in .env or Streamlit secrets
DB_HOST = os.getenv("DB_HOST", "localhost")

MYSQLDUMP_PATH = os.getenv("MYSQLDUMP_PATH", "mysqldump")  # path or command  # <- change this

command = [
    MYSQLDUMP_PATH,
    f"-h{DB_HOST}",
    f"-u{DB_USER}",
    f"-p{DB_PASSWORD}",
    DB_NAME
]

with open(f"{DB_NAME}_backup.sql", "w", encoding="utf-8") as f:
    subprocess.run(command, stdout=f, check=True)

print("done")