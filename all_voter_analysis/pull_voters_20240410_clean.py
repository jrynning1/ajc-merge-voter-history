import pandas as pd
import mariadb
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

# only do this block once to get narrow_demographics.csv, then import that csv instead

sql_query = "SELECT * FROM `ga_sos_voters`.`voters_20240410_clean`"

# Connect to data warehouse with MariaDB
try:
    conn = mariadb.connect(
        user=os.getenv("MARIADB_USER"),
        password=os.getenv("MARIADB_PASSWORD"),
        host=os.getenv("MARIADB_HOST"),
        port=int(os.getenv("MARIADB_PORT"))
    )
except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)

# Get Cursor
cur = conn.cursor()

original_demographics_table = pd.read_sql(sql_query, conn, dtype='str')

output_path = Path().cwd().parent.joinpath('main_voter_dataset/voters_20240410_clean.csv')

original_demographics_table.to_csv(output_path)