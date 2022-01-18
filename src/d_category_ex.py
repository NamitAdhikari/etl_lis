from pathlib import Path
BASE_DIR = Path(__file__).resolve().parents[1]

import sys
sys.path.insert(0, f'{BASE_DIR}/lib')

from validate import db_connection_open, db_connection_close

import logging
logging.basicConfig(filename=f'{BASE_DIR}/log/d_category_ex.log',
                    format="%(asctime)s | ETL_LIS | %(levelname)s: %(message)s", 
                    datefmt="%b %d, '%y %H:%M:%S", 
                    level=logging.INFO)

try:
    logging.info("Starting Category Data Extraction")
    conn, cur = db_connection_open()

    cur.execute("USE BHATBHATENI.TRANSACTIONS")
    logging.info("Cursor Execution - Used Transactions Schema of Bhatbhateni Database")

    # EXTRACTING CATEGORY DATA
    cur.execute("SELECT * FROM CATEGORY")
    logging.info("Cursor Execution - Fetched all Category Data")

    category_df = cur.fetch_pandas_all()
    logging.info("Converted fetched Category data to Pandas Dataframe")
    category_df.to_csv(f'{BASE_DIR}/data/d_category_data.csv', index=False)
    logging.info("Converted Dataframe to csv, and saved to 'd_category_data.csv' file")

finally:
    db_connection_close(conn, cur)