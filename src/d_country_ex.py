from pathlib import Path
BASE_DIR = Path(__file__).resolve().parents[1]

import sys
sys.path.insert(0, f'{BASE_DIR}/lib')

from validate import db_connection_open, db_connection_close

import logging
logging.basicConfig(filename=f'{BASE_DIR}/log/d_country_ex.log',
                    format="%(asctime)s | ETL_LIS | %(levelname)s: %(message)s", 
                    datefmt="%b %d, '%y %H:%M:%S", 
                    level=logging.INFO)

try:
    logging.info("Starting Country Data Extraction")
    conn, cur = db_connection_open()

    cur.execute("USE BHATBHATENI.TRANSACTIONS")
    logging.info("Cursor Execution - Used Transactions Schema of Bhatbhateni Database")

    # EXTRACTING COUNTRY DATA
    cur.execute("SELECT * FROM COUNTRY")
    logging.info("Cursor Execution - Fetched all Country Data")

    country_df = cur.fetch_pandas_all()
    logging.info("Converted fetched Country data to Pandas Dataframe")
    country_df.to_csv(f'{BASE_DIR}/data/d_country_data.csv', index=False)
    logging.info("Converted Dataframe to csv, and saved to 'd_country_data.csv' file")

finally:
    db_connection_close(conn, cur)