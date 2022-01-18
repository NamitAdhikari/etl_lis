from pathlib import Path
BASE_DIR = Path(__file__).resolve().parents[1]

import sys
sys.path.insert(0, f'{BASE_DIR}/lib')

from validate import db_connection_open, db_connection_close

import logging
logging.basicConfig(filename=f'{BASE_DIR}/log/d_store_ex.log',
                    format="%(asctime)s | ETL_LIS | %(levelname)s: %(message)s", 
                    datefmt="%b %d, '%y %H:%M:%S", 
                    level=logging.INFO)


try:
    logging.info("Starting Store Data Extraction")
    conn, cur = db_connection_open()

    cur.execute("USE BHATBHATENI.TRANSACTIONS")
    logging.info("Cursor Execution - Used Transactions Schema of Bhatbhateni Database")

    # EXTRACTING STORE DATA
    cur.execute("SELECT * FROM STORE")
    logging.info("Cursor Execution - Fetched all Store Data")

    store_df = cur.fetch_pandas_all()
    logging.info("Converted fetched Store data to Pandas Dataframe")
    store_df.to_csv(f'{BASE_DIR}/data/d_store_data.csv', index=False)
    logging.info("Converted Dataframe to csv, and saved to 'd_store_data.csv' file")

finally:
    db_connection_close(conn, cur)