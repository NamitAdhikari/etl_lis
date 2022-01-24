from datetime import datetime
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parents[1]

import sys
sys.path.insert(0, f'{BASE_DIR}/lib')

from validate import db_connection_open, db_connection_close
from snowflake.connector.pandas_tools import write_pandas

import pandas as pd
import logging
logging.basicConfig(filename=f'{BASE_DIR}/log/d_store_tmp_ld.log',
                    format="%(asctime)s | ETL_LIS | %(levelname)s: %(message)s", 
                    datefmt="%b %d, '%y %H:%M:%S", 
                    level=logging.INFO)

try:
    logging.info("Loading D_STORE_TMP Table")
    conn, cur = db_connection_open()

    cur.execute("USE NAMIT_BHATBHATENI.DWH_STG_BBSM")
    logging.info("Cursor Execution - Used DWH_STG_BBSM Schema of NAMIT_BHATBHATENI Database to fetch data")

    # FETCHING STORE DATA FROM STG
    cur.execute("SELECT * FROM STG_STORE")
    logging.info("Cursor Execution - Fetching all data from STG_STORE Table")
    store_df = cur.fetch_pandas_all()
    logging.info("Converted Fetched Data to Pandas Dataframe")

    cur.execute("USE NAMIT_BHATBHATENI.DWH_TMP_BBSM")
    logging.info("Cursor Execution - Used DWH_TMP_BBSM Schema of NAMIT_BHATBHATENI Database to load data")

    # LOADING STORE DATA TO TMP
    query = '''INSERT INTO D_STORE_TMP 
    (STORE_ID, REGION_ID, STORE_DESC, CURRENT_FLAG, EFFECTIVE_FROM, EFFECTIVE_TO, INSERT_TIME, UPDATE_TIME)
    VALUES '''
    for row in store_df.itertuples():
        query += f"({row.ID}, '{row.REGION_ID}', '{row.STORE_DESC}', 1,"\
            f"'{datetime.now()}', NULL, '{datetime.now()}', '{datetime.now()}'),"

    query = query.rstrip(",") + ";"
    logging.info("Generated Batch Insert Query by iterating over DataFrame")

    cur.execute(query)
    logging.info("Batch Insertion of Store Data to D_STORE_TMP Table")
   
finally:
    db_connection_close(conn, cur)