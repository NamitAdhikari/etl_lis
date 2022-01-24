from pathlib import Path

import numpy as np
BASE_DIR = Path(__file__).resolve().parents[1]

import sys
sys.path.insert(0, f'{BASE_DIR}/lib')

from validate import db_connection_open, db_connection_close
from snowflake.connector.pandas_tools import write_pandas

import pandas as pd
import logging
logging.basicConfig(filename=f'{BASE_DIR}/log/d_store_ld.log',
                    format="%(asctime)s | ETL_LIS | %(levelname)s: %(message)s", 
                    datefmt="%b %d, '%y %H:%M:%S", 
                    level=logging.INFO)

try:
    logging.info("Loading D_STORE Table")
    conn, cur = db_connection_open()

    cur.execute("USE NAMIT_BHATBHATENI.DWH_TMP_BBSM")
    logging.info("Cursor Execution - Used DWH_TMP_BBSM Schema of NAMIT_BHATBHATENI Database to fetch data")

    cur.execute('''
        UPDATE NAMIT_BHATBHATENI.DWH_TMP_BBSM.D_STORE_TMP str 
            SET str.REGION_ID = rgn.REGION_KEY
            FROM NAMIT_BHATBHATENI.DWH_TGT_BBSM.D_REGION rgn
            WHERE rgn.REGION_ID = str.REGION_ID;    
    ''')
    logging.info("Cursor Execution - Changed Region_id business key to point surrogate key as foreign key")

    # FETCHING STORE DATA FROM TMP
    cur.execute("SELECT * FROM D_STORE_TMP")
    logging.info("Cursor Execution - Fetching all data from D_STORE_TMP Table")
    store_df = cur.fetch_pandas_all()
    logging.info("Converted Fetched Data to Pandas Dataframe")

    cur.execute("USE NAMIT_BHATBHATENI.DWH_TGT_BBSM")
    logging.info("Cursor Execution - Used DWH_TGT_BBSM Schema of NAMIT_BHATBHATENI Database to load data")

    store_df.drop(columns='STORE_KEY', inplace=True)
    # LOADING STORE DATA TO TGT
    write_pandas(conn, store_df, 'D_STORE')
    logging.info("Loaded all Store Data to Target Table 'D_STORE'")
   
finally:
    db_connection_close(conn, cur)