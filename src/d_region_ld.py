from pathlib import Path

import numpy as np
BASE_DIR = Path(__file__).resolve().parents[1]

import sys
sys.path.insert(0, f'{BASE_DIR}/lib')

from validate import db_connection_open, db_connection_close
from snowflake.connector.pandas_tools import write_pandas

import pandas as pd
import logging
logging.basicConfig(filename=f'{BASE_DIR}/log/d_region_ld.log',
                    format="%(asctime)s | ETL_LIS | %(levelname)s: %(message)s", 
                    datefmt="%b %d, '%y %H:%M:%S", 
                    level=logging.INFO)

try:
    logging.info("Loading D_REGION Table")
    conn, cur = db_connection_open()

    cur.execute("USE NAMIT_BHATBHATENI.DWH_TMP_BBSM")
    logging.info("Cursor Execution - Used DWH_TMP_BBSM Schema of NAMIT_BHATBHATENI Database to fetch data")

    cur.execute('''
        UPDATE NAMIT_BHATBHATENI.DWH_TMP_BBSM.D_REGION_TMP reg 
            SET reg.COUNTRY_ID = cnt.COUNTRY_KEY
            FROM NAMIT_BHATBHATENI.DWH_TGT_BBSM.D_COUNTRY cnt
            WHERE reg.COUNTRY_ID = cnt.COUNTRY_ID;
    ''')
    logging.info("Cursor Execution - Changed Country_id business key to point surrogate key as foreign key")

    # FETCHING REGION DATA FROM TMP
    cur.execute("SELECT * FROM D_REGION_TMP")
    logging.info("Cursor Execution - Fetching all data from D_REGION_TMP Table")
    region_df = cur.fetch_pandas_all()
    logging.info("Converted Fetched Data to Pandas Dataframe")

    cur.execute("USE NAMIT_BHATBHATENI.DWH_TGT_BBSM")
    logging.info("Cursor Execution - Used DWH_TGT_BBSM Schema of NAMIT_BHATBHATENI Database to load data")

    region_df.drop(columns='REGION_KEY', inplace=True)
    # LOADING REGION DATA TO TGT
    write_pandas(conn, region_df, 'D_REGION')
    logging.info("Loaded all Region Data to Target Table 'D_REGION'")
   
finally:
    db_connection_close(conn, cur)