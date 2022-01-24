from pathlib import Path
BASE_DIR = Path(__file__).resolve().parents[1]

import sys
sys.path.insert(0, f'{BASE_DIR}/lib')

from validate import db_connection_open, db_connection_close
from snowflake.connector.pandas_tools import write_pandas

import pandas as pd
import logging
logging.basicConfig(filename=f'{BASE_DIR}/log/d_country_ld.log',
                    format="%(asctime)s | ETL_LIS | %(levelname)s: %(message)s", 
                    datefmt="%b %d, '%y %H:%M:%S", 
                    level=logging.INFO)

try:
    logging.info("Loading D_COUNTRY Table")
    conn, cur = db_connection_open()

    cur.execute("USE NAMIT_BHATBHATENI.DWH_TMP_BBSM")
    logging.info("Cursor Execution - Used DWH_TMP_BBSM Schema of NAMIT_BHATBHATENI Database to fetch data")

    # FETCHING COUNTRY DATA FROM TMP
    cur.execute("SELECT * FROM D_COUNTRY_TMP")
    logging.info("Cursor Execution - Fetching all data from D_COUNTRY_TMP Table")
    country_df = cur.fetch_pandas_all()
    logging.info("Converted Fetched Data to Pandas Dataframe")

    cur.execute("USE NAMIT_BHATBHATENI.DWH_TGT_BBSM")
    logging.info("Cursor Execution - Used DWH_TGT_BBSM Schema of NAMIT_BHATBHATENI Database to load data")

    country_df.drop(columns='COUNTRY_KEY', inplace=True)
    # LOADING COUNTRY DATA TO TGT
    write_pandas(conn, country_df, 'D_COUNTRY')
    logging.info("Loaded all Country Data to Target Table 'D_COUNTRY'")
   
finally:
    db_connection_close(conn, cur)