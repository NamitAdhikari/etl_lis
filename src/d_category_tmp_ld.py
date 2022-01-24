from datetime import datetime
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parents[1]

import sys
sys.path.insert(0, f'{BASE_DIR}/lib')

from validate import db_connection_open, db_connection_close
from snowflake.connector.pandas_tools import write_pandas

import pandas as pd
import logging
logging.basicConfig(filename=f'{BASE_DIR}/log/d_category_tmp_ld.log',
                    format="%(asctime)s | ETL_LIS | %(levelname)s: %(message)s", 
                    datefmt="%b %d, '%y %H:%M:%S", 
                    level=logging.INFO)

try:
    logging.info("Loading D_CATEGORY_TMP Table")
    conn, cur = db_connection_open()

    cur.execute("USE NAMIT_BHATBHATENI.DWH_STG_BBSM")
    logging.info("Cursor Execution - Used DWH_STG_BBSM Schema of NAMIT_BHATBHATENI Database to fetch data")

    # FETCHING CATEGORY DATA FROM STG
    cur.execute("SELECT * FROM STG_CATEGORY")
    logging.info("Cursor Execution - Fetching all data from STG_CATEGORY Table")
    category_df = cur.fetch_pandas_all()
    logging.info("Converted Fetched Data to Pandas Dataframe")

    cur.execute("USE NAMIT_BHATBHATENI.DWH_TMP_BBSM")
    logging.info("Cursor Execution - Used DWH_TMP_BBSM Schema of NAMIT_BHATBHATENI Database to load data")

    # LOADING CATEGORY DATA TO TMP
    query = '''INSERT INTO D_CATEGORY_TMP 
    (CATEGORY_ID, CATEGORY_DESC, INSERT_TIME, UPDATE_TIME, ACTIVE_FLAG, START_DATE, END_DATE)
    VALUES '''
    for row in category_df.itertuples():
        query += f"({row.ID}, '{row.CATEGORY_DESC}', '{datetime.now()}', '{datetime.now()}', 1, '{datetime.now()}', NULL),"

    query = query.rstrip(",") + ";"
    logging.info("Generated Batch Insert Query by iterating over DataFrame")

    cur.execute(query)
    logging.info("Batch Insertion of Category Data to D_CATEGORY_TMP Table")

   
finally:
    db_connection_close(conn, cur)