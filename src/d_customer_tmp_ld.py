from datetime import datetime
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parents[1]

import sys
sys.path.insert(0, f'{BASE_DIR}/lib')

from validate import db_connection_open, db_connection_close
from snowflake.connector.pandas_tools import write_pandas

import pandas as pd
import logging
logging.basicConfig(filename=f'{BASE_DIR}/log/d_customer_tmp_ld.log',
                    format="%(asctime)s | ETL_LIS | %(levelname)s: %(message)s", 
                    datefmt="%b %d, '%y %H:%M:%S", 
                    level=logging.INFO)

try:
    logging.info("Loading D_CUSTOMER_TMP Table")
    conn, cur = db_connection_open()

    cur.execute("USE NAMIT_BHATBHATENI.DWH_STG_BBSM")
    logging.info("Cursor Execution - Used DWH_STG_BBSM Schema of NAMIT_BHATBHATENI Database to fetch data")

    # FETCHING CUSTOMER DATA FROM STG
    cur.execute("SELECT * FROM STG_CUSTOMER")
    logging.info("Cursor Execution - Fetching all data from STG_CUSTOMER Table")
    customer_df = cur.fetch_pandas_all()
    logging.info("Converted Fetched Data to Pandas Dataframe")

    cur.execute("USE NAMIT_BHATBHATENI.DWH_TMP_BBSM")
    logging.info("Cursor Execution - Used DWH_TMP_BBSM Schema of NAMIT_BHATBHATENI Database to load data")

    # LOADING CUSTOMER DATA TO TMP
    query = '''INSERT INTO D_CUSTOMER_TMP 
    (CUSTOMER_ID, CUSTOMER_FIRST_NAME, CUSTOMER_LAST_NAME, CUSTOMER_MIDDLE_NAME, CUSTOMER_ADDRESS, INSERT_TIME, UPDATE_TIME, ACTIVE_FLAG, START_DATE, END_DATE)
    VALUES '''
    for row in customer_df.itertuples():
        query += f"({row.ID}, '{row.CUSTOMER_FIRST_NAME}', '{row.CUSTOMER_LAST_NAME}', '{row.CUSTOMER_MIDDLE_NAME}', '{row.CUSTOMER_ADDRESS}', "\
            f"'{datetime.now()}', '{datetime.now()}', 1, '{datetime.now()}', NULL),"

    query = query.rstrip(",") + ";"
    logging.info("Generated Batch Insert Query by iterating over DataFrame")

    cur.execute(query)
    logging.info("Batch Insertion of Customer Data to D_CUSTOMER_TMP Table")
   
finally:
    db_connection_close(conn, cur)