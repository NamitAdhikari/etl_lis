from datetime import datetime
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parents[1]

import sys
sys.path.insert(0, f'{BASE_DIR}/lib')

from validate import db_connection_open, db_connection_close
from snowflake.connector.pandas_tools import write_pandas

import pandas as pd
import logging
logging.basicConfig(filename=f'{BASE_DIR}/log/f_sales_tmp_ld.log',
                    format="%(asctime)s | ETL_LIS | %(levelname)s: %(message)s", 
                    datefmt="%b %d, '%y %H:%M:%S", 
                    level=logging.INFO)

try:
    logging.info("Loading F_SALES_TMP Table")
    conn, cur = db_connection_open()

    cur.execute("USE NAMIT_BHATBHATENI.DWH_STG_BBSM")
    logging.info("Cursor Execution - Used DWH_STG_BBSM Schema of NAMIT_BHATBHATENI Database to fetch data")

    # FETCHING SALES DATA FROM STG
    cur.execute("SELECT * FROM STG_SALES")
    logging.info("Cursor Execution - Fetching all data from STG_SALES Table")
    sales_df = cur.fetch_pandas_all()
    logging.info("Converted Fetched Data to Pandas Dataframe")

    cur.execute("USE NAMIT_BHATBHATENI.DWH_TMP_BBSM")
    logging.info("Cursor Execution - Used DWH_TMP_BBSM Schema of NAMIT_BHATBHATENI Database to load data")

    # LOADING SALES DATA TO TMP
    query = '''INSERT INTO F_SALES_TMP 
    (SALES_ID, STORE_ID, PRODUCT_ID, CUSTOMER_ID, TRANSACTION_TIME, QUANTITY, AMOUNT, DISCOUNT, INSERT_TIME, UPDATE_TIME, START_DATE, END_DATE)
    VALUES '''

    sales_df.fillna(value='NULL', inplace=True)
    for row in sales_df.itertuples():
        query += f"({row.ID}, {row.STORE_ID}, {row.PRODUCT_ID}, {row.CUSTOMER_ID}, '{row.TRANSACTION_TIME}', {row.QUANTITY}, "\
            f"{row.AMOUNT}, {row.DISCOUNT}, '{datetime.now()}', '{datetime.now()}', '{datetime.now()}', NULL),"

    query = query.rstrip(",") + ";"
    logging.info("Generated Batch Insert Query by iterating over DataFrame")

    cur.execute(query)
    logging.info("Batch Insertion of Sales Data to F_SALES_TMP Table")
   
finally:
    db_connection_close(conn, cur)