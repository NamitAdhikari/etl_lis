from pathlib import Path

import numpy as np
BASE_DIR = Path(__file__).resolve().parents[1]

import sys
sys.path.insert(0, f'{BASE_DIR}/lib')

from validate import db_connection_open, db_connection_close
from snowflake.connector.pandas_tools import write_pandas

import pandas as pd
import logging
logging.basicConfig(filename=f'{BASE_DIR}/log/f_sales_ld.log',
                    format="%(asctime)s | ETL_LIS | %(levelname)s: %(message)s", 
                    datefmt="%b %d, '%y %H:%M:%S", 
                    level=logging.INFO)

try:
    logging.info("Loading F_SALES Table")
    conn, cur = db_connection_open()

    cur.execute("USE NAMIT_BHATBHATENI.DWH_TMP_BBSM")
    logging.info("Cursor Execution - Used DWH_TMP_BBSM Schema of NAMIT_BHATBHATENI Database to fetch data")

    cur.execute('''
        UPDATE NAMIT_BHATBHATENI.DWH_TMP_BBSM.F_SALES_TMP sale
            SET sale.STORE_ID = str.STORE_KEY
            FROM NAMIT_BHATBHATENI.DWH_TGT_BBSM.D_STORE str
            WHERE sale.STORE_ID = str.STORE_ID;
    ''')
    logging.info("Cursor Execution - Changed Store_id business key to point surrogate key as foreign key")


    cur.execute('''
        UPDATE NAMIT_BHATBHATENI.DWH_TMP_BBSM.F_SALES_TMP sale
            SET sale.PRODUCT_ID = prod.PRODUCT_KEY
            FROM NAMIT_BHATBHATENI.DWH_TGT_BBSM.D_PRODUCT prod
            WHERE sale.PRODUCT_ID = prod.PRODUCT_ID; 
    ''')
    logging.info("Cursor Execution - Changed Product_id business key to point surrogate key as foreign key")

    cur.execute('''
        UPDATE NAMIT_BHATBHATENI.DWH_TMP_BBSM.F_SALES_TMP sale
            SET sale.CUSTOMER_ID = cust.CUSTOMER_KEY
            FROM NAMIT_BHATBHATENI.DWH_TGT_BBSM.D_CUSTOMER cust
            WHERE sale.CUSTOMER_ID = cust.CUSTOMER_ID;
    ''')
    logging.info("Cursor Execution - Changed Customer_id business key to point surrogate key as foreign key")

    # FETCHING SALES DATA FROM TMP
    cur.execute("SELECT * FROM F_SALES_TMP")
    logging.info("Cursor Execution - Fetching all data from F_SALES_TMP Table")
    sales_df = cur.fetch_pandas_all()
    logging.info("Converted Fetched Data to Pandas Dataframe")

    cur.execute("USE NAMIT_BHATBHATENI.DWH_TGT_BBSM")
    logging.info("Cursor Execution - Used DWH_TGT_BBSM Schema of NAMIT_BHATBHATENI Database to load data")

    sales_df.drop(columns='SALES_KEY', inplace=True)
    # LOADING SALES DATA TO TGT
    write_pandas(conn, sales_df, 'F_SALES')
    logging.info("Loaded all Sales Data to Target Table 'F_SALES'")
   
finally:
    db_connection_close(conn, cur)