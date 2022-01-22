from pathlib import Path
BASE_DIR = Path(__file__).resolve().parents[1]

import sys
sys.path.insert(0, f'{BASE_DIR}/lib')

from validate import db_connection_open, db_connection_close
from snowflake.connector.pandas_tools import write_pandas

import pandas as pd
import logging
logging.basicConfig(filename=f'{BASE_DIR}/log/f_sales_stg_ld.log',
                    format="%(asctime)s | ETL_LIS | %(levelname)s: %(message)s", 
                    datefmt="%b %d, '%y %H:%M:%S", 
                    level=logging.INFO)

try:
    logging.info("Loading STG_SALES Table")
    conn, cur = db_connection_open()

    cur.execute("USE NAMIT_BHATBHATENI.DWH_STG_BBSM")
    logging.info("Cursor Execution - Used DWH_STG_BBSM Schema of NAMIT_BHATBHATENI Database")

    # LOADING SALES DATA TO STG
    sales_data = pd.read_csv(f'{BASE_DIR}/data/f_sales_data.csv')
    logging.info("Fetched sales data from 'f_sales_data.csv'")
    sales_df = pd.DataFrame(sales_data)
    logging.info("Converted sales data into pandas Dataframe")

    write_pandas(conn, sales_df, 'STG_SALES')
    logging.info("Cursor Execution - Loaded all Sales Data to Staging Table 'STG_SALES'")

finally:
    db_connection_close(conn, cur)