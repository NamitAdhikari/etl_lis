from pathlib import Path
BASE_DIR = Path(__file__).resolve().parents[1]

import sys
sys.path.insert(0, f'{BASE_DIR}/lib')

from validate import db_connection_open, db_connection_close
from snowflake.connector.pandas_tools import write_pandas

import pandas as pd
import logging
logging.basicConfig(filename=f'{BASE_DIR}/log/d_region_stg_ld.log',
                    format="%(asctime)s | ETL_LIS | %(levelname)s: %(message)s", 
                    datefmt="%b %d, '%y %H:%M:%S", 
                    level=logging.INFO)

try:
    logging.info("Loading STG_REGION Table")
    conn, cur = db_connection_open()

    cur.execute("USE NAMIT_BHATBHATENI.DWH_STG_BBSM")
    logging.info("Cursor Execution - Used DWH_STG_BBSM Schema of NAMIT_BHATBHATENI Database")

    # LOADING REGION DATA TO STG
    region_data = pd.read_csv(f'{BASE_DIR}/data/d_region_data.csv')
    logging.info("Fetched region data from 'd_region_data.csv'")
    region_df = pd.DataFrame(region_data)
    logging.info("Converted region data into pandas Dataframe")

    write_pandas(conn, region_df, 'STG_REGION')
    logging.info("Cursor Execution - Loaded all Region Data to Staging Table 'STG_REGION'")

finally:
    db_connection_close(conn, cur)