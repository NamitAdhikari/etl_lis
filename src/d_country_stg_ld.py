from pathlib import Path
BASE_DIR = Path(__file__).resolve().parents[1]

import sys
sys.path.insert(0, f'{BASE_DIR}/lib')

from validate import db_connection_open, db_connection_close
from snowflake.connector.pandas_tools import write_pandas

import pandas as pd
import logging
logging.basicConfig(filename=f'{BASE_DIR}/log/d_country_stg_ld.log',
                    format="%(asctime)s | ETL_LIS | %(levelname)s: %(message)s", 
                    datefmt="%b %d, '%y %H:%M:%S", 
                    level=logging.INFO)

try:
    logging.info("Loading STG_COUNTRY Table")
    conn, cur = db_connection_open()

    cur.execute("USE NAMIT_BHATBHATENI.DWH_STG_BBSM")
    logging.info("Cursor Execution - Used DWH_STG_BBSM Schema of NAMIT_BHATBHATENI Database")

    # LOADING COUNTRY DATA TO STG
    country_data = pd.read_csv(f'{BASE_DIR}/data/d_country_data.csv')
    logging.info("Fetched country data from 'd_country_data.csv'")
    country_df = pd.DataFrame(country_data)
    logging.info("Converted country data into pandas Dataframe")

    write_pandas(conn, country_df, 'STG_COUNTRY')
    logging.info("Cursor Execution - Loaded all Country Data to Staging Table 'STG_COUNTRY'")

finally:
    db_connection_close(conn, cur)