from pathlib import Path
BASE_DIR = Path(__file__).resolve().parents[1]

import sys
sys.path.insert(0, f'{BASE_DIR}/lib')

from validate import db_connection_open, db_connection_close

import logging
logging.basicConfig(filename=f'{BASE_DIR}/log/f_sales_agg_month.log',
                    format="%(asctime)s | ETL_LIS | %(levelname)s: %(message)s", 
                    datefmt="%b %d, '%y %H:%M:%S", 
                    level=logging.INFO)

try:
    logging.info("Aggregation of Sales Data By Month")
    conn, cur = db_connection_open()

    cur.execute("USE NAMIT_BHATBHATENI.DWH_TMP_BBSM")
    logging.info("Cursor Execution - Used DWH_TMP_BBSM Schema of NAMIT_BHATBHATENI Database for aggregation")

    # EXTRACTING SALES DATA
    cur.execute('''
        SELECT MONTH(TRANSACTION_TIME) AS AGG_MONTH, SUM(QUANTITY) AS TOTAL_QUANTITY, SUM(AMOUNT) AS TOTAL_AMOUNT, SUM(DISCOUNT) AS TOTAL_DISCOUNT
        FROM F_SALES_TMP
        GROUP BY AGG_MONTH
        ORDER BY AGG_MONTH;
    ''')
    logging.info("Cursor Execution - Fetched Sales Month Aggregation Data")

    sales_agg_df = cur.fetch_pandas_all()
    logging.info("Converted fetched Sales Month Aggregation data to Pandas Dataframe")
    sales_agg_df.to_csv(f'{BASE_DIR}/data/f_sales_agg_month_data.csv', index=False)
    logging.info("Converted Dataframe to csv, and saved to 'f_sales_agg_month_data.csv' file")

finally:
    db_connection_close(conn, cur)