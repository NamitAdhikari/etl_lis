from pathlib import Path
BASE_DIR = Path(__file__).resolve().parents[1]

import sys
sys.path.insert(0, f'{BASE_DIR}/lib')

from validate import db_connection_open, db_connection_close


try:
    conn, cur = db_connection_open()

    cur.execute("USE BHATBHATENI.TRANSACTIONS")

    # EXTRACTING PRODUCT DATA
    cur.execute("SELECT * FROM PRODUCT")

    product_df = cur.fetch_pandas_all()
    product_df.to_csv(f'{BASE_DIR}/data/d_product_data.csv', index=False)

finally:
    db_connection_close(conn, cur)