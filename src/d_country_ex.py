from pathlib import Path
BASE_DIR = Path(__file__).resolve().parents[1]

import sys
sys.path.insert(0, f'{BASE_DIR}/lib')

from validate import db_connection_open, db_connection_close


try:
    conn, cur = db_connection_open()

    cur.execute("USE BHATBHATENI.TRANSACTIONS")

    # EXTRACTING COUNTRY DATA
    cur.execute("SELECT * FROM COUNTRY")

    country_df = cur.fetch_pandas_all()
    country_df.to_csv(f'{BASE_DIR}/data/d_country_data.csv', index=False)

finally:
    db_connection_close(conn, cur)