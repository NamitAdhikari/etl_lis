import snowflake.connector


def db_connection_open():

    conn = snowflake.connector.connect (
        user='rajesht',
        password='LisNepal@2022',
        account='rb23930.southeast-asia.azure'
    )

    cur = conn.cursor()

    return conn, cur

# try:
#     cur.execute("USE BHATBHATENI.TRANSACTIONS")

#     # EXTRACTING SALES DATA
#     cur.execute("SELECT * FROM SALES")
  
#     sales_df = cur.fetch_pandas_all()
#     sales_df.to_csv('sales_data.csv', index=False)

#     print(sales_df)

#     cur.execute('''
#         SELECT STORE.ID AS STORE_ID, COUNTRY_DESC, REGION_DESC, STORE_DESC FROM STORE
#         JOIN REGION ON STORE.REGION_ID = REGION.ID
#         JOIN COUNTRY ON REGION.COUNTRY_ID = COUNTRY.ID;
#     ''')

#     location_df = cur.fetch_pandas_all()
#     location_df.to_csv('location_data.csv', index=False)

#     print(location_df)

# finally:

def db_connection_close(cur, conn):
    cur.close()
    conn.close()

