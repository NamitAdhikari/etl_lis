import snowflake.connector
import logging


def db_connection_open():

    logging.info("Attempting Snowflake Database Connection with user raj**** and account rb239**")
    conn = snowflake.connector.connect (
        user='rajesht',
        password='LisNepal@2022',
        account='rb23930.southeast-asia.azure'
    )
    logging.info("Database Connection Successful")

    cur = conn.cursor()
    logging.info("Created Database Connection Cursor")

    return conn, cur


def db_connection_close(cur, conn):
    cur.close()
    logging.info("Closed Database Connection Cursor")
    conn.close()
    logging.info("Closed Database Connection")

