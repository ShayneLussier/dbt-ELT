from datetime import datetime, timedelta
import snowflake.connector

# -------------------- VALUES -------------------- #

# Snowflake connection details
SNOWFLAKE_ACCOUNT = 'YXSKMAV-FX89628'
SNOWFLAKE_USER = 'SHAYNELUSSIER1'
SNOWFLAKE_PASSWORD = 'Australia19!'
SNOWFLAKE_DATABASE = 'store'
SNOWFLAKE_SCHEMA = 'analytics'
SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'
SNOWFLAKE_ROLE = 'ACCOUNTADMIN'

# -------------------- FUNCTIONS -------------------- #

def get_snowflake_connection():
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        warehouse=SNOWFLAKE_WAREHOUSE,
        role=SNOWFLAKE_ROLE
    )
    return conn

def get_max_customer_id():
    conn = get_snowflake_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT MAX(customer_id) AS max_customer_id FROM customers_dim")
        result = cur.fetchone()
        max_customer_id = [result[0] if result[0] is not None else 0]
    finally:
        cur.close()
        conn.close()
    return max_customer_id