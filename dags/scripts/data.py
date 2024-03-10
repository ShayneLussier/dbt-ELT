from faker import Faker
import random
from datetime import datetime, timedelta
import csv
import json
import os
import uuid
import snowflake.connector

# -------------------- VALUES -------------------- #

START_DATE = datetime.today() - timedelta(days=365)
END_DATE = datetime.today()

CSV = "products.csv"
JSON = "transactions.json"

MAX_CUSTOMER_ID = 0 # I should be able to delete this variable

# Snowflake connection details
SNOWFLAKE_ACCOUNT = 'https://mj85741.ca-central-1.aws.snowflakecomputing.com'
SNOWFLAKE_USER = 'SHAYNELUSSIER1'
SNOWFLAKE_PASSWORD = 'Australia19!'
SNOWFLAKE_DATABASE = 'Store'
SNOWFLAKE_SCHEMA = 'Analytics'
SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'
SNOWFLAKE_ROLE = 'ACCOUNTADMIN '

# -------------------- FUNCTIONS -------------------- #

fake = Faker(locale='fr_CA')

def generate_quebec_address():
    while True:
        address = fake.address()
        # Check if the address contains "Québec" or "QC" to filter out non-Quebec addresses
        if "Québec" in address or "QC" in address:
            address = address.replace('\n', ', ')
            if "Suite" not in address and "Apt." not in address:
                return address

def generate_date(start_date=START_DATE, end_date=END_DATE):
    time_delta = end_date - start_date
    random_days = random.randint(0, time_delta.days)
    random_date = (start_date + timedelta(days=random_days)).strftime("%d%m%Y")
    return random_date

def get_snowflake_connection():
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        warehouse=SNOWFLAKE_WAREHOUSE,
        role=SNOWFLAKE_ROLE,
        )
    return conn


def get_max_customer_id():
    conn = get_snowflake_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT MAX(customer_id) AS max_customer_id FROM analytics.customers_dim")
        result = cur.fetchone()
        max_customer_id = result[0] if result[0] is not None else 0
        return max_customer_id
    finally:
        cur.close()
        conn.close()

def generate_id(starting_id):
    starting_id += 1
    return starting_id

def read_json(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        json_data = [json.loads(line) for line in file if line.strip()]
    return json_data

def append_to_json(file_path, data):
    with open(file=file_path, mode='a', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)
        f.write('\n')

def csv_file_check():
    file_path = CSV
    if not os.path.isfile(file_path):
        print(f"Error: File '{file_path}' not found in the current working directory. Please fetch this file to generate fake transactions.\n")
        raise FileNotFoundError(f"Error: File '{file_path}' not found in the current working directory. Please fetch this file to generate fake transactions.")

def transaction_new_customer(csv_content):
    global MAX_CUSTOMER_ID
    MAX_CUSTOMER_ID = get_max_customer_id()
    customer_name = fake.name()
    num_items = random.randint(1, 5)
    data = {
        "id": uuid.uuid4().hex,
        "date": generate_date(),
        # returns the product_id and price of a random product in 'products.csv' for a random number of items
        "items": [
            {"product_id": chosen_product['product_id'],
                "price": chosen_product['price']}
            for chosen_product in [random.choice(csv_content) for _ in range(num_items)]
        ],
        "customer": {
            "id": generate_id(MAX_CUSTOMER_ID),
            "name": customer_name,
            "email": f"{''.join(customer_name.split())}@email.com",
            "address": generate_quebec_address(),
            "phone": fake.phone_number(),
            "card_number": fake.credit_card_number()
        }
    }
    append_to_json(JSON, data)

def transaction_returning_customer(csv_content):
    global MAX_CUSTOMER_ID
    MAX_CUSTOMER_ID = get_max_customer_id()
    num_items = random.randint(1, 5)

    if not os.path.exists(JSON):
        # If the file doesn't exist, create a new customer as in the transaction_new_customer function
        transaction_new_customer(csv_content)
        return

    # Select a random customer from the JSON data
    json_data = read_json(JSON)
    existing_customer = random.choice(json_data)
    
    # Generate transaction data using the selected customer's details
    data = {
        "id": uuid.uuid4().hex,
        "date": generate_date(),
        # returns the product_id and price of a random product in 'products.csv' for a random number of items
        "items": [
            {"product_id": chosen_product['product_id'],
                "price": chosen_product['price']}
            for chosen_product in [random.choice(csv_data) for _ in range(num_items)]
        ],
        "customer": {
            "id": existing_customer["customer"]["id"],
            "name": existing_customer["customer"]["name"],
            "email": existing_customer["customer"]["email"],
            "address": existing_customer["customer"]["address"],
            "phone": existing_customer["customer"]["phone"],
            "card_number": existing_customer["customer"]["card_number"]
        }
    }
    append_to_json(JSON, data)

def generate_fake_transaction(csv_content, amount=100):
    for _ in range(amount):
        # Decide whether the customer will be a new or returning customer at an 25% return rate
        is_repeat_customer = random.choices([True, False], weights=[0.25, 0.75])[0]

        if is_repeat_customer:
            transaction_returning_customer(csv_content)
        else:
            transaction_new_customer(csv_content)


def main(csv_content, amount):
    generate_fake_transaction(csv_content, amount)