from faker import Faker
import random
from datetime import datetime, timedelta
import csv
import json
import os
import uuid
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# -------------------- VALUES -------------------- #

START_DATE = datetime.today() - timedelta(days=365)
END_DATE = datetime.today()

JSON = "/opt/airflow/dags/scripts/transactions.json"

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

def fetch_product_data(key='products/products.csv', bucket_name='data-pipeline-repo', **kwargs):
    s3_hook = S3Hook(aws_conn_id='aws_airflow')
    csv_file = s3_hook.get_key(key, bucket_name).get()['Body'].read().decode('utf-8')

    reader = csv.reader(csv_file.splitlines())
    # Skip the header row
    next(reader, None)

    product_data = []
    for row in reader:
        product = {
            "product_id": row[0],
            "product_name": row[1],
            "product_brand": row[2],
            "gender": row[3],
            "price": float(row[4]),
            "description": row[5],
            "primary_color": row[6] if row[6] else "NA"  # Some rows have missing color
        }
        product_data.append(product)
    return product_data

def read_json(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        json_data = json.load(f)
    return json_data

def append_to_json(file_path, data):
    try:
        with open(file=file_path, mode='r', encoding='utf-8') as f:
            existing_data = json.load(f)
    except FileNotFoundError:
        existing_data = []
    
    existing_data.append(data)
    with open(file=file_path, mode='w', encoding='utf-8') as f:
        json.dump(existing_data, f, ensure_ascii=False)
        f.write('\n')

def transaction_new_customer(product_data, max_id):
    customer_name = fake.name()
    num_items = random.randint(1, 5)
    data = {
        "id": uuid.uuid4().hex,
        "date": generate_date(),
        # returns the product_id and price of a random product in 'products.csv' for a random number of items
        "items": [
            {"product_id": chosen_product['product_id'],
                "price": chosen_product['price']}
            for chosen_product in [random.choice(product_data) for _ in range(num_items)]
        ],
        "customer": {
            "id": max_id[0] + 1,
            "name": customer_name,
            "email": (f"{''.join(customer_name.split())}@email.com").replace('"', ''),
            "address": generate_quebec_address().replace('"', ''),
            "phone": fake.phone_number(),
            "card_number": fake.credit_card_number().replace('"', '')
        }
    }
    max_id[0] += 1
    append_to_json(JSON, data)

def transaction_returning_customer(product_data, max_customer_id):
    num_items = random.randint(1, 5)

    if not os.path.exists(JSON):
        # If the file doesn't exist, create a new customer as in the transaction_new_customer function
        transaction_new_customer(product_data, max_customer_id)
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
            for chosen_product in [random.choice(product_data) for _ in range(num_items)]
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

def generate_fake_transaction(amount, max_customer_id):
    if os.path.exists(JSON):
        os.remove(JSON)
    max_id = [int(max_customer_id[0]["MAX_CUSTOMER_ID"]) if max_customer_id[0]["MAX_CUSTOMER_ID"] is not None else 0]
    product_data = fetch_product_data()
    for _ in range(amount):
        print(_)
        # Decide whether the customer will be a new or returning customer at an 25% return rate
        is_repeat_customer = random.choices([True, False], weights=[0.25, 0.75])[0]

        if is_repeat_customer:
            transaction_returning_customer(product_data, max_id)
        else:
            transaction_new_customer(product_data, max_id)

def upload_to_s3(file_path=JSON, key='transactions/transactions.json', bucket_name='data-pipeline-repo', **kwargs):
    s3_hook = S3Hook(aws_conn_id='aws_airflow')
    
    # Upload the data to S3
    s3_hook.load_file(
        filename=file_path,
        key=key,
        bucket_name=bucket_name,
        replace=True
    )
