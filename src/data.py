import json
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

# Load data from the JSON file
with open(os.getenv('DATA_PATH'), 'r') as file:
    data_list = json.load(file)  # Load JSON array from file

# Connect to PostgreSQL
conn = psycopg2.connect(
    host=os.getenv('HOST'),
    database=os.getenv('DB_NAME'),
    user=os.getenv('USER'),
    password=os.getenv('DB_PASSWORD')
)
cur = conn.cursor()
def truncate(value, max_length):
    return value[:max_length] if isinstance(value, str) and len(value) > max_length else value


# Loop over each item in the JSON array and insert it into the database
for data in data_list:
    # Convert date string to datetime format
    crawled_at = datetime.strptime(data["crawled_at"], "%d/%m/%Y, %H:%M:%S")
    average_rating = float(data["average_rating"]) if data["average_rating"].strip() else None

    # Insert data into the table
    cur.execute("""
        INSERT INTO products (
            _id, actual_price, average_rating, brand, category, crawled_at, description, discount, 
            images, out_of_stock, pid, product_details, seller, selling_price, sub_category, title, url
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """, (
        data["_id"],
        truncate(data["actual_price"], 50),
        average_rating,
        truncate(data["brand"], 100),
        truncate(data["category"], 100),
        crawled_at,
        data["description"],
        truncate(data["discount"], 50),
        json.dumps(data["images"]),
        data["out_of_stock"],
        truncate(data["pid"], 50),
        json.dumps(data["product_details"]),
        truncate(data["seller"], 100),
        truncate(data["selling_price"], 50),
        truncate(data["sub_category"], 100),
        truncate(data["title"], 255),
        data["url"]
    ))

# Commit the transaction
conn.commit()
