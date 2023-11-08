
from dotenv import load_dotenv
import os
import datetime
import random

import pandas as pd
from faker import Faker
import psycopg2
from sqlalchemy import create_engine

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

fake = Faker()

conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
)

cur = conn.cursor()

def insert_data_to_db(df, table_name):
    # Define your PostgreSQL database connection URL
    db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(db_url)

    df_sorted = df.sort_values(by='created_at', ascending=True)

    # Insert data into the table
    df_sorted.to_sql(table_name, con=engine, if_exists="append", index=False)

# Create a Faker object
fake = Faker()

# Function to generate a single sales record with customer_id, employee_id, and sales_territory_id from existing data
def generate_single_sales_record():
    # Retrieve the latest customer_id, employee_id, and sales_territory_id from the database
    cur.execute("SELECT customer_id FROM customer ORDER BY customer_id DESC LIMIT 1;")
    last_customer_id = cur.fetchone()[0]

    cur.execute("SELECT employee_id FROM employee ORDER BY employee_id DESC LIMIT 1;")
    last_employee_id = cur.fetchone()[0]

    cur.execute("SELECT sales_territory_id FROM sales_territory ORDER BY sales_territory_id DESC LIMIT 1;")
    last_sales_territory_id = cur.fetchone()[0]

    # Get the current time as sales_created_at
    sales_created_at = datetime.datetime.now()
    sales_amount = int(fake.random_int(min=10, max=100))
    order_quantity = fake.random_int(min=1, max=20)
    discount_amount = fake.random_int(min=0, max=int(sales_amount/5))

    delta = datetime.timedelta(days=random.randint(1, 30))

    sales_record = {
        "CurrencyKey": fake.currency(),
        "Customer_id": fake.random_int(min=1, max=last_customer_id),
        "Discount_Amount": discount_amount,
        "DueDate": fake.date_time_between(start_date=sales_created_at, end_date=sales_created_at + delta),
        "DueDateKey":  fake.random_int(min=1, max=50),
        "Extended_Amount": fake.random_int(min=0, max=int(sales_amount*1.5)),
        "Freight": fake.random_int(min=0, max=500),
        "Order_Date": sales_created_at,
        "Order_Quantity": order_quantity,
        "Product_Standard_Cost": fake.random_int(min=10, max=100),
        "Revision_Number": fake.random_int(min=10, max=100),
        "Sales_Amount": sales_amount,
        "Sales_Order_Line_Number": fake.random_int(min=1, max=10),
        "Sales_Order_Number": fake.random_int(min=1, max=10),
        "Sales_Territory_Id": fake.random_int(min=1, max=last_sales_territory_id),
        "ShipDate": fake.date_time_between(start_date=sales_created_at, end_date=sales_created_at + delta),
        "Tax_Amt": fake.random_int(min=0, max=int(sales_amount/3)),
        "Total_Product_Cost": fake.random_int(min=sales_amount, max=int(sales_amount*1.5)),
        "Unit_Price": int(sales_amount/order_quantity),
        "Unit_Price_Discount_Pct": int(discount_amount/order_quantity),
        "Employee_Id": fake.random_int(min=1, max=last_employee_id),
        "Created_At": sales_created_at,
    }

    # Create a DataFrame for the sales record
    sales_df = pd.DataFrame([sales_record])
    sales_df.columns = [col.lower() for col in sales_df.columns]

    return sales_df

# Generate a single sales record with data from the database
single_sales_record = generate_single_sales_record()

# Insert the single sales record into the database
insert_data_to_db(single_sales_record, "sales")

# Print the inserted sales information
cur.execute("SELECT sales_id, sales_amount, order_date, customer_id, sales_territory_id, employee_id FROM sales WHERE sales_id = (SELECT max(sales_id) FROM sales);")
inserted_sales_info = cur.fetchone()
print("Inserted Sales Info:")
print(f"Sales ID: {inserted_sales_info[0]}")
print(f"Sales Amount: {inserted_sales_info[1]}")
print(f"Sales Order Date: {inserted_sales_info[2]}")
print(f"Customer ID: {inserted_sales_info[3]}")
print(f"Sales Territory ID: {inserted_sales_info[4]}")
print(f"Employee ID: {inserted_sales_info[5]}\n")


cur.close()
conn.close()
