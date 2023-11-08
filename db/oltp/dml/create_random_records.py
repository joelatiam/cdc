
from dotenv import load_dotenv
import os
import datetime
import random
import math

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
# Create a Faker object
fake = Faker()
# number of rows to insert
CUSTOMER_AND_SELL_NUM_ROW = 500000

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


sales_territories = [
    {
        "Sales_Territory_Country": "South Africa",
        "Sales_Territory_Region": "Western Cape",
        "Sales_Territory_City": "Cape Town",
        "Created_At": fake.date_time_between(start_date="-100y", end_date="-30y"),
    },
    {
        "Sales_Territory_Country": "South Africa",
        "Sales_Territory_Region": "Gauteng",
        "Sales_Territory_City": "Johannesburg",
        "Created_At": fake.date_time_between(start_date="-100y", end_date="-30y"),
    },
    {
        "Sales_Territory_Country": "Nigeria",
        "Sales_Territory_Region": "Lagos",
        "Sales_Territory_City": "Lagos",
        "Created_At": fake.date_time_between(start_date="-100y", end_date="-30y"),
    },
    {
        "Sales_Territory_Country": "Egypt",
        "Sales_Territory_Region": "Cairo",
        "Sales_Territory_City": "Cairo",
        "Created_At": fake.date_time_between(start_date="-100y", end_date="-30y")
    },
    {
        "Sales_Territory_Country": "Kenya",
        "Sales_Territory_Region": "Nairobi",
        "Sales_Territory_City": "Nairobi",
        "Created_At": fake.date_time_between(start_date="-100y", end_date="-30y"),
    },
    {
        "Sales_Territory_Country": "Rwanda",
        "Sales_Territory_Region": "Kigali City",
        "Sales_Territory_City": "Kigali",
        "Created_At": fake.date_time_between(start_date="-100y", end_date="-30y"),
    },
    {
        "Sales_Territory_Country": "Ghana",
        "Sales_Territory_Region": "Greater Accra",
        "Sales_Territory_City": "Accra",
        "Created_At": fake.date_time_between(start_date="-100y", end_date="-30y"),
    },
    {
        "Sales_Territory_Country": "Ethiopia",
        "Sales_Territory_Region": "Addis Ababa",
        "Sales_Territory_City": "Addis Ababa",
        "Created_At": fake.date_time_between(start_date="-100y", end_date="-30y"),
    },
    {
        "Sales_Territory_Country": "Cameroon",
        "Sales_Territory_Region": "Douala",
        "Sales_Territory_City": "Douala",
        "Created_At": fake.date_time_between(start_date="-100y", end_date="-30y"),
    },
    {
        "Sales_Territory_Country": "Tanzania",
        "Sales_Territory_Region": "Dar es Salaam",
        "Sales_Territory_City": "Dar es Salaam",
        "Created_At": fake.date_time_between(start_date="-100y", end_date="-30y"),
    },
    {
        "Sales_Territory_Country": "Morocco",
        "Sales_Territory_Region": "Casablanca",
        "Sales_Territory_City": "Casablanca",
        "Created_At": fake.date_time_between(start_date="-100y", end_date="-30y"),
    }
]

sales_territory_df = pd.DataFrame(sales_territories)
sales_territory_df.columns = [col.lower()
                              for col in sales_territory_df.columns]
insert_data_to_db(sales_territory_df, "sales_territory")
print("Inserted 11 records into the Sales Territory table.")


employee_data = [
    {
        "Employee_Name": "John Ruto",
        "Sales_Territory_Id": fake.random_int(min=1, max=11),
        "Created_At": fake.date_time_between(start_date="-20y", end_date="-10y"),
    },
    {
        "Employee_Name": "Jeanne Ingabire",
        "Sales_Territory_Id": fake.random_int(min=1, max=11),
        "Created_At": fake.date_time_between(start_date="-20y", end_date="-10y"),
    },
    {
        "Employee_Name": "Bob Johnson",
        "Sales_Territory_Id": fake.random_int(min=1, max=11),
        "Created_At": fake.date_time_between(start_date="-20y", end_date="-10y"),
    },
    {
        "Employee_Name": "Mary Mputu",
        "Sales_Territory_Id": fake.random_int(min=1, max=11),
        "Created_At": fake.date_time_between(start_date="-20y", end_date="-10y"),
    },
    {
        "Employee_Name": "Mike Wilson",
        "Sales_Territory_Id": fake.random_int(min=1, max=11),
        "Created_At": fake.date_time_between(start_date="-20y", end_date="-10y"),
    },
]

employee_df = pd.DataFrame(employee_data)
employee_df.columns = [col.lower() for col in employee_df.columns]
insert_data_to_db(employee_df, "employee")
print("Inserted 5 records into the Employee table.")

CUSTOMER_COLUMS = [
    "Last_Name",
    "Address_Line1",
    "Address_Line2",
    "Birth_Date",
    "Commute_Distance",
    "Customer_Alternate_Key",
    "Date_First_Purchase",
    "Email_Address",
    "Education",
    "Occupation",
    "First_Name",
    "Gender",
    "House_Owner_Flag",
    "Marital_Status",
    "Middle_Name",
    "Name_Style",
    "Number_Cars_Owned",
    "Number_Children_At_Home",
    "Phone",
    "Suffix",
    "Title",
    "Total_Children",
    "Yearly_Income",
    "Created_At",
]

customer_df = pd.DataFrame(columns=CUSTOMER_COLUMS)

SALES_COLUMNS = [
    "CurrencyKey",
    "Customer_id",
    "Discount_Amount",
    "DueDate",
    "DueDateKey",
    "Extended_Amount",
    "Freight",
    "Order_Date",
    "Order_Quantity",
    "Product_Standard_Cost",
    "Revision_Number",
    "Sales_Amount",
    "Sales_Order_Line_Number",
    "Sales_Order_Number",
    "Sales_Territory_Id",
    "ShipDate",
    "Tax_Amt",
    "Total_Product_Cost",
    "Unit_Price",
    "Unit_Price_Discount_Pct",
    "Employee_Id",
    "Created_At"
]
sales_df = pd.DataFrame(columns=SALES_COLUMNS)

customers_and_sales_to_create = 5000
for i in range(customers_and_sales_to_create):
    end_date = datetime.datetime.now()
    customer_created_at = fake.date_time_between(start_date="-13y", end_date="-11y")
    customer_dict = {
        "Last_Name": fake.last_name(),
        "Address_Line1": fake.street_address(),
        "Address_Line2": fake.secondary_address(),
        "Birth_Date": fake.date_of_birth(minimum_age=18, maximum_age=80),
        "Commute_Distance": fake.random_int(min=0, max=100),
        "Customer_Alternate_Key": fake.swift11(),
        "Date_First_Purchase": fake.date_time_between(start_date=customer_created_at, end_date=end_date),
        "Email_Address": fake.email(),
        "Education": fake.random_element(["Bachelor", "Master", "Phd", "Primary", "College"]),
        "Occupation": fake.random_element(["Footballer", "Military", "Public Servant", "Lawyer", "Trader"]),
        "First_Name": fake.first_name(),
        "Gender": fake.random_element(["Male", "Female"]),
        "House_Owner_Flag": fake.random_element([True, False]),
        "Marital_Status": fake.random_element(["Single", "Married"]),
        "Middle_Name": fake.last_name(),
        "Name_Style": fake.word(),
        "Number_Cars_Owned": fake.random_int(min=0, max=10),
        "Number_Children_At_Home": fake.random_int(min=0, max=10),
        "Phone": fake.phone_number(),
        "Suffix": fake.word(),
        "Title": fake.random_element([
            "Mr.",
            "Mrs.",
            "Miss",
            "Ms.",
            "Sir",
            "Madam"
        ]),
        "Total_Children": fake.random_int(min=0, max=5),
        "Yearly_Income": fake.random_int(min=20000, max=1000000),
        "Created_At": customer_created_at,
    }

    customer_df = customer_df._append(customer_dict, ignore_index=True)

    customer_memory_usage = customer_df.memory_usage(
        deep=True).sum() / (1024 ** 2)  # in megabytes
    print(i, f"Memory usage of customer_df: {customer_memory_usage:.2f} MB")

    employee_id = random.randint(1, 5)
    sales_territory_key = random.randint(1, 10)
    customer_id = random.randint(1, customers_and_sales_to_create)

    sales_created_at = fake.date_time_between(start_date="-13y", end_date="-11y")
    sales_amount = int(fake.random_int(min=10, max=100))
    order_quantity = fake.random_int(min=1, max=20)
    discount_amount = fake.random_int(min=0, max=int(sales_amount/5))

    delta = datetime.timedelta(hours=random.randint(1, 4))

    if not sales_df.empty:
        last_date_value = sales_df.iloc[-1]['Created_At']
        sales_created_at = fake.date_time_between(start_date=last_date_value, end_date=last_date_value + delta)

    if sales_created_at > end_date:
        sales_created_at = fake.date_time_between(start_date=last_date_value, end_date=end_date)

    sales_dict = {
        "CurrencyKey": fake.currency(),
        "Customer_id": customer_id,
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
        "Sales_Territory_Id": sales_territory_key,
        "ShipDate": fake.date_time_between(start_date=sales_created_at, end_date=sales_created_at + delta),
        "Tax_Amt": fake.random_int(min=0, max=int(sales_amount/3)),
        "Total_Product_Cost": fake.random_int(min=sales_amount, max=int(sales_amount*1.5)),
        "Unit_Price": int(sales_amount/order_quantity),
        "Unit_Price_Discount_Pct": int(discount_amount/order_quantity),
        "Employee_Id": employee_id,
        "Created_At": sales_created_at,
    }
    sales_df = sales_df._append(sales_dict, ignore_index=True)

    # Calculate memory usage for sales_df
    sales_memory_usage = sales_df.memory_usage(
        deep=True).sum() / (1024 ** 2)  # in megabytes
    print(i, f"Memory usage of sales_df: {sales_memory_usage:.2f} MB")


customer_df.columns = [col.lower() for col in customer_df.columns]
sales_df.columns = [col.lower() for col in sales_df.columns]


for i in range(CUSTOMER_AND_SELL_NUM_ROW//customers_and_sales_to_create):
    insert_data_to_db(customer_df, "customer")
    print('inserted: ', (i + 1) * customers_and_sales_to_create, "customer")

    if i > 0:
        delta = datetime.timedelta(days=random.randint(3, 60))

        sales_amount_to_add = 3
        end_date = datetime.datetime.today()

        def compute_amount(x, sales_amount_to_add):
            new_amount = math.ceil((x * sales_amount_to_add) / 2)
            
            if new_amount > 1000:
                return math.ceil((x / sales_amount_to_add) / 2)
            return new_amount
        
        def compute_date(x, end_date):
            start_date = datetime.datetime.fromtimestamp((x+ delta).timestamp())
            if (start_date > end_date) or (start_date + delta > end_date):
                new_delta = datetime.timedelta(days=365*3)
                return fake.date_time_between(start_date= end_date - new_delta, end_date=end_date)
            return fake.date_time_between(start_date=start_date, end_date=start_date + delta)
        
        def get_id(limit):
            start = (i * limit) + 1
            end = (i+1) * limit
            return random.randint(start , end)

        sales_df['sales_amount'] = sales_df['sales_amount'].apply(lambda x: compute_amount(x, sales_amount_to_add))
        sales_df['order_date'] = sales_df['order_date'].apply(lambda x: compute_date(x, end_date))
        sales_df['created_at'] = sales_df['order_date']
        sales_df['duedate'] = sales_df['order_date'] + delta
        sales_df['shipdate'] = sales_df['duedate'] - delta
        sales_df['total_product_cost'] = sales_df['total_product_cost'].apply(lambda x: compute_amount(x, sales_amount_to_add))
        sales_df['unit_price'] = sales_df['sales_amount'] // sales_df['order_quantity']

        sales_df['customer_id'] = sales_df['customer_id'].apply(lambda x: get_id(len(customer_df)))

    insert_data_to_db(sales_df, "sales")
    print('inserted: ', (i + 1) * customers_and_sales_to_create, "sales")


cur.close()
conn.close()
