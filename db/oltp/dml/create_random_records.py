
from dotenv import load_dotenv
import os

import random
import string

import numpy as np
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
    # Insert data into the table
    df.to_sql(table_name, con=engine, if_exists="append", index=False)


sales_territories = [
    {
        "Sales_Territory_Country": "South Africa",
        "Sales_Territory_Region": "Western Cape",
        "Sales_Territory_City": "Cape Town",
        "Created_At": fake.date_between(start_date="-100y", end_date="-30y"),
    },
    {
        "Sales_Territory_Country": "South Africa",
        "Sales_Territory_Region": "Gauteng",
        "Sales_Territory_City": "Johannesburg",
        "Created_At": fake.date_between(start_date="-100y", end_date="-30y"),
    },
    {
        "Sales_Territory_Country": "Nigeria",
        "Sales_Territory_Region": "Lagos",
        "Sales_Territory_City": "Lagos",
        "Created_At": fake.date_between(start_date="-100y", end_date="-30y"),
    },
    {
        "Sales_Territory_Country": "Egypt",
        "Sales_Territory_Region": "Cairo",
        "Sales_Territory_City": "Cairo",
        "Created_At": fake.date_between(start_date="-100y", end_date="-30y")
    },
    {
        "Sales_Territory_Country": "Kenya",
        "Sales_Territory_Region": "Nairobi",
        "Sales_Territory_City": "Nairobi",
        "Created_At": fake.date_between(start_date="-100y", end_date="-30y"),
    },
    {
        "Sales_Territory_Country": "Morocco",
        "Sales_Territory_Region": "Casablanca",
        "Sales_Territory_City": "Casablanca",
        "Created_At": fake.date_between(start_date="-100y", end_date="-30y"),
    },
    {
        "Sales_Territory_Country": "Ghana",
        "Sales_Territory_Region": "Greater Accra",
        "Sales_Territory_City": "Accra",
        "Created_At": fake.date_between(start_date="-100y", end_date="-30y"),
    },
    {
        "Sales_Territory_Country": "Ethiopia",
        "Sales_Territory_Region": "Addis Ababa",
        "Sales_Territory_City": "Addis Ababa",
        "Created_At": fake.date_between(start_date="-100y", end_date="-30y"),
    },
    {
        "Sales_Territory_Country": "Algeria",
        "Sales_Territory_Region": "Algiers",
        "Sales_Territory_City": "Algiers",
        "Created_At": fake.date_between(start_date="-100y", end_date="-30y"),
    },
    {
        "Sales_Territory_Country": "Tanzania",
        "Sales_Territory_Region": "Dar es Salaam",
        "Sales_Territory_City": "Dar es Salaam",
        "Created_At": fake.date_between(start_date="-100y", end_date="-30y"),
    },
    {
        "Sales_Territory_Country": "Rwanda",
        "Sales_Territory_Region": "Kigali City",
        "Sales_Territory_City": "Kigali",
        "Created_At": fake.date_between(start_date="-100y", end_date="-30y"),
    }
]

sales_territory_df = pd.DataFrame(sales_territories)
sales_territory_df.columns = [col.lower()
                              for col in sales_territory_df.columns]
insert_data_to_db(sales_territory_df, "sales_territory")
print("Inserted 11 records into the Sales Territory table.")


employee_data = [
    {
        "Employee_Name": "John Doe",
        "Sales_Territory_Id": fake.random_int(min=1, max=11),
        "Created_At": fake.date_between(start_date="-20y", end_date="-10y"),
    },
    {
        "Employee_Name": "Jane Smith",
        "Sales_Territory_Id": fake.random_int(min=1, max=11),
        "Created_At": fake.date_between(start_date="-20y", end_date="-10y"),
    },
    {
        "Employee_Name": "Bob Johnson",
        "Sales_Territory_Id": fake.random_int(min=1, max=11),
        "Created_At": fake.date_between(start_date="-20y", end_date="-10y"),
    },
    {
        "Employee_Name": "Mary Brown",
        "Sales_Territory_Id": fake.random_int(min=1, max=11),
        "Created_At": fake.date_between(start_date="-20y", end_date="-10y"),
    },
    {
        "Employee_Name": "Mike Wilson",
        "Sales_Territory_Id": fake.random_int(min=1, max=11),
        "Created_At": fake.date_between(start_date="-20y", end_date="-10y"),
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


for i in range(2500):
    customer_created_at = fake.date_between(start_date="-10y", end_date="today")
    customer_dict = {
        "Last_Name": fake.last_name(),
        "Address_Line1": fake.street_address(),
        "Address_Line2": fake.secondary_address(),
        "Birth_Date": fake.date_of_birth(minimum_age=18, maximum_age=80).strftime("%Y-%m-%d"),
        "Commute_Distance": fake.random_int(min=0, max=100),
        "Customer_Alternate_Key": fake.swift11(),
        "Date_First_Purchase": fake.date_between(start_date=customer_created_at, end_date="today").strftime("%Y-%m-%d"),
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
    customer_id = random.randint(1, 2500)

    sales_created_at = fake.date_between(start_date="-10y", end_date="today").strftime("%Y-%m-%d")
    sales_dict = {
        "CurrencyKey": fake.currency(),
        "Customer_id": customer_id,
        "Discount_Amount": fake.random_int(min=0, max=100),
        "DueDate": fake.date_between(start_date="-30d", end_date="today").strftime("%Y-%m-%d"),
        "DueDateKey": fake.date_between(start_date="-30d", end_date="today").strftime("%Y-%m-%d"),
        "Extended_Amount": fake.random_int(min=0, max=1000),
        "Freight": fake.random_int(min=0, max=500),
        "Order_Date": sales_created_at,
        "Order_Quantity": fake.random_int(min=1, max=100),
        "Product_Standard_Cost": fake.random_int(min=10, max=100),
        "Revision_Number": fake.random_int(min=10, max=100),
        "Sales_Amount": fake.random_int(min=1, max=10),
        "Sales_Order_Line_Number": fake.random_int(min=1, max=10),
        "Sales_Order_Number": fake.random_int(min=1, max=10),
        "Sales_Territory_Id": sales_territory_key,
        "ShipDate": fake.date_between(start_date="-30d", end_date="today").strftime("%Y-%m-%d"),
        "Tax_Amt": fake.random_int(min=0, max=100),
        "Total_Product_Cost": fake.random_int(min=0, max=1000),
        "Unit_Price": fake.random_int(min=5000, max=100000),
        "Unit_Price_Discount_Pct": fake.random_int(min=1, max=2500),
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


for i in range(CUSTOMER_AND_SELL_NUM_ROW//2500):
    insert_data_to_db(customer_df, "customer")
    print('inserted: ', (i + 1) * 2500, "customer")

    insert_data_to_db(sales_df, "sales")
    print('inserted: ', (i + 1) * 2500, "sales")


cur.close()
conn.close()
