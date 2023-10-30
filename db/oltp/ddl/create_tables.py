
from dotenv import load_dotenv
import os

import psycopg2

# Load environment variables from .env file
load_dotenv()

# Get the database credentials from the .env file
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


# table creation SQL statements

DROP_SALES_TERRITORY_TABLE = 'DROP TABLE IF EXISTS Sales_Territory CASCADE;'
CREATE_SALES_TERRITORY_TABLE = """CREATE TABLE IF NOT EXISTS Sales_Territory (
    Sales_Territory_Id SERIAL NOT NULL,
    Sales_Territory_Country varchar(50) NULL,
    Sales_Territory_Region varchar(50) NULL,
    Sales_Territory_City varchar(50) NULL,
    Created_At TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (Sales_Territory_Id)
);"""

DROP_EMPLOYEE_TABLE = 'DROP TABLE IF EXISTS Employee CASCADE;'
CREATE_EMPLOYEE_TABLE = """CREATE TABLE IF NOT EXISTS Employee (
    Employee_Id SERIAL NOT NULL,
    Employee_Name varchar(50) NULL,
    Sales_Territory_Id INT NULL,
    Created_At TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (Employee_Id),
    FOREIGN KEY (Sales_Territory_Id) REFERENCES Sales_Territory (Sales_Territory_Id)
);"""

DROP_CUSTOMER_TABLE = 'DROP TABLE IF EXISTS Customer CASCADE;'
CREATE_CUSTOMER_TABLE = """CREATE TABLE IF NOT EXISTS Customer (
    Customer_Id SERIAL NOT NULL,
    Last_Name varchar(50) NULL,
    Address_Line1 varchar(50) NULL,
    Address_Line2 varchar(50) NULL,
    Birth_Date varchar(50) NULL,
    Commute_Distance INT NULL,
    Customer_Alternate_Key varchar(50) NULL,
    Date_First_Purchase varchar(50) NULL,
    Email_Address varchar(50) NULL,
    Education varchar(50) NULL,
    Occupation varchar(50) NULL,
    First_Name varchar(50) NULL,
    Gender varchar(50) NULL,
    House_Owner_Flag Boolean NULL,
    Marital_Status varchar(50) NULL,
    Middle_Name varchar(50) NULL,
    Name_Style varchar(50) NULL,
    Number_Cars_Owned INT NULL,
    Number_Children_At_Home INT NULL,
    Phone varchar(50) NULL,
    Suffix varchar(50) NULL,
    Title varchar(50) NULL,
    Total_Children varchar(50) NULL,
    Yearly_Income INT NULL,
    Created_At TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (Customer_Id)
);"""

DROP_SALES_TABLE = 'DROP TABLE IF EXISTS Sales CASCADE;'
CREATE_SALES_TABLE = """CREATE TABLE IF NOT EXISTS Sales (
        Sales_Id SERIAL NOT NULL,
        CurrencyKey varchar(50) NULL,
        Customer_id INT NULL,
        Discount_Amount varchar(50) NULL,
        DueDate varchar(50) NULL,
        DueDateKey Date NULL,
        Extended_Amount INT,
        Freight INT,
        Order_Date Date,
        Order_Quantity INT,
        Product_Standard_Cost INT,
        Revision_Number INT NULL,
        Sales_Amount INT,
        Sales_Order_Line_Number varchar(50) NULL,
        Sales_Order_Number varchar(50) NULL,
        Sales_Territory_Id INT NULL,
        ShipDate Date,
        Tax_Amt INT,
        Total_Product_Cost INT,
        Unit_Price INT,
        Unit_Price_Discount_Pct INT,
        Employee_Id INT NOT NULL,
        Created_At TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (Sales_Id),
        FOREIGN KEY (Employee_Id) REFERENCES Employee (Employee_Id),
        FOREIGN KEY (Sales_Territory_Id) REFERENCES Sales_Territory (Sales_Territory_Id),
        FOREIGN KEY (Customer_id) REFERENCES Customer (Customer_Id)
);"""

# Connect to the PostgreSQL database
conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
)

cur = conn.cursor()


TABLES = [
DROP_SALES_TERRITORY_TABLE, CREATE_SALES_TERRITORY_TABLE, DROP_EMPLOYEE_TABLE, CREATE_EMPLOYEE_TABLE, DROP_CUSTOMER_TABLE, CREATE_CUSTOMER_TABLE, DROP_SALES_TABLE, CREATE_SALES_TABLE
]

for table in TABLES:
    cur.execute(table)

# Commit the changes and close the connection
conn.commit()
conn.close()
