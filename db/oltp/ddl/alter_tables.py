import psycopg2
from dotenv import load_dotenv
import os


load_dotenv()


DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# List of table names
TABLES = ['Customer', 'Sales_Territory', 'Employee', 'Sales']

# Connect to the PostgreSQL database
conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
)

cur = conn.cursor()


for table_name in TABLES:
    alter_sql = f"ALTER TABLE {table_name} REPLICA IDENTITY FULL;"
    cur.execute(alter_sql)
    print(f"Altered {table_name} with REPLICA IDENTITY FULL")


conn.commit()
conn.close()
