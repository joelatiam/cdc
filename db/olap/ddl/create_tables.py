import os

from dotenv import load_dotenv
import clickhouse_connect

from customer import CUSTOMER_QUERIES
from sales import SALES_QUERIES
from sales_territory import SALES_TERRITORY_QUERIES
from employee import EMPLOYEE_QUERIES



load_dotenv()

# Get the ClickHouse credentials from the .env file
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
DB_NAME = os.getenv("CLICKHOUSE_DB_NAME")


table_queries = CUSTOMER_QUERIES + SALES_QUERIES + SALES_TERRITORY_QUERIES + EMPLOYEE_QUERIES

# Create a ClickHouse client
client = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD, database=DB_NAME)


for query in table_queries:
    client.command(query)

