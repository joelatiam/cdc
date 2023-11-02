import os

from dotenv import load_dotenv
import clickhouse_connect


from sales_territory_kafka import SALES_TERRITORY_QUERIES
from employee_kafka import EMPLOYEE_QUERIES
from customer_kafka import CUSTOMER_QUERIES
from sales_kafka import SALES_QUERIES



load_dotenv()

# Get the ClickHouse credentials from the .env file
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
DB_NAME = os.getenv("CLICKHOUSE_DB_NAME")


table_queries = SALES_TERRITORY_QUERIES

# Create a ClickHouse client
client = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD, database=DB_NAME)


for query in table_queries:
    client.command(query)


print('Created Kafka Tables + Material Views')
