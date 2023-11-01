import os

from dotenv import load_dotenv
import clickhouse_connect

from insert_queries import INSERT_QUERIES



load_dotenv()

# Get the ClickHouse credentials from the .env file
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
DB_NAME = os.getenv("CLICKHOUSE_DB_NAME")



# Create a ClickHouse client
client = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD, database=DB_NAME)


for query in INSERT_QUERIES:
    client.command(query)

print('Completed Postgres -> ClickHouse Records Transfer')
