import os

from dotenv import load_dotenv

load_dotenv()


PG_HOST = os.getenv('PG_DB_HOST')
PG_DB = os.getenv('PG_DB_NAME')
PG_USER = os.getenv('PG_DB_USER')
PG_PASSWORD = os.getenv('PG_DB_PASSWORD')

INSERT_SALES_TERRITORY_QUERY = f"""
INSERT INTO sales_territory SELECT
sales_territory_id,
sales_territory_country,
sales_territory_region,
sales_territory_city,
created_at,
1 AS record_version,
now() AS arrival_date_time_tz,
toUnixTimestamp(now()) AS source_updated_at_ms,
'create' AS db_action,
0 AS is_deleted
FROM postgresql('{PG_HOST}', '{PG_DB}', 'sales_territory', '{PG_USER}', '{PG_PASSWORD}');
"""

INSERT_EMPLOYEE_QUERY = f"""
INSERT INTO employee SELECT
employee_id,
employee_name,
sales_territory_id,
created_at,
1 AS record_version,
now() AS arrival_date_time_tz,
toUnixTimestamp(now()) AS source_updated_at_ms,
'create' AS db_action,
0 AS is_deleted
FROM postgresql('{PG_HOST}', '{PG_DB}', 'employee', '{PG_USER}', '{PG_PASSWORD}');
"""

INSERT_CUSTOMER_QUERY = f"""
INSERT INTO customer SELECT
customer_id,
last_name,
address_line1,
address_line2,
birth_date,
commute_distance,
customer_alternate_key,
date_first_purchase,
email_address,
education,
occupation,
first_name,
gender,
house_owner_flag,
marital_status,
middle_name,
name_style,
number_cars_owned,
number_children_at_home,
phone,
suffix,
title,
total_children,
yearly_income,
created_at,
1 AS record_version,
now() AS arrival_date_time_tz,
toUnixTimestamp(now()) AS source_updated_at_ms,
'create' AS db_action,
0 AS is_deleted
FROM postgresql('{PG_HOST}', '{PG_DB}', 'customer', '{PG_USER}', '{PG_PASSWORD}');
"""

INSERT_SALES_QUERY = f"""
INSERT INTO sales SELECT
sales_id,
currencykey,
customer_id,
sales_amount,
sales_territory_id,
employee_id,
order_quantity,
order_date,
duedate,
shipdate,
tax_amt,
total_product_cost,
unit_price,
unit_price_discount_pct,
discount_amount,
duedatekey,
extended_amount,
freight,
product_standard_cost,
revision_number,
sales_order_line_number,
sales_order_number,
created_at,
1 AS record_version,
now() AS arrival_date_time_tz,
toUnixTimestamp(now()) AS source_updated_at_ms,
'create' AS db_action,
0 AS is_deleted
FROM postgresql('{PG_HOST}', '{PG_DB}', 'sales', '{PG_USER}', '{PG_PASSWORD}');
"""




# Create a list of queries
INSERT_QUERIES = [INSERT_SALES_TERRITORY_QUERY, INSERT_CUSTOMER_QUERY, INSERT_EMPLOYEE_QUERY, INSERT_SALES_QUERY ]

# You can access the queries using the list like this:
# for query in insert_queries:
#     print(query)
