import os

from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_GROUP = os.getenv("KAFKA_GROUP")



CREATE_SALES_TABLE = """
CREATE TABLE IF NOT EXISTS sales
(
    sales_id UInt32,
    currencykey Nullable(String),
    customer_id Nullable(Int32),
    sales_amount Nullable(Int64),
    sales_territory_id Nullable(Int32),
    employee_id Nullable(Int32),
    order_quantity Nullable(Int32),
    order_date Nullable(DateTime),
    duedate Nullable(DateTime),
    shipdate Nullable(DateTime),
    tax_amt Nullable(Int32),
    total_product_cost Nullable(Int64),
    unit_price Nullable(Int32),
    unit_price_discount_pct Nullable(Int32),
    discount_amount Nullable(String),
    duedatekey Nullable(Int32),
    extended_amount Nullable(Int32),
    freight Nullable(Int32),
    product_standard_cost Nullable(Int32),
    revision_number Nullable(Int32),
    sales_order_line_number Nullable(String),
    sales_order_number Nullable(String),
    created_at Nullable(DateTime),
    record_version Int64,
    arrival_date_time_tz DateTime DEFAULT now(),
    source_updated_at_ms Nullable(Int64),
    db_action Nullable(String),
    is_deleted Nullable(UInt8)
) ENGINE = MergeTree()
ORDER BY (sales_id);
"""

CREATE_VIEW_SALES = """
CREATE VIEW view_sales_last_rec AS
SELECT * FROM 
(
WITH LatestSales AS (
    SELECT
        s1.sales_id,
        MAX(record_version) AS max_record_version
    FROM sales s1
    WHERE s1.is_deleted = 0
    GROUP BY s1.sales_id
)
SELECT
    s.sales_id,
    s.currencykey, s.customer_id, s.sales_amount, s.sales_territory_id, s.employee_id,
    s.order_quantity, s.order_date, s.duedate, s.shipdate, s.tax_amt, s.total_product_cost,
    s.unit_price, s.unit_price_discount_pct, s.discount_amount, s.duedatekey, s.extended_amount,
    s.freight, s.product_standard_cost, s.revision_number, s.sales_order_line_number,
    s.sales_order_number, s.created_at, s.record_version, s.arrival_date_time_tz,
    s.source_updated_at_ms, s.is_deleted
FROM sales s
INNER JOIN LatestSales ls ON s.sales_id = ls.sales_id AND s.record_version = ls.max_record_version
WHERE s.is_deleted = 0
) sales;
"""


DROP_VIEW_SALES = 'DROP VIEW IF EXISTS view_sales_last_rec;'
DROP_SALE = 'DROP TABLE IF EXISTS sales;'

SALES_QUERIES = [DROP_VIEW_SALES, DROP_SALE, CREATE_SALES_TABLE, CREATE_VIEW_SALES]
