import os

from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_GROUP = os.getenv("KAFKA_GROUP")

CREATE_SALES_QUEUE_TABLE = """
CREATE TABLE IF NOT EXISTS sales_queue
(
    all String
) ENGINE = Kafka('{broker}', 'rembo.public.sales', '{group}', 'JSONAsString');
""".format(broker = KAFKA_BROKER, group = KAFKA_GROUP)

CREATE_SALES_ORDERS_MV = """
CREATE MATERIALIZED VIEW IF NOT EXISTS sales_mv TO sales
AS
SELECT
    JSONExtract(all, 'payload', 'sales_id', 'UInt32') AS sales_id,
    JSONExtract(all, 'payload', 'currencykey', 'Nullable(String)') AS currencykey,
    JSONExtract(all, 'payload', 'customer_id', 'Nullable(Int32)') AS customer_id,
    JSONExtract(all, 'payload', 'discount_amount', 'Nullable(String)') AS discount_amount,
    JSONExtract(all, 'payload', 'duedate', 'Nullable(String)') AS duedate,
    JSONExtract(all, 'payload', 'duedatekey', 'Nullable(Int32)') AS duedatekey,
    JSONExtract(all, 'payload', 'extended_amount', 'Nullable(Int32)') AS extended_amount,
    JSONExtract(all, 'payload', 'freight', 'Nullable(Int32)') AS freight,
    JSONExtract(all, 'payload', 'order_date', 'Nullable(Int32)') AS order_date,
    JSONExtract(all, 'payload', 'order_quantity', 'Nullable(Int32)') AS order_quantity,
    JSONExtract(all, 'payload', 'product_standard_cost', 'Nullable(Int32)') AS product_standard_cost,
    JSONExtract(all, 'payload', 'revision_number', 'Nullable(Int32)') AS revision_number,
    JSONExtract(all, 'payload', 'sales_amount', 'Nullable(Int32)') AS sales_amount,
    JSONExtract(all, 'payload', 'sales_order_line_number', 'Nullable(String)') AS sales_order_line_number,
    JSONExtract(all, 'payload', 'sales_order_number', 'Nullable(String)') AS sales_order_number,
    JSONExtract(all, 'payload', 'sales_territory_id', 'Nullable(Int32)') AS sales_territory_id,
    JSONExtract(all, 'payload', 'shipdate', 'Nullable(Int32)') AS shipdate,
    JSONExtract(all, 'payload', 'tax_amt', 'Nullable(Int32)') AS tax_amt,
    JSONExtract(all, 'payload', 'total_product_cost', 'Nullable(Int32)') AS total_product_cost,
    JSONExtract(all, 'payload', 'unit_price', 'Nullable(Int32)') AS unit_price,
    JSONExtract(all, 'payload', 'unit_price_discount_pct', 'Nullable(Int32)') AS unit_price_discount_pct,
    JSONExtract(all, 'payload', 'employee_id', 'Nullable(UInt32)') AS employee_id,
    JSONExtract(all, 'payload', 'created_at', 'Nullable(String)') AS created_at,
    CASE
        WHEN JSONExtract(all, 'payload', '__op', 'String') = 'c' THEN 'create'
        WHEN JSONExtract(all, 'payload', '__op', 'String') = 'u' THEN 'update'
        WHEN JSONExtract(all, 'payload', '__op', 'String') = 'd' THEN 'delete'
        WHEN JSONExtract(all, 'payload', '__op', 'String') = 'r' THEN 'read'
        WHEN JSONExtract(all, 'payload', '__op', 'String') = 't' THEN 'truncate'
        WHEN JSONExtract(all, 'payload', '__op', 'String') = 'm' THEN 'message'
        ELSE NULL
    END AS db_action,
    toUInt8(JSONExtract(all, 'payload', '__deleted', 'Nullable(String)') = 'true') AS is_deleted,
    JSONExtract(all, 'payload', '__op', 'Nullable(String)') AS operation,
    JSONExtract(all, 'payload', '__lsn', 'Nullable(Int64)') AS record_version,
    JSONExtract(all, 'payload', '__source_ts_ms', 'Nullable(Int64)') AS source_updated_at_ms,
    now() AS arrival_date_time_tz
FROM sales_queue;
"""


DROP_SALE_MV = 'DROP TABLE IF EXISTS sales_mv;'
DROP_SALE_QUEUE = 'DROP TABLE IF EXISTS sales_queue;'

SALES_QUERIES = [DROP_SALE_MV, DROP_SALE_QUEUE, CREATE_SALES_QUEUE_TABLE, CREATE_SALES_ORDERS_MV]
