import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_GROUP = os.getenv("KAFKA_GROUP")

CREATE_CUSTOMER_TABLE = """
CREATE TABLE IF NOT EXISTS customer
(
    customer_id Int32,
    last_name Nullable(String),
    address_line1 Nullable(String),
    address_line2 Nullable(String),
    birth_date Nullable(String),
    commute_distance Nullable(Int32),
    customer_alternate_key Nullable(String),
    date_first_purchase Nullable(String),
    email_address Nullable(String),
    education Nullable(String),
    occupation Nullable(String),
    first_name Nullable(String),
    gender Nullable(String),
    house_owner_flag Nullable(UInt8),
    marital_status Nullable(String),
    middle_name Nullable(String),
    name_style Nullable(String),
    number_cars_owned Nullable(Int32),
    number_children_at_home Nullable(Int32),
    phone Nullable(String),
    suffix Nullable(String),
    title Nullable(String),
    total_children Nullable(String),
    yearly_income Nullable(Int32),
    created_at Nullable(String),
    record_version Nullable(Int64),
    arrival_date_time_tz DateTime DEFAULT now(),
    source_updated_at_ms Nullable(Int64),
    db_action Nullable(String),
    is_deleted Nullable(UInt8)
) ENGINE = MergeTree()
ORDER BY (customer_id);
"""

CREATE_CUSTOMER_QUEUE_TABLE = """
CREATE TABLE IF NOT EXISTS customer_queue
(
    all String
) ENGINE = Kafka('{broker}', 'rembo.public.customer', '{group}', 'JSONAsString');
""".format(broker=KAFKA_BROKER, group=KAFKA_GROUP)

CREATE_CUSTOMER_MV = """
CREATE MATERIALIZED VIEW IF NOT EXISTS customer_mv TO customer
AS
SELECT
    JSONExtract(all, 'payload', 'customer_id', 'Int32') AS customer_id,
    JSONExtract(all, 'payload', 'last_name', 'Nullable(String)') AS last_name,
    JSONExtract(all, 'payload', 'address_line1', 'Nullable(String)') AS address_line1,
    JSONExtract(all, 'payload', 'address_line2', 'Nullable(String)') AS address_line2,
    JSONExtract(all, 'payload', 'birth_date', 'Nullable(String)') AS birth_date,
    JSONExtract(all, 'payload', 'commute_distance', 'Nullable(Int32)') AS commute_distance,
    JSONExtract(all, 'payload', 'customer_alternate_key', 'Nullable(String)') AS customer_alternate_key,
    JSONExtract(all, 'payload', 'date_first_purchase', 'Nullable(String)') AS date_first_purchase,
    JSONExtract(all, 'payload', 'email_address', 'Nullable(String)') AS email_address,
    JSONExtract(all, 'payload', 'education', 'Nullable(String)') AS education,
    JSONExtract(all, 'payload', 'occupation', 'Nullable(String)') AS occupation,
    JSONExtract(all, 'payload', 'first_name', 'Nullable(String)') AS first_name,
    JSONExtract(all, 'payload', 'gender', 'Nullable(String)') AS gender,
    JSONExtract(all, 'payload', 'house_owner_flag', 'Nullable(UInt8)') AS house_owner_flag,
    JSONExtract(all, 'payload', 'marital_status', 'Nullable(String)') AS marital_status,
    JSONExtract(all, 'payload', 'middle_name', 'Nullable(String)') AS middle_name,
    JSONExtract(all, 'payload', 'name_style', 'Nullable(String)') AS name_style,
    JSONExtract(all, 'payload', 'number_cars_owned', 'Nullable(Int32)') AS number_cars_owned,
    JSONExtract(all, 'payload', 'number_children_at_home', 'Nullable(Int32)') AS number_children_at_home,
    JSONExtract(all, 'payload', 'phone', 'Nullable(String)') AS phone,
    JSONExtract(all, 'payload', 'suffix', 'Nullable(String)') AS suffix,
    JSONExtract(all, 'payload', 'title', 'Nullable(String)') AS title,
    JSONExtract(all, 'payload', 'total_children', 'Nullable(String)') AS total_children,
    JSONExtract(all, 'payload', 'yearly_income', 'Nullable(Int32)') AS yearly_income,
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
FROM customer_queue;
"""
CREATE_VIEW_CUSTOMER = """
CREATE VIEW view_customer_last_rec AS
WITH LatestCustomer AS (
    SELECT
        c1.customer_id,
        MAX(record_version) AS max_record_version
    FROM customer c1
    WHERE c1.is_deleted = 0
    GROUP BY c1.customer_id
)
SELECT
    c.customer_id,
    c.last_name, c.address_line1, c.address_line2, c.birth_date, c.commute_distance,
    c.customer_alternate_key, c.date_first_purchase, c.email_address, c.education, c.occupation,
    c.first_name, c.gender, c.house_owner_flag, c.marital_status, c.middle_name, c.name_style,
    c.number_cars_owned, c.number_children_at_home, c.phone, c.suffix, c.title,
    c.total_children, c.yearly_income, c.created_at, c.record_version, c.arrival_date_time_tz,
    c.source_updated_at_ms, c.db_action, c.is_deleted
FROM customer c
INNER JOIN LatestCustomer lc ON c.customer_id = lc.customer_id AND c.record_version = lc.max_record_version
WHERE c.is_deleted = 0;
"""

DROP_VIEW_SALES_CUSTOMER = 'DROP VIEW IF EXISTS view_customer_last_rec;'
DROP_CUSTOMER = 'DROP TABLE IF EXISTS customer;'
DROP_CUSTOMER_MV = 'DROP TABLE IF EXISTS customer_mv;'
DROP_CUSTOMER_QUEUE = 'DROP TABLE IF EXISTS customer_queue;'


CUSTOMER_QUERIES = [DROP_VIEW_SALES_CUSTOMER, DROP_CUSTOMER, DROP_CUSTOMER_MV, DROP_CUSTOMER_QUEUE, CREATE_CUSTOMER_TABLE, CREATE_CUSTOMER_QUEUE_TABLE, CREATE_CUSTOMER_MV, CREATE_VIEW_CUSTOMER]
