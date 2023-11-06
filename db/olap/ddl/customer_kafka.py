import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_GROUP = os.getenv("KAFKA_GROUP")

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
    JSONExtract(all, 'payload', 'birth_date', 'Nullable(Date)') AS birth_date,
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
    parseDateTime64BestEffort(JSONExtract(all, 'payload', 'created_at', 'Nullable(String)')) AS created_at,
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

DROP_CUSTOMER_MV = 'DROP TABLE IF EXISTS customer_mv;'
DROP_CUSTOMER_QUEUE = 'DROP TABLE IF EXISTS customer_queue;'


CUSTOMER_QUERIES = [DROP_CUSTOMER_MV, DROP_CUSTOMER_QUEUE, CREATE_CUSTOMER_QUEUE_TABLE, CREATE_CUSTOMER_MV]
