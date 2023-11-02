import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_GROUP = os.getenv("KAFKA_GROUP")



CREATE_SALES_TERRITORY_QUEUE_TABLE = """
CREATE TABLE IF NOT EXISTS sales_territory_queue
(
    all String
) ENGINE = Kafka('{broker}', 'rembo.public.sales_territory', '{group}', 'JSONAsString');
""".format(broker=KAFKA_BROKER, group=KAFKA_GROUP)

CREATE_SALES_TERRITORY_MV = """
CREATE MATERIALIZED VIEW IF NOT EXISTS sales_territory_mv TO sales_territory
AS
SELECT
    JSONExtract(all, 'payload', 'sales_territory_id', 'Int32') AS sales_territory_id,
    JSONExtract(all, 'payload', 'sales_territory_country', 'Nullable(String)') AS sales_territory_country,
    JSONExtract(all, 'payload', 'sales_territory_region', 'Nullable(String)') AS sales_territory_region,
    JSONExtract(all, 'payload', 'sales_territory_city', 'Nullable(String)') AS sales_territory_city,
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
FROM sales_territory_queue;
"""

DROP_SALES_TERRITORY_MV = 'DROP TABLE IF EXISTS sales_territory_mv;'
DROP_SALES_TERRITORY_QUEUE = 'DROP TABLE IF EXISTS sales_territory_queue;'

SALES_TERRITORY_QUERIES = [DROP_SALES_TERRITORY_MV, DROP_SALES_TERRITORY_QUEUE, CREATE_SALES_TERRITORY_QUEUE_TABLE, CREATE_SALES_TERRITORY_MV]
