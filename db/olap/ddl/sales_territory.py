import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_GROUP = os.getenv("KAFKA_GROUP")

CREATE_SALES_TERRITORY_TABLE = """
CREATE TABLE IF NOT EXISTS sales_territory
(
    sales_territory_id Int32,
    sales_territory_country Nullable(String),
    sales_territory_region Nullable(String),
    sales_territory_city Nullable(String),
    created_at Nullable(String),
    record_version Int64,
    arrival_date_time_tz DateTime DEFAULT now(),
    source_updated_at_ms Nullable(Int64),
    db_action Nullable(String),
    is_deleted Nullable(UInt8)
) ENGINE = MergeTree()
ORDER BY (sales_territory_id);
"""

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

CREATE_VIEW_SALES_TERRITORY = """
CREATE VIEW view_sales_territory_last_rec AS
WITH LatestSalesTerritory AS (
    SELECT
        s1.sales_territory_id,
        MAX(record_version) AS max_record_version
    FROM sales_territory s1
    WHERE s1.is_deleted = 0
    GROUP BY s1.sales_territory_id
)
SELECT
    s.sales_territory_id,
    s.sales_territory_country, s.sales_territory_region, s.sales_territory_city,
    s.created_at, s.record_version, s.arrival_date_time_tz,
    s.source_updated_at_ms, s.is_deleted
FROM sales_territory s
INNER JOIN LatestSalesTerritory lst ON s.sales_territory_id = lst.sales_territory_id AND s.record_version = lst.max_record_version
WHERE s.is_deleted = 0;
"""

DROP_VIEW_SALES_TERRITORY = 'DROP VIEW IF EXISTS view_sales_territory_last_rec;'
DROP_SALES_TERRITORY = 'DROP TABLE IF EXISTS sales_territory;'
DROP_SALES_TERRITORY_MV = 'DROP TABLE IF EXISTS sales_territory_mv;'
DROP_SALES_TERRITORY_QUEUE = 'DROP TABLE IF EXISTS sales_territory_queue;'

SALES_TERRITORY_QUERIES = [DROP_VIEW_SALES_TERRITORY, DROP_SALES_TERRITORY, DROP_SALES_TERRITORY_MV, DROP_SALES_TERRITORY_QUEUE, CREATE_SALES_TERRITORY_TABLE, CREATE_SALES_TERRITORY_QUEUE_TABLE, CREATE_SALES_TERRITORY_MV, CREATE_VIEW_SALES_TERRITORY]
