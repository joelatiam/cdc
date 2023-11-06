CREATE_SALES_TERRITORY_TABLE = """
CREATE TABLE IF NOT EXISTS sales_territory
(
    sales_territory_id Int32,
    sales_territory_country Nullable(String),
    sales_territory_region Nullable(String),
    sales_territory_city Nullable(String),
    created_at Nullable(DateTime),
    record_version Int64,
    arrival_date_time_tz DateTime DEFAULT now(),
    source_updated_at_ms Nullable(Int64),
    db_action Nullable(String),
    is_deleted Nullable(UInt8)
) ENGINE = MergeTree()
ORDER BY (sales_territory_id);
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


SALES_TERRITORY_QUERIES = [DROP_VIEW_SALES_TERRITORY, DROP_SALES_TERRITORY, CREATE_SALES_TERRITORY_TABLE, CREATE_VIEW_SALES_TERRITORY]
