import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_GROUP = os.getenv("KAFKA_GROUP")

CREATE_EMPLOYEE_QUEUE_TABLE = """
CREATE TABLE IF NOT EXISTS employee_queue
(
    all String
) ENGINE = Kafka('{broker}', 'rembo.public.employee', '{group}', 'JSONAsString');
""".format(broker=KAFKA_BROKER, group=KAFKA_GROUP)

CREATE_EMPLOYEE_MV = """
CREATE MATERIALIZED VIEW IF NOT EXISTS employee_mv TO employee
AS
SELECT
    JSONExtract(all, 'payload', 'employee_id', 'Int32') AS employee_id,
    JSONExtract(all, 'payload', 'employee_name', 'Nullable(String)') AS employee_name,
    JSONExtract(all, 'payload', 'sales_territory_id', 'Nullable(Int32)') AS sales_territory_id,
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
    toUInt8(JSONExtract(all, 'payload', '__deleted', 'String') = 'true') AS is_deleted,
    JSONExtract(all, 'payload', '__op', 'Nullable(String)') AS operation,
    JSONExtract(all, 'payload', '__lsn', 'Nullable(Int64)') AS record_version,
    JSONExtract(all, 'payload', '__source_ts_ms', 'Nullable(Int64)') AS source_updated_at_ms,
    now() AS arrival_date_time_tz
FROM employee_queue;
"""


DROP_EMPLOYEE_MV = 'DROP TABLE IF EXISTS employee_mv;'
DROP_EMPLOYEE_QUEUE = 'DROP TABLE IF EXISTS employee_queue;'


EMPLOYEE_QUERIES = [DROP_EMPLOYEE_MV, DROP_EMPLOYEE_QUEUE, CREATE_EMPLOYEE_QUEUE_TABLE, CREATE_EMPLOYEE_MV]
