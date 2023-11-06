CREATE_EMPLOYEE_TABLE = """
CREATE TABLE IF NOT EXISTS employee
(
    employee_id Int32,
    employee_name Nullable(String),
    sales_territory_id Nullable(Int32),
    created_at Nullable(DateTime),
    record_version Nullable(Int64),
    arrival_date_time_tz DateTime DEFAULT now(),
    source_updated_at_ms Nullable(Int64),
    db_action Nullable(String),
    is_deleted Nullable(UInt8)
) ENGINE = MergeTree()
ORDER BY (employee_id);
"""

CREATE_VIEW_EMPLOYEE="""
CREATE VIEW view_employees_last_rec AS
WITH LatestEmployee AS (
    SELECT
        e.employee_id,
        MAX(record_version) AS max_record_version
    FROM employee e
    WHERE e.is_deleted = 0
    GROUP BY e.employee_id
)
SELECT
    e.employee_id,
    e.employee_name,
    e.sales_territory_id,
    e.created_at,
    e.arrival_date_time_tz,
    e.source_updated_at_ms,
    e.is_deleted,
    e.record_version
FROM employee e
INNER JOIN LatestEmployee le ON e.employee_id = le.employee_id AND e.record_version = le.max_record_version
WHERE e.is_deleted = 0;
"""


DROP_VIEW_EMPLOYEE = 'DROP VIEW IF EXISTS view_employees_last_rec;'
DROP_EMPLOYEE = 'DROP TABLE IF EXISTS employee;'


EMPLOYEE_QUERIES = [DROP_VIEW_EMPLOYEE, DROP_EMPLOYEE, CREATE_EMPLOYEE_TABLE, CREATE_VIEW_EMPLOYEE]
