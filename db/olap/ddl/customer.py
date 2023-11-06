CREATE_CUSTOMER_TABLE = """
CREATE TABLE IF NOT EXISTS customer
(
    customer_id Int32,
    last_name Nullable(String),
    address_line1 Nullable(String),
    address_line2 Nullable(String),
    birth_date Nullable(Date),
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
    created_at Nullable(DateTime),
    record_version Nullable(Int64),
    arrival_date_time_tz DateTime DEFAULT now(),
    source_updated_at_ms Nullable(Int64),
    db_action Nullable(String),
    is_deleted Nullable(UInt8)
) ENGINE = MergeTree()
ORDER BY (customer_id);
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

CUSTOMER_QUERIES = [DROP_VIEW_SALES_CUSTOMER, DROP_CUSTOMER, CREATE_CUSTOMER_TABLE, CREATE_VIEW_CUSTOMER]
