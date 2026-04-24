CREATE TABLE click_table (
    payload ROW < product_id STRING,
    user_id STRING,
    view_time INT,
    page_url STRING,
    ip STRING,
    ts TIMESTAMP(3) >
) WITH (
    'connector' = 'kafka',
    'topic' = 'shoe_clickstream',
    'properties.bootstrap.servers' = 'localhost:9092',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

CREATE VIEW click_view AS
SELECT
    payload.user_id as user_id,
    payload.ip as ip,
    payload.product_id as product_id
from
    click_table;

--

CREATE TABLE customer_table (
    payload row < id STRING,
    first_name STRING,
    last_name STRING,
    email STRING,
    phone STRING,
    street_address STRING,
    state STRING,
    zip_code STRING,
    country STRING,
    country_code STRING >
) WITH (
    'connector' = 'kafka',
    'topic' = 'shoe_customers',
    'scan.startup.mode' = 'earliest-offset',
    'properties.bootstrap.servers' = 'localhost:9092',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

CREATE VIEW customer_view AS
SELECT
    payload.id as id,
    payload.first_name as first_name
FROM
    customer_table;

--

CREATE TABLE product_table (
    payload row < id STRING,
    brand STRING,
    name STRING,
    sale_price INT,
    rating DOUBLE >
) WITH (
    'connector' = 'kafka',
    'topic' = 'shoes',
    'scan.startup.mode' = 'earliest-offset',
    'properties.bootstrap.servers' = 'localhost:9092',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

CREATE VIEW product_view AS
SELECT
    payload.id as id,
    payload.brand as brand
FROM
    product_table;

--

CREATE VIEW join_1_view AS
SELECT
    *
FROM
    click_view
    JOIN customer_view ON click_view.user_id = customer_view.id;


CREATE VIEW join_2_view AS
SELECT
    *
FROM
    join_1_view
    JOIN product_view ON join_1_view.product_id = product_view.id;

--upsert-kafka sink

CREATE TABLE upsert_kafka_sink (
    user_id STRING,
    ip STRING,
    product_id STRING,
    first_name STRING,
    brand STRING
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = '2_joins_upsert_kafka_sink',
    'properties.bootstrap.servers' = 'localhost:9092',
    'key.format' = 'raw',
    'value.format' = 'avro-confluent',
    'value.avro-confluent.url' = 'http://localhost:8081'
);

INSERT INTO upsert_kafka_sink
SELECT 
    user_id,
    ip,
    product_id,
    first_name,
    brand
FROM join_2_view;

--kafka-sink

-- CREATE TABLE kafka_sink (
--     user_id STRING,
--     ip STRING,
--     product_id STRING,
--     first_name STRING,
--     brand STRING
-- ) WITH (
--     'connector' = 'kafka',
--     'topic' = '2_joins_kafka_sink',
--     'properties.bootstrap.servers' = 'localhost:9092',
--     'format' = 'avro-confluent',
--     'avro-confluent.url' = 'http://localhost:8081'
-- );

-- INSERT INTO kafka_sink
-- SELECT 
--     user_id,
--     ip,
--     product_id,
--     first_name,
--     brand
-- FROM join_2_view;
