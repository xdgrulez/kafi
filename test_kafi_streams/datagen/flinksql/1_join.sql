CREATE TABLE shoe_clickstream (
    product_id STRING,
    user_id STRING,
    view_time INT,
    page_url STRING,
    ip STRING,
    ts TIMESTAMP(3)
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
    user_id as user_id,
    ip as ip
FROM
    shoe_clickstream;

--

CREATE TABLE shoe_customers (
    id STRING,
    first_name STRING,
    last_name STRING,
    email STRING,
    phone STRING,
    street_address STRING,
    state STRING,
    zip_code STRING,
    country STRING,
    country_code STRING
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
    id as id,
    first_name as first_name
FROM
    shoe_customers;

--

CREATE VIEW join_1_view AS
SELECT
    *
FROM
    click_view
    JOIN customer_view ON click_view.user_id = customer_view.id;

--upsert-kafka sink

CREATE TABLE upsert_kafka_sink (
    user_id STRING,
    first_name STRING,
    ip STRING,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'flink_1_join',
    'properties.bootstrap.servers' = 'localhost:9092',
    'key.format' = 'json',
    'value.format' = 'json'
);

INSERT INTO
    upsert_kafka_sink
SELECT
    user_id,
    first_name,
    ip
FROM
    join_1_view;

--kafka-sink
-- CREATE TABLE kafka_sink (
--     user_id STRING,
--     first_name STRING,
--     ip STRING
-- ) WITH (
--     'connector' = 'kafka',
--     'topic' = '1_join_kafka_sink',
--     'properties.bootstrap.servers' = 'localhost:9092',
--     'format' = 'avro-confluent',
--     'avro-confluent.url' = 'http://localhost:8081'
-- );
-- INSERT INTO kafka_sink
-- SELECT 
--     user_id,
--     first_name,
--     ip
-- FROM join_1_view;
