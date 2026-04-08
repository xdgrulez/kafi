CREATE TABLE click_table (
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
    'format' = 'avro-confluent',
    'avro-confluent.url' = 'http://localhost:8081'
);

CREATE VIEW click_view AS
SELECT
    user_id,
    ip,
    product_id
from
    click_table;

--

CREATE TABLE customer_table (
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
    'format' = 'avro-confluent',
    'avro-confluent.url' = 'http://localhost:8081'
);

CREATE VIEW customer_view AS
SELECT
    id,
    first_name
FROM
    customer_table;

--

CREATE TABLE product_table (
    id STRING,
    brand STRING,
    name STRING,
    sale_price INT,
    rating DOUBLE
) WITH (
    'connector' = 'kafka',
    'topic' = 'shoes',
    'scan.startup.mode' = 'earliest-offset',
    'properties.bootstrap.servers' = 'localhost:9092',
    'format' = 'avro-confluent',
    'avro-confluent.url' = 'http://localhost:8081'
);

CREATE VIEW product_view AS
SELECT
    id,
    brand
FROM
    product_table;

--

CREATE TABLE order_table (
    order_id INT,
    product_id STRING,
    customer_id STRING,
    ts TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'shoe_orders',
    'scan.startup.mode' = 'earliest-offset',
    'properties.bootstrap.servers' = 'localhost:9092',
    'format' = 'avro-confluent',
    'avro-confluent.url' = 'http://localhost:8081'
);

CREATE VIEW order_view AS
SELECT
    order_id,
    product_id,
    customer_id
FROM
    order_table;

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

CREATE VIEW join_3_view AS
SELECT
    *
FROM
    join_2_view
    JOIN order_view ON join_2_view.product_id = order_view.product_id AND join_2_view.user_id = order_view.customer_id;

--upsert-kafka sink

CREATE TABLE upsert_kafka_sink (
    user_id STRING,
    ip STRING,
    product_id STRING,
    first_name STRING,
    brand STRING,
    order_id INT,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = '3_joins_upsert_kafka_sink',
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
    brand,
    order_id
FROM join_3_view;

--kafka-sink

-- CREATE TABLE kafka_sink (
--     user_id STRING,
--     ip STRING,
--     product_id STRING,
--     first_name STRING,
--     brand STRING,
--     order_id INT
-- ) WITH (
--     'connector' = 'kafka',
--     'topic' = '2_join_kafka_sink',
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
--     brand,
--     order_id
-- FROM join_3_view;
