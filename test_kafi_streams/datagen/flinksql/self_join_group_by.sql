CREATE TABLE shoe_orders (
    order_id INT,
    product_id STRING,
    customer_id STRING,
    ts TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'shoe_orders',
    'scan.startup.mode' = 'earliest-offset',
    'properties.bootstrap.servers' = 'localhost:9092',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

CREATE VIEW order_view AS
SELECT DISTINCT
    order_id order_id,
    product_id as product_id,
    customer_id as customer_id
FROM
    shoe_orders;

--

CREATE VIEW self_join_group_by_view AS
SELECT
    o1.product_id AS product_id_1,
    o2.product_id AS product_id_2,
    COUNT(DISTINCT p1.customer_id) AS count
FROM
    order_view o1
JOIN
    order_view o2 ON o1.customer_id = o2.customer_id
WHERE
    o1.product_id < o2.product_id
GROUP BY
    o1.product_id, o2.product_id

--

CREATE TABLE upsert_kafka_sink (
    product_id_1 STRING,
    product_id_2 STRING,
    count INT,
    PRIMARY KEY (product_id_1, product_id_2) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'flink_self_join_group_by',
    'properties.bootstrap.servers' = 'localhost:9092',
    'key.format' = 'json',
    'value.format' = 'json'
);

INSERT INTO
    upsert_kafka_sink
SELECT
    product_id_1,
    product_id_2,
    count
FROM
    self_join_group_by_view;
