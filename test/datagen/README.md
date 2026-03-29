CREATE TABLE shoe_clickstream (
    product_id STRING,
    user_id STRING,
    view_time INT,
    page_url STRING,
    ip STRING,
    ts TIMESTAMP(3))
WITH (
    'connector' = 'kafka',
    'topic' = 'shoe_clickstream',
    'properties.bootstrap.servers' = 'localhost:9092',
--    'properties.group.id' = 'flink_shoe_clickstream_consumer',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'avro-confluent',
    'avro-confluent.url' = 'http://localhost:8081'
);

---

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
    country_code STRING)
WITH (
    'connector' = 'kafka',
    'topic' = 'shoe_customers',
    'scan.startup.mode' = 'earliest-offset',
    'properties.bootstrap.servers' = 'localhost:9092',
--    'properties.group.id' = 'flink_shoe_customers_consumer',
    'format' = 'avro-confluent',
    'avro-confluent.url' = 'http://localhost:8081'
);

--

create view shoe_enriched as select * from shoe_clickstream click join shoe_customers cust on click.user_id = cust.id; 

CREATE TABLE shoe_enriched_sink (
    view_time_ts TIMESTAMP(3),
    first_name STRING,
    last_name STRING,
    email STRING,
    page_url STRING,
    PRIMARY KEY (email) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'shoe_enriched_sink',
    'properties.bootstrap.servers' = 'localhost:9092',
    'key.format' = 'raw',
    'value.format' = 'avro-confluent',
    'value.avro-confluent.url' = 'http://localhost:8081'
);

CREATE TABLE shoe_enriched_append_sink (
    view_time_ts TIMESTAMP(3),
    first_name STRING,
    last_name STRING,
    email STRING,
    page_url STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'shoe_enriched_append',
    'properties.bootstrap.servers' = 'localhost:9092',
    'format' = 'avro-confluent',
    'avro-confluent.url' = 'http://localhost:8081'
);


INSERT INTO shoe_enriched_sink
SELECT 
    ts,
    first_name,
    last_name,
    email,
    page_url
FROM shoe_enriched;

INSERT INTO shoe_enriched_append_sink
SELECT 
    ts,
    first_name,
    last_name,
    email,
    page_url
FROM shoe_enriched;
