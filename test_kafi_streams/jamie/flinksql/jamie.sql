CREATE TABLE
    transactions (from_account INT, to_account INT, amount INT)
WITH
    (
        'connector' = 'kafka',
        'topic' = 'transactions',
        'properties.bootstrap.servers' = 'localhost:9092',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json',
        'json.ignore-parse-errors' = 'false'
    );


CREATE VIEW
    credits (account, credits) AS
SELECT
    to_account as account,
    sum(amount) as credits
FROM
    transactions
GROUP BY
    to_account;


CREATE VIEW
    debits (account, debits) AS
SELECT
    from_account as account,
    sum(amount) as debits
FROM
    transactions
GROUP BY
    from_account;


CREATE VIEW
    balance (account, balance) AS
SELECT
    credits.account,
    credits - debits as balance
FROM
    credits,
    debits
WHERE
    credits.account = debits.account;


CREATE VIEW
    total (total) AS
SELECT
    sum(balance)
FROM
    balance;


CREATE TABLE
    total_sink (total DOUBLE, PRIMARY KEY (total) NOT ENFORCED)
WITH
    (
        'connector' = 'upsert-kafka',
        'property-version' = 'universal',
        'properties.bootstrap.servers' = 'localhost:9092',
        'topic' = 'flink_total',
        'key.format' = 'json',
        'value.format' = 'json'
    );


INSERT INTO
    total_sink
SELECT
    *
FROM
    total;
