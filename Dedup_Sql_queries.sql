-- Use below SQL queries to periodically deduplicate data in BigQuery tables.

CREATE OR REPLACE TABLE Transactions AS
SELECT DISTINCT * 
FROM raw_transactions;

--OR use below incremental steps to drop the necessary partitions and re-insert the deduped data into the original table

-- Step 1: Insert distinct records from the original table based on the max timestamp available

CREATE OR REPLACE TABLE STAGE_TRANSACTIONS AS
SELECT DISTINCT *
FROM raw_transactions
WHERE 
    event_timestamp > (
        SELECT MAX(event_timestamp)
        FROM raw_transactions 
    );

-- Step 2: Drop the partition after deduplication
DELETE FROM raw_transactions
WHERE event_timestamp = > (
        SELECT MAX(event_timestamp)
        FROM raw_transactions 
    );

-- Step 3: Insert deduplicated data into the main table
INSERT INTO raw_transactions
SELECT DISTINCT *
FROM STAGE_TRANSACTIONS;


--OR Use below SQL query to Merge new data without duplicates the table

MERGE INTO raw_transactions AS target
USING (
    SELECT * 
    FROM STAGE_TRANSACTIONS
) AS source
ON target.transaction_id = source.transaction_id
   AND target.event_timestamp <= source.event_timestamp
WHEN MATCHED THEN
    UPDATE SET
        target.product = source.product,
        target.price = source.price,
        target.location = source.location,
        target.store = source.store,
        target.zipcode = source.zipcode,
        target.city = source.city,
        target.promotion = source.promotion,
        target.event_timestamp = source.event_timestamp
WHEN NOT MATCHED THEN
    INSERT (transaction_id, product, price, location, store, zipcode, city, promotion, event_timestamp)
    VALUES (source.transaction_id, source.product, source.price, source.location, source.store, source.zipcode, source.city, source.promotion, source.event_timestamp);

--OR to get the real-time data without duplicates, use following materialized view and a finest solution to retrieve dedup records quickly

CREATE MATERIALIZED VIEW raw_transactions_mv AS
SELECT 
    transaction_id,
    product,
    price,
    location,
    store,
    zipcode,
    city,
    promotion,
    event_timestamp
FROM (
    SELECT 
        transaction_id,
        product,
        price,
        location,
        store,
        zipcode,
        city,
        promotion,
        event_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id 
            ORDER BY event_timestamp DESC
        ) AS row_num
    FROM raw_transactions
)
WHERE row_num = 1;


