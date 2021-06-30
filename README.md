# Benchmark Postgre Partitioning

```
go test -run=. -bench=. -benchtime=5s -count 5 -benchmem -cpuprofile=cpu.out -memprofile=mem.out -trace=trace.out ./... | tee bench.txt
```

## Run

```go run main.go```

## cURL

### Post partitioned

```
curl -X POST 'localhost:8080/partition' \
-H 'Content-Type: application/json' \
--data-raw '{
    "n": 10,
    "user_id": "1234",
    "date": "2021-06-30T00:00:00Z"
}'
```

### Get Partitioned

```
curl -X GET 'localhost:8080/partition/f1b84754-db10-4ce6-89ff-83fb412481e6/1234/2021-06-30'
```

### Post without partition

```
curl -X POST 'localhost:8080/no-partition' \
-H 'Content-Type: application/json' \
--data-raw '{
    "n": 10,
    "user_id": "1234",
    "date": "2021-06-30T00:00:00Z"
}'
```

### Get without Partition

```
curl -X GET 'localhost:8080/no-partition/f1b84754-db10-4ce6-89ff-83fb412481e6/1234/2021-06-30'
```


## Seed data

### Transaction without partition

Create table and add 1.000.000 data transaction per day with unique id per day.

Add index after seeding the data.

```sql
CREATE TABLE IF NOT EXISTS "transactions" (
    "id" VARCHAR NOT NULL,
    "user_id" VARCHAR NOT NULL,
    "info" VARCHAR NOT NULL,
    "status" VARCHAR NOT NULL,
    "trx_date" DATE NOT NULL,
    "trx_timestamp" TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY ("id", "trx_date")
);

CREATE OR REPLACE FUNCTION seed_transactions(timestamp, timestamp)
  RETURNS void AS
$func$
DECLARE
d timestamp := $1;
BEGIN

LOOP
    INSERT INTO transactions(id, user_id, info, status, trx_date, trx_timestamp)
    SELECT LPAD(i::text, 7, '0'), (i%1000)::text, i::text, 'SUCCESS', d, d + i * INTERVAL '1 millisecond'
    FROM generate_series(1, 1000000) AS t(i);
    d = d + '1 day';
    EXIT WHEN d > $2;
END LOOP;

END
$func$ LANGUAGE plpgsql;

SELECT seed_transactions('2020-01-01 00:00:00'::timestamp, '2020-12-31 00:00:00'::timestamp);

DROP FUNCTION IF EXISTS seed_transactions(timestamp, timestamp);

CREATE INDEX IF NOT EXISTS idx_transactions_id_trx_date_timestamp ON transactions (id, user_id, trx_date, trx_timestamp DESC);
```

### Transaction Partitioned

Create parent table and child table and add 1.000.000 data transaction per day with unique id per day.

Add index after seeding the data.

```sql
CREATE TABLE IF NOT EXISTS "transactions_partitioned" (
    "id" VARCHAR NOT NULL,
    "user_id" VARCHAR NOT NULL,
    "info" VARCHAR NOT NULL,
    "status" VARCHAR NOT NULL,
    "trx_date" DATE NOT NULL,
    "trx_timestamp" TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY ("id", "trx_date")
) PARTITION BY LIST(trx_date);

CREATE OR REPLACE FUNCTION seed_transactions_partitioned(timestamp, timestamp)
  RETURNS void AS
$func$
DECLARE
    d timestamp := $1;
   	query text;
BEGIN

LOOP
		query = 'CREATE TABLE IF NOT EXISTS transaction_partition_y' || date_part('year', d)::text || '_m' || date_part('month', d)::text || '_d' ||  date_part('day', d)::text || ' PARTITION OF transactions_partitioned FOR VALUES IN (''' || (d::DATE) || ''');';
		EXECUTE query;

    INSERT INTO transactions_partitioned (id, user_id, info, status, trx_date, trx_timestamp)
		SELECT LPAD(i::text, 7, '0'), (i%1000)::text, i::text, 'SUCCESS', d, d + i * INTERVAL '1 millisecond'
		FROM generate_series(1, 1000000) AS t(i);
    d = d + '1 day';
    EXIT WHEN d > $2;
END LOOP;

END
$func$ LANGUAGE plpgsql;

SELECT seed_transactions_partitioned('2020-01-01 00:00:00'::timestamp, '2020-12-31 00:00:00'::timestamp);

DROP FUNCTION IF EXISTS seed_transactions_partitioned(timestamp, timestamp);

CREATE INDEX IF NOT EXISTS idx_transactions_partitioned_id_trx_date_timestamp ON transactions_partitioned (id, user_id, trx_date, trx_timestamp DESC);
```