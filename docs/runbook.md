# Runbook

## 1. Prepare the source

Create an ADLS Gen2 container (`landing`) with a `data/` folder. Do not hard-code credentials anywhere.

```text
abfss://<CONTAINER>@<STORAGE_ACCOUNT>.dfs.core.windows.net/data/
```

## 2. Unity Catalog access

1. Create an Access Connector for Azure Databricks and grant it a data role on the storage account (for example *Storage Blob Data Contributor*).
2. Create a storage credential from the connector.
3. Create an external location on the container using that credential.

The pipeline identity must be allowed to read that external location.

## 3. Create the pipeline

```text
Name    : File-Streaming
Catalog : streamingdb
Schema  : flight_bookings
Compute : Serverless
Source  : transformations/ingest.py
```

## 4. First run (Day 1)

Place `flight_bookings_day1.json` in the landing folder and run the pipeline.

```sql
SELECT COUNT(*) FROM streamingdb.flight_bookings.flight_bookings_bronze;  -- 10
```

## 5. Schema evolution (Day 2)

Add `flight_bookings_day2.json` to the same folder and run the pipeline again. With `addNewColumns`, Auto Loader stops the stream when it first sees a new column, records the updated schema, and uses it on the next run or retry, so a second run or an automatic retry may be needed.

```sql
DESCRIBE streamingdb.flight_bookings.flight_bookings_bronze;               -- LoyaltyPoints present
SELECT COUNT(*)             FROM streamingdb.flight_bookings.flight_bookings_bronze;  -- 20
SELECT COUNT(LoyaltyPoints) FROM streamingdb.flight_bookings.flight_bookings_bronze;  -- 10
```

## 6. Clean up

Stop the pipeline when done. Avoid leaving a continuous pipeline running in a cost-controlled environment.

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| Schema inference fails on an empty folder | Auto Loader cannot infer from nothing. Put an initial file in the folder or supply a schema or schema hints. |
| Error when combining `.schema(...)` with `addNewColumns` | A fixed schema conflicts with evolution. Use inference plus evolution, or schema hints. |
| JSON array not parsed / inference fails | Add `.option("multiLine", "true")`. |
| Files re-processed or progress lost | Do not delete or alter the pipeline's managed state or checkpoint directories. |
| Access denied | Check the pipeline identity has read access on the Unity Catalog external location and the managed identity has a role on the storage account. |
