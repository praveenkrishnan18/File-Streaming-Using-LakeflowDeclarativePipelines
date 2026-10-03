# Resources

The project runs as a Lakeflow Declarative Pipeline, not a hand-managed Structured Streaming job.

```text
Pipeline name : File-Streaming
Catalog       : streamingdb
Schema        : flight_bookings
Table         : flight_bookings_bronze
Compute       : Serverless
Source        : transformations/ingest.py
```

No ADF, Synapse, Silver/Gold layer or separate checkpoint resource is needed.

Do not commit workspace-specific pipeline JSON/YAML unless it was exported deliberately and contains no secrets or workspace IDs.
