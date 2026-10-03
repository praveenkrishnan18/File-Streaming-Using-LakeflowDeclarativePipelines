# Architecture

```mermaid
flowchart TB
    subgraph Azure["Azure (rg-streaming)"]
        ADLS["ADLS Gen2<br/>container: landing"]
        AC["Access Connector<br/>(managed identity)"]
    end
    subgraph DBX["Azure Databricks"]
        UC["Unity Catalog<br/>credential + external location"]
        LDP["Lakeflow pipeline: File-Streaming"]
        AL["Auto Loader (cloudFiles)"]
        BR[("streamingdb.flight_bookings<br/>.flight_bookings_bronze")]
    end
    ADLS --> AL
    AC --> UC --> AL
    LDP --- AL --> BR
```

## File-arrival behaviour

| Event | What happens |
|---|---|
| Day 1 file lands | Auto Loader discovers it, infers the schema, 10 rows are written |
| Day 2 file lands | Auto Loader discovers it and finds `LoyaltyPoints`; the schema evolves (`addNewColumns`); 10 more rows are written |

Auto Loader tracks which files it has already processed in pipeline-managed state, so re-running the pipeline does not re-ingest earlier files.

## Target table

`streamingdb.flight_bookings.flight_bookings_bronze`, a Lakeflow streaming table backed by Delta Lake.

## Why Bronze only

The project isolates ingestion mechanics (incremental discovery, schema inference, schema evolution). Silver/Gold modelling is covered in a separate project, which keeps this one easy to explain.
