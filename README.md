# OpenHexa, Superset, and DuckDB. Quack.

This project provides a custom DuckDB connector for Superset with:
- **Per-user connection pooling** - Each user gets their own persistent DuckDB connection
- **Pre-installed UDFs** - Custom functions for OpenHexa dataset access
- **HTTP request caching** - API calls cached to minimize external requests

As a result, it is possible to query files in OpenHexa datasets from Superset, such as:
```sql
SELECT 
  variable, count(*), sum(value) as sum
FROM read_parquet(get_dataset_file_url('pathways-senegal-2019-dhs8/sen-2019dhs8-aggregated-metrics/latest/metrics.parquet')) 
WHERE stat_type = 'mean'
GROUP BY variable
```

Given the request to OpenHexa for a signed URL of the dataset, Superset keeps a per-user DuckDB connection between queries, and DuckDB cache is configured, we have:
- first pass: ~1,600ms
- second pass: `~120ms. When bypassing the Superset UI (authz, parsing, serialisation, http rendering), we get ~8ms SQLAlchemy/DuckDB response time.

## Getting started
1. Create a `.env` file with a valid `OPENHEXA_API_TOKEN`
2. Run the stack
```sh
docker compose up
```

3. Go to [http://localhost:8088](http://localhost:8088), login/password admin/admin

## Connection Type

The custom connector uses **`duckdb_oh://`** URIs (separate from standard `duckdb://`).

Example: `duckdb_oh:////app/superset_home/openhexa.duckdb`

This allows you to use both standard DuckDB connections and OpenHexa-enhanced connections side-by-side.

## API

### openhexa_dataset_files (table)

Returns all dataset files from OpenHexa. Optionally filter by workspace slug.

**Returns:** `workspace`, `dataset`, `version`, `filename`, `file_path`

**Note:** Download URLs are not included because generating them is time-consuming on the OpenHexa side. Use `get_dataset_file_url()` to fetch URLs selectively only for the files you need.

### get_dataset_file_url (scalar)

Returns the download URL for a dataset file by its ID.

**Arguments:** `file_path` (string)  
**Returns:** Download URL (string or NULL)

**Note:** Observed execution time: ~900ms

## Example queries

### List all the files available in the workspace

```sql
SELECT * FROM openhexa_dataset_files('pathways-meg-ind-dhs72020');
```

### Get the url of a file from its unique id

```sql
SELECT get_dataset_file_url('pathways-senegal-2019-dhs8/sen-2019dhs8-aggregated-metrics/latest/metrics.parquet');
```

### Querying a parquet file

```sql
-- File-level details
SELECT * FROM parquet_metadata(get_dataset_file_url('pathways-senegal-2019-dhs8/sen-2019dhs8-aggregated-metrics/latest/metrics.parquet'));

-- Column schema
DESCRIBE SELECT * FROM read_parquet(get_dataset_file_url('pathways-senegal-2019-dhs8/sen-2019dhs8-aggregated-metrics/latest/metrics.parquet'));

-- Count total rows
SELECT count(*) AS total_rows FROM read_parquet(get_dataset_file_url('pathways-senegal-2019-dhs8/sen-2019dhs8-aggregated-metrics/latest/metrics.parquet'));

-- See the first few rows
SELECT * FROM read_parquet(get_dataset_file_url('pathways-senegal-2019-dhs8/sen-2019dhs8-aggregated-metrics/latest/metrics.parquet')) LIMIT 10;
```

## Learnings
### Not nice to lookup to files using the GraphQL api

- for looking at the available datasets and files, it would be more efficient to connect direclty to the OpenHexa DB. But then we would bypass the OH authorisation layer.
- similarly, we could directly use the OpenHexa bucket and avoid using pre-signed, short-term urls. But it would also require other security safeguards.

### Do we already have a Superset instance that uses the OpenHexa credentials?
If so, we could filter the above queries transparently according to the user's permissions

### Not much needs to be done to improve DX for pipeline editors
Instead of implementing UDF for pipelines, we would only need to implement a `get_dataset_file_url` function in the Python SDK:
```python
from openhexa.sdk import workspace
import duckdb

file_path = workspace.get_dataset_file_url('dataset_slug/(version|latest)/file.parquet')
rel = duckdb.read_parquet(file_path)
duckdb.register('my_table', rel)
duckdb.sql("SELECT * FROM my_table LIMIT 10;").show()
```

Or maybe something simpler:

```python
from openhexa.sdk import workspace
import duckdb

workspace.load_table('my_table', 'dataset_slug/(version|latest)/file.parquet')
duckdb.sql("SELECT * FROM my_table LIMIT 10;").show()
```

### DuckDB vs Clickhouse

#### DuckDB
- pros
  - Simpler to implement e.g. run in-memory inside the superset instance
  - portable
  - RAM efficient
  - easy to learn
  - first-class Python/R support

- cons  
  - Difficult to distribute workloads outside of Superset
  - Requires additional Superset customisation that may become hard to maintain
  - Limited access control

#### Clickhouse
- pros
  - scalability including horizontal scale
  - performance, concurrency
  - features++ eg materialised views

- cons
  - Hard to set, need ops expertise
  - Harder SQL dialect 
  - Depending on our use case, may be overkill

Quack.