# OpenHexa, Superset, and DuckDB. Quack.


## Getting started
1. Create a `.env` file with a valid `OPENHEXA_API_TOKEN`
2. Run the stack
```sh
docker compose up
```

3. Go to [http://localhost:8088](http://localhost:8088), login/password admin/admin

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