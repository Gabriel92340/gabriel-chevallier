# Lab report: Bronze to silver with Spark DataFrames



**The credentials are temporary. What happens to a running Spark job if the service is restarted mid-lab, and why?**

The job stops, because Spark runs inside the service and a restart kills the process. The new service also gets new credentials. Even without a restart, a long job fails with a 403 error when the token expires, because the keys and the token are copied into the Hadoop configuration when the session starts and are never renewed.

**Why does a plain `SimpleAWSCredentialsProvider` fail here, while it worked in setups without a session token?**

It only uses the access key and the secret key, and it ignores the session token. Temporary STS credentials are only accepted when the token is sent with every signed request, so the S3 server rejects the calls. `TemporaryAWSCredentialsProvider` adds the token.

## Silver layer

**Explain the output of the code that was just run.**

The query groups the users by state and gives the number of users and the number of missing cities. Most states have no missing city. Only the military codes have one: `AE` has 3 users with 3 missing cities, and `AP` has 2 users with 2 missing cities. Their addresses, like `DPO AP 09617`, have no city. Only the 10 first states are shown, so the totals are lower than 50.

**Compare the DataFrame and SQL cells. Which one would you choose for a one-off exploration, and which for a model reused by several colleagues?**

For a quick exploration I would use SQL: it is short and everybody can read it. For a model shared with colleagues I prefer the DataFrame API, because we can split the code into functions, test it and version it like Python code, and a wrong column name is found earlier. Both give the same Catalyst plan, so the performance is the same. In our test, the two versions of the deduplication returned the same result.

**`regexp_extract()` returned an empty string instead of `NULL` on no match. Where else in this notebook could that same behavior silently produce a wrong result if left unhandled?**

On `state` and `zip_code`, which are extracted in the same way and were not corrected. If an address did not match the pattern, these columns would contain an empty string. A `groupBy("state")` would create an empty group, a `not_null` check would pass by mistake, and a join on the zip code would not detect the problem. In our data all the addresses match, so it does not happen.

## Parquet

**List the objects written under `silver/users.parquet/`. How many part-files are there, and why?**

There is one part-file (`part-00000-...snappy.parquet`) and a `_SUCCESS` file. Spark writes one file per partition of the DataFrame. Both CSV files are read with `multiLine=True`, which cannot be split into blocks, so we get a single partition. For `orders`, the shuffle partitions of the window are very small, so Spark also ends with one file. On a bigger dataset, we would get one file per partition.

**`coalesce(1)` before `.write` would produce a single file. What do you lose by doing that on a larger dataset?**

We lose the parallelism of the write: only one task writes everything, so it is slow and its memory can be saturated. We also get one big file that readers cannot read in parallel, which makes the next queries slower.

## Iceberg

**Compare `silver/orders.parquet/` and `warehouse/silver/orders/`. Which files hold the data, and which ones describe the table?**

In both cases a Parquet file holds the data. In the plain Parquet directory there is nothing else than a `_SUCCESS` file, and Spark finds the schema in the footer of the files. In the Iceberg table, the `metadata/` folder describes the table: `v1.metadata.json` (schema and snapshots), the Avro manifests (which data files belong to the table, with their statistics) and `version-hint.text` (the current version).

**A second job overwrites the Parquet directory while a query is reading it. What does the reader see? What changes with an Iceberg table?**

With a Parquet directory there is no isolation. The reader can see a mix of old and new files, or a file which disappears during the read, so the result can be wrong or the query can fail. With Iceberg, a query reads one precise snapshot. The writer creates new files and then commits a new snapshot in one step. The reader sees the old version or the new one, never something in between, and the old files stay available until they are deleted.

**With this catalog, the pointer to the current version of a table is a file in the bucket. What can go wrong if two jobs commit at the same time, and what would a catalog service add?**

Both jobs read version 1 and write their own version 2, and one commit overwrites the other. We lose a commit without any error. The `hadoop` catalog needs an atomic rename, which object stores do not guarantee. A catalog service (REST, Hive Metastore, Glue, Nessie) updates the pointer atomically and checks the expected version. If two jobs commit together, the second one is refused and must read the table again and retry.

## Comparison with dbt

I compared the two silver layers with DuckDB. Both have 2829 orders with the same total quantity (8615) and 50 users.

**What differs between the two schemas? Spark has no `UUID` type: what did you do about it, and what is the consequence for the consumers of the silver layer?**

In dbt, `order_id` and `user_id` are `uuid` and `ordered_at` is `timestamp with time zone`. In Spark, they are `varchar` and `timestamp`. I used a `cast` to string for the identifiers, so they take 36 bytes instead of 16 and the type does not check the format. A consumer must cast them back to UUID if needed, and joins are a bit heavier. For `ordered_at`, Spark does not keep the time zone in the schema, so the consumer must know that the value is in UTC, or the hours can be shifted.

**Compare the output layout: one object per model with dbt, one directory of part-files with Spark. Which one is easier for a consumer to read? Which one scales?**

One file is simpler to read: one path and one read. But it does not scale, because one task writes it and it must be rewritten completely at each update. A directory of part-files needs a wildcard (`*.parquet`), but every task writes in parallel and readers can read in parallel, so it scales.

**The dbt lab validates the silver layer with the `unique`, `not_null`, `accepted_values` and `relationships` tests. What did this lab use instead, and what would you need to write to get the same guarantees?**

Only `assert` on row counts and column names, and some checks by eye with `show()`. To get the same guarantees we would need functions: a `groupBy().count()` for uniqueness, a `filter` on nulls for `not_null`, an `isin` for the accepted values and an anti join for the foreign keys. They would have to raise an error, run automatically and produce a report, which `dbt test` already does. A library such as Great Expectations or Deequ can play this role with Spark.

**`ref()` and `source()` give dbt the dependency graph of the models. What plays this role in the notebook?**

Nothing formal: only the order of the cells and the names of the Python variables. `stg_orders` depends on `orders_typed`, which depends on `orders_bronze`, but nobody declares it. Spark knows the lineage of a DataFrame inside a session, but not between notebooks or between written tables. An orchestrator such as Airflow or Dagster, or dbt with a Spark adapter, would give this graph.

**Compare the time to build the silver layer with `dbt build` and with the cells of this lab. Why are these numbers not directly comparable?**

They do not measure the same thing. In my dbt lab, `dbt build` took about 4 to 6 seconds: it creates 2 views, which store no data, and runs the tests. The Spark cells include the start of the JVM, the download of Iceberg, the connection to S3, the read with `inferSchema`, and the real writing of Parquet and Iceberg files. On such small data, the fixed costs of starting dominate, not the computation.

**For each of these cases, would you choose dbt with DuckDB or Spark: a daily batch of 10 GB, a daily batch of 10 TB, a team of analysts who only know SQL?**

- 10 GB per day: dbt with DuckDB. It fits on one machine, with no cluster to manage.
- 10 TB per day: Spark. This volume is too big for one machine and the work must be shared on a cluster.
- Analysts who only know SQL: dbt, because it only needs SQL and has tests and documentation built in.

## Exercises

**1. `birthdate_is_valid`**

I joined `stg_users` with the first order date of each user, and the column is `False` when the birthdate is later than this date. I wrote it with the DataFrame API and with Spark SQL, and both give the same result: 45 users are valid and 5 are not. In the dbt lab, the fix also replaced the invalid birthdate by `NULL`. Here I kept the original value and only flagged it, so each consumer can decide what to do.

**2. UDF and `pandas_udf`**

I extracted the zip code in three ways, on 5000 rows (50 users repeated 100 times). The three methods gave exactly the same result (0 difference). Times in seconds for three runs:

| Method | Run 1 | Run 2 | Run 3 |
|---|---|---|---|
| `regexp_extract` | 0.326 | 0.136 | 0.144 |
| Python UDF | 0.295 | 0.268 | 0.212 |
| `pandas_udf` | 0.226 | 1.116 | 0.185 |

`regexp_extract` is the fastest (0.136 s against 0.212 s for the Python UDF), because it runs inside the JVM and Catalyst can optimize it. A Python UDF sends each row from the JVM to a Python process. The `pandas_udf` sends batches of rows in the Arrow format, which reduces this cost, but the work is still done in Python. The 1.116 s of the `pandas_udf` is an outlier. With only 5000 rows, the differences are small and the fixed costs dominate, so I would expect a bigger gap on a larger dataset.

**3. Cache**

I read the two CSV files from S3 three times, with and without `.cache()`. Times in seconds:

| Read | Without cache | With cache |
|---|---|---|
| 1 | 0.505 | 0.535 |
| 2 | 0.446 | 0.288 |
| 3 | 0.468 | 0.274 |

The first read with cache is as slow as without it, because it fills the cache. The next reads are about 40% faster, because Spark reads the data from memory and not from S3. The `storageLevel` of both DataFrames is `Disk Memory Deserialized 1x Replicated`. The gain is small because the files are tiny.

**4. Append and time travel with Iceberg**

I appended one order to `lab.silver.orders`. By mistake I ran the append twice, so the table had 3 snapshots (`overwrite`, `append`, `append`) and 2831 rows, with the same order twice. This shows that Iceberg does not check uniqueness, so we must deduplicate before writing. With `VERSION AS OF`, I read the table before the first append (2829 rows) and after it (2830 rows), and I used `rollback_to_snapshot` to come back to 2829 rows.

With the Parquet directory, the append also gives 2830 rows, but there is no way to read the previous state: the directory only contains the data files, with no history or version. To restore the previous state, I had to write the whole directory again from the DataFrame. So Iceberg keeps the history of the table (time travel, audit, rollback), while a Parquet directory only keeps its current state.