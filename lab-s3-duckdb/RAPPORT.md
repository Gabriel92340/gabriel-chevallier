# Lab report: S3 object storage and DuckDB

Gabriel Chevallier

## S3 lab: Kubernetes Job

**Why are the credentials stored in a Secret and not in the ConfigMap?**

A ConfigMap is made for non-sensitive configuration and its content is stored in plain text. Anyone who can read it can see everything. A Secret is designed for sensitive data: we can restrict who can read it (RBAC) and it can be encrypted at rest. Using a Secret also means that we can share or version the ConfigMap without leaking the credentials.

**The credentials of Onyxia are temporary. What happens if the Job is executed again tomorrow? How would a production platform provide credentials to a Job?**

The Job would fail with an `ExpiredToken` error, because the token stored in the Secret is no longer valid. In production, we would not copy temporary credentials by hand. The Job would run with a Kubernetes service account that receives short-lived credentials automatically, for example through a role with minimal permissions, or from a secrets manager such as Vault that rotates them.

**How would you turn this Job into a daily ingestion?**

I would use a CronJob with a schedule such as `0 2 * * *`, so Kubernetes creates a new Job every day. The data should also come from the source system and not from a ConfigMap, which is limited to 1 MiB.

## DuckDB lab

### S3 secret

**The persistent secret is stored in a file of your home directory. What are the risks, and why are they limited on Onyxia?**

Anyone, or any process, that can read the file can use the credentials to access the bucket. The file could also be copied by mistake, for example in a backup or in a Git repository. On Onyxia the risk is limited because the credentials are temporary and expire with the session, they only give access to our own bucket, and our home directory is private to our service.

**The secret is visible to any process running as your user. How would you restrict the access to S3 for a Kubernetes Job?**

I would give the Job its own credentials with minimal permissions, for example read-only access to the prefix it needs. They would be stored in a Secret that only this Job mounts, and not shared with the whole user. Even better, the Job would get short-lived credentials through its service account.

### Reading the CSV files

**The dataset contains 50 users and 2829 orders with unique identifiers. Why does `approx_unique` return 40 and 3167?**

`approx_unique` does not count exactly. It gives an estimate with a probabilistic algorithm (HyperLogLog), which is much faster and uses very little memory on large tables. The price is a small error. To get the exact values, we use `count(DISTINCT ...)`, which returns 50 and 2829.

**The types are inferred from a sample of the file. Which issue may occur with a large file whose first rows are not representative?**

DuckDB guesses the type of each column from the first rows only. If they are not representative, for example a column with integers at the beginning and decimals or text later, the guessed type is wrong. The read then fails in the middle of the file, or some values are badly converted. To avoid this, we can read the whole file for the detection (`sample_size = -1`), declare the types manually with `columns`, or read everything as text with `all_varchar` and cast afterwards.

### Parquet

**Why is the `uuid` column barely compressed?**

The identifiers are random and unique, so there is no repetition to exploit. Dictionary encoding and compression cannot help. The column goes from 113,189 to 102,191 bytes only. On the other hand, `user_uuid` and `product` have few distinct values and are compressed very well.

**Why is the compressed size of some columns larger than their uncompressed size?**

For small or already dense columns, compression saves nothing and adds its own overhead, such as headers. The `date` column is 22,667 bytes compressed for 22,661 uncompressed, and `product` is 1,271 for 1,266.

### Hive partitioning

**Why is partitioning by `uuid` a bad idea? Refer to the limitations of object storage presented in the previous module.**

Each `uuid` is unique, so we would get 2829 directories with one row each. An object store has no real folders, only keys with prefixes. Listing is paginated and every object needs its own HTTP request, so thousands of tiny files make the queries very slow.

**Which partition column would you choose for a dataset of orders growing every day?**

The date, by day or by month. Queries usually filter on a period, and the new data goes into a new partition without rewriting the old ones. The number of files also stays reasonable.

### CSV vs. Parquet at scale

The generated dataset has 252,416 orders. The CSV file is 29.6 MB and the Parquet file is 11.5 MB.

| Query | Data received | GET requests | Time |
|---|---|---|---|
| CSV, sum per product | 28.2 MiB | 1 | 1.13 s |
| Parquet, sum per product | 203.5 KiB | 4 | 0.43 s |
| Parquet, filter `date >= '2100-01-01'` | 16.0 KiB | 1 | 0.29 s |

**How many row groups does the file contain, and how many are skipped by the filter `date >= '2100-01-01'`?**

The file contains 3 row groups (123,573, 124,276 and 4,567 rows). The filter skips all three, because the biggest date, in the last group, is in 2048. That is why DuckDB only reads 16 KiB, the footer with the statistics, and no data.

**The orders are sorted by date. What would happen to the efficiency of the filter if they were shuffled?**

Each row group would contain dates from the whole period, so its minimum and maximum would cover everything. The statistics could not exclude any group, and DuckDB would have to read the `date` column of all of them. The filter is only efficient when the data is sorted, or grouped, on the filtered column.

**Compare the execution time of the CSV and Parquet queries. Which part of the difference is due to the network, which part to the parsing of the CSV file?**

The Parquet query is about 2.6 times faster (0.43 s against 1.13 s). For the network, the CSV query downloads the whole file (28.2 MiB) to read only 2 columns out of 5, while the Parquet query reads the footer and the two useful columns (203.5 KiB), about 140 times less. For the parsing, the CSV is text: DuckDB must split every line and convert each value, for all the columns. Parquet already stores typed binary columns. With one measure we cannot separate the two parts exactly. Also, the S3 server is in the same cluster, so the network is fast here. On a slower network, the gap would be bigger.