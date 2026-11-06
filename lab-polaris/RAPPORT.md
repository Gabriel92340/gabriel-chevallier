# Lab report: Govern Iceberg tables with Polaris


## SparkSession with the Polaris catalog

**Which component answered each of the two `SHOW NAMESPACES` commands? How can you tell?**

`lab` is answered by the Iceberg Hadoop catalog. It runs inside the Spark process and reads the `warehouse/` directory of the bucket directly, with no server. `polaris` is answered by the Polaris server, which Spark calls with HTTP requests on the REST API (`polaris:8181`). We can tell from the configuration of the session: `lab` has the type `hadoop` and a warehouse path in S3, while `polaris` has the type `rest`, a URI and a catalog name.

**Where is the credential used: in the Spark session, in the bucket, or in a request to Polaris?**

In a request to Polaris. The Spark session holds it in its configuration, but it is used to get an access token from Polaris, and this token is sent with every REST call. The bucket does not know it. To read and write the files, Spark uses the S3A keys. So the Polaris credential says who the user is, and the S3 keys say who can touch the files.

## Register the Silver table in Polaris

I registered `silver.orders` with the metadata file `v7.metadata.json`, the last version of my table. The count through `polaris.silver.orders` was 2833 orders.

**What did `register_table` write, in Polaris and in the bucket? Compare the number of objects under `warehouse/silver/orders/` before and after.**

In Polaris, it created an entry for the table: its name, its namespace and the path of the current metadata file. In the bucket, it wrote nothing. After the registration, the listing of `warehouse/silver/orders/` only contained the files of the Spark lab (data files, manifests, `v1` to `v7.metadata.json` and `version-hint.text`), so the number of objects did not change. Polaris points to the existing files, and the data exist only once.

**Why is it dangerous to keep writing to this table through the `lab` catalog once it is registered in `polaris`?**

Each catalog keeps its own pointer to the current version. For `lab` it is the `version-hint.text` file, and for Polaris it is its record in the database. A commit through `lab` creates a new metadata file and moves the pointer of the Hadoop catalog, but Polaris stays on version 7, so its readers never see the new data. If both catalogs write, the histories split into two different versions, and one of them loses its work. We must choose one single catalog for the writes, here Polaris.

## Create tables as `dev`

**Where did Polaris store the files for `silver.users`, and why?**

In `warehouse/silver/users/`, with `data/` for the Parquet files and `metadata/` for the metadata. Polaris builds the location of a new table from the base location of the catalog (`s3://<bucket>/warehouse/`), and adds the namespace and the name of the table. It also refuses to write outside the allowed locations of the catalog. Here, this path was the directory of the `lab.silver.users` table of the previous lab, so the two tables shared the same directory. We could see side by side `v1` to `v4.metadata.json` with `version-hint.text` (catalog `lab`) and `00000-<uuid>.metadata.json` (Polaris). Each catalog used its own files, so nothing was broken.

**Both tables are in your bucket. What is the difference between "registered" and "created through Polaris" for the data files?**

For the registered table (`orders`), Polaris did not touch any file. The data and the metadata were written by the Hadoop catalog (`vN.metadata.json`), and Polaris only keeps a pointer to `v7`. For the table created through Polaris (`users`), Polaris chose the location, and the Spark session wrote the data and metadata files there, with the naming of the REST catalog (`00000-<uuid>.metadata.json`).

## Schema change as `dev`

I added a `region` column to `polaris.silver.users`, because the table already had a `country` column from the Iceberg lab.

**Which metadata file records the change? Is it the same file that `register_table` pointed to?**

A new file, `00001-<uuid>.metadata.json`, written next to `00000-<uuid>.metadata.json` of the creation of the table. It is not the file used by `register_table`, which was `v7.metadata.json` of the `orders` table. This change is about the `users` table, created through Polaris, and Polaris moves the pointer of this table to the new file.

**Which privilege does Polaris check for this statement?**

A write privilege on the table, for the operation of changing the schema. The analyst was refused for the operation `ADD_TABLE_SCHEMA`, which shows that this operation needs more than the read privileges. `dev` has it through `catalog_contributor`, which contains `CATALOG_MANAGE_METADATA` and `CATALOG_MANAGE_CONTENT`. `catalog_reader` does not have any write privilege.

## Read as the analyst

The reads worked: `SHOW TABLES` listed `orders` and `users`, and the aggregation by product gave the 6 products (between 459 and 491 orders each). Both writes failed. The message of the `INSERT` was:

`Forbidden: Principal 'analyst' with activated PrincipalRoles '[data_analyst]' and activated grants via '[data_analyst, catalog_reader]' is not authorized for op ADD_TABLE_SNAPSHOT`

The `ALTER TABLE` gave the same message with `op ADD_TABLE_SCHEMA`.

**Which privilege is missing for the `INSERT`? Which privileges does `catalog_reader` provide?**

The right to write data to the table, which is `TABLE_WRITE_DATA` in Polaris. The operation `ADD_TABLE_SNAPSHOT` needs it. `catalog_reader` only provides three privileges: `TABLE_READ_DATA`, `TABLE_READ_PROPERTIES` and `CATALOG_READ_PROPERTIES`. They allow to read the data, the properties of the tables and the properties of the catalog. `dev` can write because `catalog_contributor` contains `CATALOG_MANAGE_CONTENT`.

**Check the snapshots of `silver.orders` as `dev`. Did the failed `INSERT` create one?**

No. As `dev`, the table still had 5 snapshots, and the last one was older than my attempt. The folder `data/` also still had 5 files, the same number as before. Polaris refused the commit, so Spark cancelled the write and removed its files.

## Exercises

**1. Remove the `country` column from `polaris.silver.users`**

After `ALTER TABLE ... DROP COLUMN country`, the table had 11 columns (without `country`, with `region`). The table still had only one snapshot, and the `data/` directory still had 4 files. A new file `00002-<uuid>.metadata.json` appeared. So it is a metadata operation: Iceberg identifies each column by an internal identifier, and dropping it only removes it from the current schema, in a new metadata file. The Parquet files are not rewritten. They still contain the column, but the readers ignore it. As no data is added or deleted, there is no new snapshot.

**2. A more restrictive catalog role named `silver_reader`**

With the REST API, I created the catalog role `silver_reader`, the principal role `silver_role` and the principal `silver_user`. I granted five privileges to `silver_reader`, only on the `silver` namespace: `NAMESPACE_LIST`, `NAMESPACE_READ_PROPERTIES`, `TABLE_LIST`, `TABLE_READ_DATA` and `TABLE_READ_PROPERTIES`. Then I linked `silver_user` to `silver_role`, and `silver_role` to `silver_reader`. As `dev`, I created a `gold` namespace with a table `orders_by_product` (the quantity per product). With `silver_user`, the read of `polaris.silver.orders` worked and gave 2833 orders. The read of `polaris.gold.orders_by_product` failed with `Forbidden ... is not authorized for op LOAD_TABLE`. So the privileges granted on a namespace do not give access to the other namespaces.

## Cleanup

As `dev`, I dropped the table `polaris.gold.orders_by_product`, then the namespace `polaris.gold`, and the table `polaris.silver.users`. A `DROP NAMESPACE` alone fails when the namespace still contains a table, so the table must be dropped first. I kept `silver.orders` registered, with 2833 orders, because the Trino lab needs it. The `lab.silver.users` table of the Hadoop catalog still had its 55 users, so dropping the Polaris table did not delete its data files.