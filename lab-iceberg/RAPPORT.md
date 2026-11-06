# Lab report: Explore Iceberg tables with Spark

## Inspect the files

**Which files contain the actual rows?**

The Parquet files in the `data/` directory. For `orders`, there were three of them: the main one (84,055 bytes, 2829 rows) and two very small ones of 1,917 bytes, which come from two appends of the previous lab.

**Which files describe the table?**

The files of the `metadata/` directory:
- `v*.metadata.json`: the schema, the list of snapshots and the configuration of the table;
- the `snap-*.avro` files: the list of manifests of each snapshot;
- the `*-m0.avro` files: the manifests, which list the data files with their statistics;
- `version-hint.text`: the current version of the table.

**Why does an Iceberg table need more than the Parquet files?**

A Parquet file does not know which table it belongs to. A directory does not say which files are part of the current version, what the schema was before, or how to make a write visible in one step. The metadata add the list of files of each version (snapshot), the history of the schema and the atomic commit.

## Explore Iceberg metadata

**How many snapshots does the table currently have?**

3 at the beginning of the lab: `overwrite`, `append` and `append`. The table has three because I appended rows twice in the previous lab, then rolled back to the first snapshot. The rollback does not delete the other snapshots: it only moves the pointer of the current version.

**How many data files belong to the current table?**

One file, with 2829 rows (84,055 bytes). The two small files exist in the bucket, but only the old snapshots refer to them.

**What information does the `snapshots` table contain that the Parquet files do not?**

The identifier of each version, its parent, the commit time, the type of operation (`overwrite`, `append`), the location of the manifest list and a summary of the changes. It is the history of the table, while a Parquet file only describes its own data.

## Create a second snapshot

I appended 3 orders, so the table had 2832 rows.

**How many snapshots are there now?**

4. The new `append` has the first snapshot as its parent (`181480370546949761`), not the third. It is because I had rolled back to the first one before. The branch of the two old appends stays in the history.

**Did the original Parquet files disappear?**

No. An append writes a new file and never changes the old ones, because the files are immutable.

**Why does Iceberg create a new snapshot instead of simply replacing the table?**

A snapshot is a consistent view of the table at one moment. The readers see the complete old version while the write prepares the new one, and the commit is atomic. It also allows time travel, audit and rollback.

## Time travel

With `VERSION AS OF` on the first snapshot, I read 2829 rows, while the current table had 2832 rows.

**What does time travel read?**

One precise snapshot: the exact list of data files of the table at that moment, according to the metadata of this snapshot. Spark only reads these files, as if the table had not changed since.

**Why can Iceberg access an older version without making a complete copy of the table?**

The data files are immutable and shared between the versions. A new snapshot reuses the unchanged files and only adds the new ones. Here the current version and the first snapshot share the main file of 2829 rows, and only a small file of 3 rows was added. A version is just a list of pointers to files.

**What would happen if the old snapshot's metadata and data files were physically deleted?**

The version could no longer be read: `VERSION AS OF` would fail because the snapshot or the file is missing. We would lose the history, the time travel and the rollback to this version. It is what maintenance operations such as `expire_snapshots` do, to free space when the history is no longer needed.

## Schema evolution

I added a `source` column to `orders`. Then I inserted one row with the new schema: the table had 2833 rows and only 1 row with a value in `source`, the others had `NULL`.

**Did the existing Parquet files have to be rewritten?**

No. Adding a column only changes the metadata. When Spark reads an old file which does not have the column, Iceberg returns `NULL`. So the operation is instant, whatever the size of the table.

**Where is the new schema recorded?**

In a new `vN.metadata.json` file. It contains the list of schemas of the table, each with an identifier, and says which one is current. In my bucket, `orders` went from `v4` to `v7`: `v5` for the append of 3 orders, `v6` for the new column and `v7` for the insert. Each column has an internal identifier, so a column can be renamed or dropped without breaking the old files.

**Why is schema evolution a table-level operation rather than simply changing a Parquet file?**

A Parquet file is immutable and carries its own schema. To change the schema of many files, we would have to rewrite all of them. Iceberg manages the schema at the table level and matches it to the columns of each file with the identifiers, so all the files of different sizes and dates stay consistent.

## Compare Iceberg with Parquet

**Which one is simply a collection of Parquet files?**

The directory `silver/spark/orders.parquet/`. It only contains a `part-` file (139,556 bytes) and a `_SUCCESS` file. Spark finds the schema in the footer of the files and considers that everything in the directory is part of the dataset.

**Which one has table metadata and snapshots?**

`warehouse/silver/orders/`, with its `metadata/` directory: seven versions of the metadata file, manifests, manifest lists and the `version-hint.text` pointer.

**How would you safely replace a Parquet directory while another query is reading it?**

I would write the new data in a new directory, for example with a version or the date in its path. Then I would change the pointer to this directory (a table of the metastore or a configuration), and delete the old directory only when no query reads it any more. I would avoid renaming, because on S3 a rename is a copy followed by a delete, so it is not atomic.

**How does Iceberg make the table state visible atomically?**

A write first creates its new data and metadata files, which stay invisible. Then it commits in one step by replacing the pointer to the current version with the new `metadata.json` file. A reader sees the old version completely or the new one, never a mix.

**Why is a catalog needed once several engines need to share the same Iceberg table?**

So that all the engines (Spark, Trino, Flink, DuckDB) find the table by its name and read the same current version. The catalog keeps the pointer to the metadata and updates it atomically, checking the expected version. Without it, two engines which write at the same time can overwrite the work of the other, as we saw with the `hadoop` catalog.

## Exercises

**1. Create `lab.silver.users` as an Iceberg table**

The table already existed from the previous lab: 50 users and one snapshot (`overwrite`).

**2. Append a new batch of users and inspect the snapshots**

I appended 3 users with new identifiers, usernames and emails. The table had 2 snapshots: the `append` has the first one as its parent.

**3. Query the table before and after the append using time travel**

With `VERSION AS OF` on the first snapshot, I read 50 users, and 53 users in the current table.

**4. Add a `country` column and write new data using the evolved schema**

The table has 11 columns after `ALTER TABLE ... ADD COLUMN country STRING`. I appended 2 users with `country = 'FR'`. The table had 55 users and only 2 with a country: the 53 other users have `NULL`, because no existing file was rewritten.

**5. Compare the S3 layout of the users and orders tables**

Both tables have a `data/` directory and a `metadata/` directory, with the same kinds of files. The `users` table has 3 data files (7,436, 3,389 and 3,573 bytes), 3 manifests, 3 manifest lists (one per snapshot) and 4 metadata files (`v1` to `v4`): `v1` is the creation, `v2` the first append, `v3` the new column and `v4` the second append. The `orders` table has a longer history because of the extra appends, the rollback and the schema change: its metadata files go from `v1` to `v7`. In both cases, each operation creates new files and never changes the old ones.

**6. Explain the difference between Parquet, Iceberg, and an Iceberg catalog**

Parquet is a file format: it stores data in columns, with compression, types and statistics in the footer. It describes one file, not a table. Iceberg is a table format built on top of Parquet files. Its metadata (manifests, snapshots, schemas) say which files make the table at each version, which gives atomic writes, time travel and schema evolution. The Iceberg catalog is the service which finds a table by its name and keeps the pointer to its current metadata file. It updates this pointer atomically, and it allows several engines to share the same table safely.