# Lab report: Bronze to Silver with dbt and DuckDB

Gabriel Chevallier

## Bronze layer

**The bronze CSV files are left untouched. What is the benefit when a bug is found in a transformation, six months later?**

The raw data is still exactly as we received it. We can fix the transformation and run it again on the whole history to rebuild the silver layer. If we had overwritten the files with a transformed version, the original information would be lost and we could not correct the mistake.

**The source reads the whole file on every query. Which other format and layout would you choose if the orders were ingested every hour for years?**

I would use Parquet files partitioned by date, for example by year, month and day. Each ingestion writes in the partition of the day and never touches the old files. A query on a period only reads the useful partitions, and Parquet only reads the columns we need. So we avoid reading and parsing a text file which grows every hour.

## Silver layer

**The silver models are views. What happens when a view is queried, and when is it preferable to a table?**

A view does not store any data, only the SQL query. Each time we query it, the query runs again. Here, DuckDB reads the CSV file on S3 and parses it again. A view is better when the transformation is light, when the source changes often and we always want the latest data, or when we want to save storage. A table is better when the transformation is heavy or when the data is queried a lot.

**UUID values are stored in 16 bytes. How many bytes does the VARCHAR representation use?**

At least 36 bytes: 32 hexadecimal characters and 4 hyphens, one byte for each character. It is more than twice the size of the UUID type, which is also faster to compare.

## Data tests

**Which strategy would you choose for a dashboard displaying the age of the customers? For a dashboard displaying the sales per region?**

For the age, a wrong birthdate gives a wrong result directly. In our data, the test found 288 orders dated before the birth of the customer. I would replace the invalid birthdates by NULL, keep them out of the age calculation, and show how many are missing. This is what we did in exercise 3. For the sales per region, the birthdate is not used. I would keep all the orders and only accept the problem with a warning, without rejecting any data.

**The addresses associate states with random zip codes, `KY 01352` for example. Which test would detect it, and which reference data would it require?**

A test which compares the zip code of each user with the valid range of its state. It needs a reference table with the minimum and maximum zip code of each state, for example from the USPS. In exercise 4, this test found 49 inconsistent users out of 50, which shows that the generator picks the zip codes at random.

## Exercise 4: invalid zip codes

**Where should the reference data come from?**

From an official source, such as the USPS files, or from a provider of address data. A table written by hand, like ours, is only an approximation.

**Should it be stored as a dbt seed?**

Yes, for a small and stable reference file. It is versioned with the code, reviewed in the pull requests and reloaded at each build. For large files, or data which changes often, I would prefer a table fed by a pipeline.

**Should the validation happen in the silver layer?**

Yes. The silver layer is where the data is cleaned and validated, so the test reports the problem before it reaches the gold layer and the dashboards.

**What happens when the reference data changes?**

The results of the tests change even if the bronze data did not move: valid rows can become invalid and the opposite. So the seed must be versioned like code, and the tests must run again after each update.

## Exercise 5: materialization experiment

**Where is the data stored?**

A view only stores the SQL query, so the data stays in the CSV file on S3. A table stores the transformed data in the DuckDB database file. In our test, the database file with tables was 1.1 MB.

**When is the transformation executed?**

For a view, at each query. For a table, only once, when we run `dbt build`.

**What happens when the underlying CSV changes?**

The view sees the new data at the next query. The table stays on the data of the last build until we run dbt again.

**Which approach requires more storage?**

The table, because it keeps a copy of the data. The view takes almost no space.

**Which approach avoids re-reading the source files for every query?**

The table. The CSV is read and parsed once at build time. In our test, a count on the table took 0.10 s, because everything was already in the database.