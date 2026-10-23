# Lab report: Introduction to Spark's RDD API


## RDD creation and partitions

**What decides the number of partitions `textFile()` picks by default, for a file this size?**

Spark splits a file into blocks of up to 128 MB, with a minimum of `min(defaultParallelism, 2)` partitions. Our files are only a few KB, so we get this minimum: 2 partitions for both files.

**`repartition()` and `coalesce()` both change the partition count. Which one shuffles data, and which one doesn't?**

`repartition()` does a full shuffle. It redistributes all the rows, so it can increase or decrease the number of partitions. `coalesce()` only merges neighbouring partitions without a shuffle, so it can only decrease the number.

## Lazy evaluation and lineage

**Call `orders_data.count()` a second time. Does the Spark UI show a new stage? Why?**

Yes, a new job appears. Without a cache, Spark keeps nothing from the first computation. It reads the file again and applies the filter again. Each action replays the lineage from the beginning.

**At which point in this notebook would `.cache()` change that answer?**

If we call `orders_data.cache()` before the first action. The first `count()` computes the data and keeps it in memory, so the second `count()` reads it from memory without touching the file. In the Spark UI, the stages are skipped and the Storage tab shows the RDD.

## The multiline address

**Why does `spark.read.csv(multiLine=True)` not have this problem, while `sc.textFile()` does?**

The CSV reader understands quotes: a line break inside a quoted field belongs to the field and does not end the record. `textFile()` does not know the CSV format. It cuts the file at every line break, whatever the context. So the address of each user, which has two lines, is split in two elements.

**In general terms, what has to be true of a file format for a line-oriented reader like `textFile()` to be safe to use on it?**

Each record must fit on exactly one physical line. A line break must never appear inside the data, or it must be escaped. JSON Lines is a good example.

## Manual parsing of orders

**What would happen to this cell if one line in `orders.csv` had an extra comma inside `product`?**

The line would give 6 pieces instead of 5, and the unpacking in `parse_order` would fail with `ValueError: too many values to unpack`. Nothing fails when the file is read. The job only crashes at the first action which touches this line, with an error far from the real problem.

**Where does the DataFrame API perform the equivalent check, and when?**

When the file is read, using a schema. Each line is compared with the expected number of columns and types. The read mode decides what happens with a bad line: `PERMISSIVE` sets it to null, `DROPMALFORMED` drops it and `FAILFAST` stops the job immediately. So we choose the error policy instead of suffering it.

## Total quantity per user

**`reduceByKey` combines values on each partition before shuffling. What does `groupByKey` shuffle instead, and why is that more expensive?**

`groupByKey` shuffles all the raw values, one per order, before grouping them. `reduceByKey` only sends one partial total per key and per partition. So there is much less data written to disk, sent and read again, and no need to keep the list of all values of a key in memory. Both gave the same totals in our test.

**For which aggregations would `groupByKey` be unavoidable?**

For aggregations which need all the values of a key at the same time and cannot be combined two by two, such as an exact median or percentile, or sorting the values of a key. A sum, a count, a minimum or a maximum can be combined partially, so `reduceByKey` is enough.

## Manual deduplication

We got 2829 orders before and after the deduplication, so there is no duplicate in our file.

**What plays the role of `partitionBy()` of the window function here?**

The key `order_id` of `reduceByKey`. The shuffle sends the pairs according to the hash of the key, so all the versions of one order arrive on the same partition, where `most_recent` combines them two by two.

**Which version states the intent more directly: the window function, or the `reduceByKey` with a custom combiner?**

The window function. It says what we want, one row per order and the most recent one, and Spark chooses how to do it. With `reduceByKey` we write the how, and the combiner must be associative and commutative so the result does not depend on the order of the partitions. In exchange, it gives more control on what goes through the shuffle.

## Joining orders with users

**What would need to be true of how `orders_kv` and `users_kv` are partitioned for Spark to skip the shuffle?**

Both RDDs must have the same partitioner, the same function and the same number of partitions, for example with `partitionBy(8)` on both and kept in cache. The rows with the same key are then already on the same partition. Another way is to broadcast the small RDD, `users`, to all the executors.

**Replace `join()` with `leftOuterJoin()`. What changes in the result if a `user_id` in `orders` has no matching user?**

The order is kept, with `None` instead of the username: `(user_id, (order, None))`. With `join()`, this order disappears silently from the result.

**What is the risk of calling `.collect()` on `enriched` directly, on a dataset much larger than this one?**

`collect()` brings all the data to the driver. On a large dataset, the memory of the driver is full and the job fails with an `OutOfMemoryError`. It is better to use `take(n)`, `count()`, an aggregation, or to write the result to files.

## Exercises

**1. `aggregateByKey`**

With `aggregateByKey` starting from `(0, 0)`, I computed the number of orders and the total quantity per user in one pass. The totals match `reduceByKey`: 12 for the first user. The average quantity per order is about 3 (3.0 for the first user, 3.33 for the second one).

**2. Orders without a user**

I added one fake order with a `user_id` which does not exist, then I used `leftOuterJoin()`. Filtering the rows where the username is `None` gave exactly 1 order, the fake one. With a normal `join()` it would have disappeared without any message.

**3. DataFrame version**

The DataFrame version takes 2 lines (`groupBy("user_uuid").sum("quantity")` and the read), while the RDD version needs a parsing function of about 8 lines, plus the `map` and the `reduceByKey`. In the plan of the DataFrame, there are two `HashAggregate`: a `partial_sum` on each partition, then the final sum after an `Exchange hashpartitioning`, which is the shuffle. This partial sum is the same idea as the combine step of `reduceByKey`. The plan also shows `ReadSchema: struct<user_uuid:string,quantity:int>`, so Spark only reads the two useful columns and knows their types, and it uses 200 shuffle partitions by default instead of 2. The `toDebugString()` of the RDD is only a chain of `PythonRDD` and `ShuffledRDD`, with no information about columns or types.

**4. Effect of the cache**

Time of the second run, in seconds:

| Measure | With cache | Without cache |
|---|---|---|
| `orders_rdd.count()` | 0.099 | 0.138 |
| `user_quantity.count()` | 0.157 | 0.530 |

The aggregation is about 3.4 times faster with the cache. Without it, Spark reads the file again, splits each line and runs `parse_order` again for every action. The first run is slower (0.325 s for the first count) because of the warm-up of the JVM. The gain is small in absolute value because the file only has 2829 lines, and it would be much bigger on a large file. The debug string of the cached RDD shows `CachedPartitions: 2; MemorySize: 168.0 KiB`.