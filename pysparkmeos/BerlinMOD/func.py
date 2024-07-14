from time import time, sleep
import pandas as pd

from pysparkmeos.utils.utils import bounds_calculate_map, \
    bounds_calculate_reduce


def query_exec(query, desc, spark, execute=True, explain=False, explainmode=''):
    plan = None
    print(desc)
    if explain:
        plan = spark.sql(f"EXPLAIN {explainmode} {query}").collect()[0].plan
    result = spark.sql(query)
    start = time()
    if execute:
        result.show()
    end = time()
    print("Query execution time: ", end-start, " seconds.")
    sleep(5)
    return result, (start, end, end-start), plan


def retrieve_exec_stats(queries, starts, ends, durations, plans):
    return pd.DataFrame({"queries": queries, "start": starts, "end": ends, "duration": durations, "plan": plans})


def run_all_queries(queries, descs, spark, execute=True, explain=True, explainmode='', printplan=False):
    """ Utility function to run all queries through subsequent experiments """
    qdfs = []
    starts = []
    ends = []
    durations = []
    plans = []
    for querytext, querydesc in zip(queries, descs):
        qdf, qstats, plan = query_exec(querytext, querydesc, spark, execute, explain, explainmode)
        qdfs.append(qdf)
        starts.append(qstats[0])
        ends.append(qstats[1])
        durations.append(qstats[2])
        plans.append(plan)
        if printplan:
            print(plan)
    exec_stats = retrieve_exec_stats(queries, starts, ends, durations, plans)
    return qdfs, exec_stats


def load_table(
        spark,
        path,
        tablename,
        partition_key=None,
        transformation_query=None,
        partition_query=None,
        partitioner_class=None,
        partitioner_args={},
        num_buckets=1,
        **kwargs
):
    spark.sql(f"""DROP TABLE IF EXISTS {tablename}""")
    spark.sql(f"""DROP TABLE IF EXISTS {tablename}Raw""")
    spark.sql(f"""DROP TABLE IF EXISTS {tablename}NoCache""")
    spark.sql(f"""DROP VIEW IF EXISTS {tablename}RawNoCache""")

    print("Reading raw csv ", path)
    rawdf = spark.read.csv(path, **kwargs)
    print("Creating temp view of raw table")
    rawdf.createOrReplaceTempView(f"{tablename}RawNoCache")
    rawdf.limit(1).show()

    print("Schema and statistics of raw table")
    rawdf.printSchema()
    rawdf.describe().show()
    print(
        f"Creating final table {tablename} based on {tablename}RawNoCache, partitioned by {partition_key}.")
    if transformation_query:
        rawdf = spark.sql(transformation_query)
        # rawdf.show()
        rawdf.createOrReplaceTempView(f"{tablename}RawNoCache")
        rawdf.createOrReplaceTempView(f"{tablename}Raw")
        #spark.sql(f"CACHE TABLE {tablename}Raw SELECT * FROM {tablename}RawNoCache")
        #spark.sql(f"SELECT * FROM {tablename}Raw LIMIT 5").show()
        # spark.catalog.dropTempView(f"{tablename}RawNoCache")
    else:
        spark.sql(
            f"CACHE TABLE {tablename}Raw SELECT * FROM {tablename}RawNoCache")
        # spark.catalog.dropTempView(f"{tablename}RawNoCache")
    partitioner = None
    if partition_query:
        bounds = rawdf.rdd.mapPartitions(
            lambda b: bounds_calculate_map(b, colname='trip')).reduce(
            bounds_calculate_reduce)
        print("Bounds: ", bounds)
        if "movingObjects" in partitioner_args:
            sample = spark.sql(f"SELECT trip FROM {tablename}Raw")
            partitioner_args['movingObjects'] = [row.trip for row in
                                                 sample.collect()]
        if "movingobjects" in partitioner_args:
            sample = spark.sql(f"SELECT trip FROM {tablename}Raw")
            partitioner_args['movingobjects'] = [row.trip for row in
                                                 sample.collect()]
        if "moving_objects" in partitioner_args:
            sample = spark.sql(f"SELECT trip FROM {tablename}Raw")
            partitioner_args['moving_objects'] = [row.trip for row in
                                                  sample.collect()]
        parttime = time()
        partitioner = partitioner_class(bounds=bounds, **partitioner_args)
        partend = time()
        print("Time to create partitioning grid: ", partend - parttime,
              " seconds.")
        grid = partitioner.as_spark_table()
        grid.cache()
        grid.show()
        grid.createOrReplaceTempView("grid")
        print("Creating partitioned table... ")
        partitionedTable = spark.sql(partition_query)
        partitionedTable.createOrReplaceTempView(f"{tablename}Raw")

    start = time()

    if partition_key:
        df = spark.sql(f"""
        CREATE TABLE {tablename}NoCache
        USING parquet
        CLUSTERED BY ({partition_key}) INTO {num_buckets} BUCKETS
        AS SELECT * FROM {tablename}Raw
        """)
    else:
        df = spark.sql(f"""
        CREATE TABLE {tablename}NoCache
        USING parquet
        AS SELECT * FROM {tablename}Raw
        """)

    end = time()

    print(f"Final table created in {end - start} seconds")

    spark.sql(f"CACHE TABLE {tablename} SELECT * FROM {tablename}NoCache")

    df = spark.table(f"{tablename}")

    print(f"Final table {tablename} schema:")
    df.printSchema()

    # Drop the temporary view
    # spark.catalog.dropTempView(f"{tablename}Raw")
    return df, (start, end, end - start)


def load_all_tables(spark, configs, sample_percent=0.01):
    tables = {}
    stats = {}
    for tablename, config in configs.items():
        table, stat = load_table(**config)
        tables[tablename] = table
        stats[tablename] = stat

    querylicences1 = spark.table("vehicles").sample(fraction=sample_percent, seed=3)
    querylicences1.cache().createOrReplaceTempView("querylicences1")

    querylicences2 = spark.table("vehicles").sample(fraction=sample_percent, seed=7)
    querylicences2.cache().createOrReplaceTempView("querylicences2")

    return tables, stats

# periods, statsperiods = load_table(spark, "periods.csv", 'points', transformation_query=transperiod, inferSchema=True, header=True)
# periods.show()
