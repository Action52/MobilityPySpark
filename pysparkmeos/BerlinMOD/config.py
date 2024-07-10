def load_config(
        spark,
        paths,
        trans_queries,
        part_queries,
        partition_keys,
        partitioner_class,
        partitioner_args,
        num_buckets = 8,
        inferSchema = True,
        header = True
):
    configs_exp = {
        'trips':    {
            'spark': spark,
            'tablename': 'trips',
            'partitioner_class': partitioner_class,
            'partitioner_args': partitioner_args,
            'num_buckets': num_buckets,
            'inferSchema': inferSchema,
            'header': header},
        'instants': {
            'spark': spark,
            'tablename': 'instants',
            'num_buckets': num_buckets,
            'inferSchema': inferSchema,
            'header': header},
        'licences': {
            'spark': spark,
            'tablename': 'licences',
            'inferSchema': inferSchema,
            'num_buckets': num_buckets,
            'header': header},
        'periods':  {
            'spark': spark,
            'tablename': 'periods',
            'num_buckets': num_buckets,
            'inferSchema': inferSchema,
            'header': header},
        'points':   {
            'spark': spark,
            'tablename': 'points',
            'num_buckets': num_buckets,
            'inferSchema': inferSchema,
            'header': header},
        'regions':  {
            'spark': spark,
            'tablename': 'regions',
            'num_buckets': num_buckets,
            'inferSchema': inferSchema,
            'header': header},
        'vehicles': {
            'spark': spark,
            'tablename': 'vehicles',
            'inferSchema': inferSchema,
            'header': header}
    }
    if paths:
        for pathkey, path in paths.items():
            configs_exp[pathkey]['path'] = path
    if trans_queries:
        for transquery_key, query in trans_queries.items():
            configs_exp[transquery_key]['transformation_query'] = query
    if part_queries:
        for partquery_key, query in part_queries.items():
            configs_exp[partquery_key]['partition_query'] = query
    if partition_keys:
        for partkey_key, querykey in partition_keys.items():
            configs_exp[partquery_key]['partition_key'] = querykey
    return configs_exp
