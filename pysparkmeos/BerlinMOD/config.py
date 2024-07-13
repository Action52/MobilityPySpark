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
        'trips':    {},
        'instants': {},
        'licences': {},
        'periods':  {},
        'points':   {},
        'regions':  {},
        'vehicles': {},
        'municipalities': {}
    }
    if paths:
        for pathkey, path in paths.items():
            configs_exp[pathkey]['path'] = path
            configs_exp[pathkey]['spark'] = spark
            configs_exp[pathkey]['tablename'] = pathkey
            if num_buckets:
                configs_exp[pathkey]['num_buckets'] = num_buckets
            if inferSchema:
                configs_exp[pathkey]['inferSchema'] = inferSchema
            if header:
                configs_exp[pathkey]['header'] = header
    if trans_queries:
        for transquery_key, query in trans_queries.items():
            configs_exp[transquery_key]['transformation_query'] = query
    if part_queries:
        for partquery_key, query in part_queries.items():
            configs_exp[partquery_key]['partition_query'] = query
    if partition_keys:
        for partkey_key, querykey in partition_keys.items():
            configs_exp[partkey_key]['partition_key'] = querykey
    if partitioner_class:
        configs_exp['trips']['partitioner_class'] = partitioner_class
    if partitioner_args:
        configs_exp['trips']['partitioner_args'] = partitioner_args
    return configs_exp
