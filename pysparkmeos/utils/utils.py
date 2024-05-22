from pysparkmeos.UDF.udf import *
from pysparkmeos.UDT.MeosDatatype import *
from pymeos import *


def register_udfs_under_spark_sql(spark):
    spark.udf.register("ever_touches", ever_touches)
    spark.udf.register("tstzspan", tstzspan)
    spark.udf.register("geometry_from_hexwkb", geometry_from_hexwkb)
    spark.udf.register("trip_from_hexwkb", trip_from_hexwkb)
    spark.udf.register("tpoint_at", tpoint_at)
    spark.udf.register("temporally_contains", temporally_contains)


def bounds_calculate_map(partition_rows):
    pymeos_initialize("UTC")
    aggregator = TemporalPointExtentAggregator.start_aggregation()
    #print(iter(partition_rows))
    for row in partition_rows:
        if type(row) == Row:
            #print(row)
            seq = TGeomPointSeq(row.trip.__str__())
            aggregator.add(seq)
    try:
        boundbox = STBoxWrap(aggregator.aggregation().__str__())
        return [(boundbox)]#[(boundbox)]
    except:
        return [(None)]


def bounds_calculate_reduce(bounds1, bounds2):
    pymeos_initialize("UTC")
    aggregation = None
    
    if bounds1 and bounds2:
        aggregation = bounds1 + bounds2
    elif not bounds1 and bounds2:
        aggregation = bounds2
    elif not bounds2:
        aggregation = bounds1

    if aggregation:
        boundbox = STBoxWrap(aggregation.__str__())
        return boundbox
    else:
        return None
    