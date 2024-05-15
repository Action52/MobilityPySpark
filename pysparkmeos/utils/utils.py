from pysparkmeos.UDF.udf import *

def register_udfs_under_spark_sql(spark):
    spark.udf.register("ever_touches", ever_touches)
    spark.udf.register("tstzspan", tstzspan)
    spark.udf.register("geometry_from_hexwkb", geometry_from_hexwkb)
    spark.udf.register("trip_from_hexwkb", trip_from_hexwkb)
    spark.udf.register("tpoint_at", tpoint_at)
    spark.udf.register("temporally_contains", temporally_contains)
    