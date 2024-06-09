from pysparkmeos.UDF.udf import *
from pysparkmeos.UDT.MeosDatatype import *
from pysparkmeos.UDTF import *

from pymeos import *


def register_udfs_under_spark_sql(spark):
    spark.udf.register("ever_touches", ever_touches)
    spark.udf.register("tstzspan", tstzspan)
    spark.udf.register("geometry_from_hexwkb", geometry_from_hexwkb)
    spark.udf.register("trip_from_hexwkb", trip_from_hexwkb)
    spark.udf.register("tpoint_at", tpoint_at)
    spark.udf.register("temporally_contains", temporally_contains)
    spark.udf.register("ever_intersects", ever_intersects)
    spark.udf.register("min_distance", min_distance)
    spark.udf.register("nearest_approach_distance", nearest_approach_distance)
    spark.udf.register("tgeompointinst", tgeompointinst)
    spark.udf.register("tgeompointseq", tgeompointseq)
    spark.udf.register("at_period", at_period)
    spark.udf.register("at_geom", at_geom)
    spark.udf.register("tboolinst_from_base_time", tboolinst_from_base_time)
    spark.udf.register("contains_stbox_stbox", contains_stbox_stbox)
    spark.udf.register("temporally_overlaps", temporally_overlaps)
    spark.udf.register("datetime_to_tinstant", datetime_to_tinstant)
    

def register_udtfs_under_spark_sql(spark):
    spark.udtf.register("TripsUDTF", TripsUDTF)
    spark.udtf.register("RegionsUDTF", RegionsUDTF)
    spark.udtf.register("PointsUDTF", PointsUDTF)
    spark.udtf.register("PeriodsUDTF", PeriodsUDTF)
    spark.udtf.register("InstantsUDTF", InstantsUDTF)


def bounds_calculate_map(partition_rows, colname='PointSeq', utc='UTC'):
    pymeos_initialize(utc)
    aggregator = TemporalPointExtentAggregator.start_aggregation()
    #print(iter(partition_rows))
    for row in partition_rows:
        if type(row) == Row:
            seq = TGeomPointSeq(row[colname].__str__())
            aggregator.add(seq)
    try:
        boundbox = STBoxWrap(aggregator.aggregation().__str__())
        return [(boundbox)]#[(boundbox)]
    except:
        return [(None)]


def bounds_calculate_reduce(bounds1, bounds2, utc='utc'):
    pymeos_initialize(utc)
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


def new_bounds_from_axis(bounds, axis, p, to):
    b = p.bounding_box()
    newbounds = None
    if axis == 'x':
        if to == 'left':
            newbounds = STBox(
                xmax=b.xmax(), xmin=bounds.xmin(),
                ymax=bounds.ymax(), ymin=bounds.ymin(),
                zmax=bounds.zmax(), zmin=bounds.zmin(),
                tmax=bounds.tmax(), tmin=bounds.tmin()
            )
        else:
            newbounds = STBox(
                xmax=bounds.xmax(), xmin=b.xmin(),
                ymax=bounds.ymax(), ymin=bounds.ymin(),
                zmax=bounds.zmax(), zmin=bounds.zmin(),
                tmax=bounds.tmax(), tmin=bounds.tmin()
            )
    if axis == 'y':
        if to == 'left':
            newbounds = STBox(
                xmax=bounds.xmax(), xmin=bounds.xmin(),
                ymax=b.ymax(), ymin=bounds.ymin(),
                zmax=bounds.zmax(), zmin=bounds.zmin(),
                tmax=bounds.tmax(), tmin=bounds.tmin()
            )
        else:
            newbounds = STBox(
                xmax=bounds.xmax(), xmin=bounds.xmin(),
                ymax=bounds.ymax(), ymin=b.ymin(),
                zmax=bounds.zmax(), zmin=bounds.zmin(),
                tmax=bounds.tmax(), tmin=bounds.tmin()
            )
    if axis == 'z':
        if to == 'left':
            newbounds = STBox(
                xmax=bounds.xmax(), xmin=bounds.xmin(),
                ymax=bounds.ymax(), ymin=bounds.ymin(),
                zmax=b.zmax(), zmin=bounds.zmin(),
                tmax=bounds.tmax(), tmin=bounds.tmin()
            )
        else:
            newbounds = STBox(
                xmax=bounds.xmax(), xmin=bounds.xmin(),
                ymax=bounds.ymax(), ymin=bounds.ymin(),
                zmax=bounds.zmax(), zmin=b.zmin(),
                tmax=bounds.tmax(), tmin=bounds.tmin()
            )
    if axis == 't':
        if to == 'left':
            newbounds = STBox(
                xmax=bounds.xmax(), xmin=bounds.xmin(),
                ymax=bounds.ymax(), ymin=bounds.ymin(),
                zmax=bounds.zmax(), zmin=bounds.zmin(),
                tmax=b.tmax(), tmin=bounds.tmin()
            )
        else:
            newbounds = STBox(
                xmax=bounds.xmax(), xmin=bounds.xmin(),
                ymax=bounds.ymax(), ymin=bounds.ymin(),
                zmax=bounds.zmax(), zmin=bounds.zmin(),
                tmax=bounds.tmax(), tmin=b.tmin()
            )
    return newbounds
