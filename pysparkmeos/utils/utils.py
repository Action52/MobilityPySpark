from pysparkmeos.UDF.udf import *
from pysparkmeos.UDT.MeosDatatype import *
from pysparkmeos.UDTF import *

from pymeos import *


def register_udfs_under_spark_sql(spark: SparkSession):
    """
    Registers the udfs in a spark session
    :param spark: The spark session
    :return:
    """
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
    spark.udf.register("at_geom", at_geom)
    spark.udf.register("at_period", at_period)
    spark.udf.register("tboolinst_from_base_time", tboolinst_from_base_time)
    spark.udf.register("contains_stbox_stbox", contains_stbox_stbox)
    spark.udf.register("temporally_overlaps", temporally_overlaps)
    spark.udf.register("datetime_to_tinstant", datetime_to_tinstant)
    spark.udf.register("timestamps", timestamps)
    spark.udf.register("spatial_values", spatial_values)
    spark.udf.register("num_instants", num_instants)
    spark.udf.register("length", length)
    spark.udf.register("geometry_values", geometry_values)
    spark.udf.register("instants", instants)
    spark.udf.register("sequences", sequences)
    spark.udf.register("nearest_approach_instant", nearest_approach_instant)
    spark.udf.register("nearest_approach_distance", nearest_approach_distance)
    spark.udf.register("distance", distance)

def register_udtfs_under_spark_sql(spark: SparkSession):
    """
    Registers the BerlinMOD udtfs in the spark session.
    :param spark: The Spark Session.
    :return:
    """
    spark.udtf.register("TripsUDTF", TripsUDTF)
    spark.udtf.register("RegionsUDTF", RegionsUDTF)
    spark.udtf.register("PointsUDTF", PointsUDTF)
    spark.udtf.register("PeriodsUDTF", PeriodsUDTF)
    spark.udtf.register("InstantsUDTF", InstantsUDTF)


def bounds_calculate_map(
        partition_rows: Iterable[Row],
        colname: str = 'PointSeq',
        utc: str = 'UTC'
) -> List[Tuple[Union[STBox, None]]]:
    """
    Function to aggregate rows containing a series of rows containing a
    column of TGeomPointSeq. Returns the aggregated STBox.
    :param partition_rows:
    :param colname:
    :param utc:
    :return:
    """
    pymeos_initialize(utc)
    aggregator = TemporalPointExtentAggregator.start_aggregation()
    for row in partition_rows:
        if type(row) == Row:
            seq = TGeomPointSeq(row[colname].__str__())
            aggregator.add(seq)
    try:
        boundbox = STBoxWrap(aggregator.aggregation().__str__())
        return [(boundbox)]#[(boundbox)]
    except:
        return [(None)]


def bounds_calculate_reduce(
        bounds1: Union[STBox, None],
        bounds2: Union[STBox, None],
        utc='utc'
) -> Union[STBox, None]:
    """
    Reduces a series of STBoxes into the overall spatiotemporal space of all the
    boxes.
    :param bounds1:
    :param bounds2:
    :param utc:
    :return:
    """
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


def new_bounds_from_axis(bounds: STBox, axis: str, p: TPoint, to: str) -> STBox:
    """
    Helper function to calculate the new bounds STBox based on an align axis.
    Useful for KDTree.
    :param bounds:
    :param axis:
    :param p: The midpoint from KDTree.
    :param to: Indicates if bounds will be calculated to the 'left' or 'right'.
    :return: The new bounding STBox.
    """
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


def from_axis(p: TPoint, axis: str):
    """
    Helper function to get the sorting dimension of a TPoint.
    :param p: TPoint to analize
    :param axis: Character representing the axis. Example: 'x'.
    :return: Sorting value.
    """
    b = p
    if axis == "x":
        return b.x().start_value()
    if axis == "y":
        return b.y().start_value()
    if axis == "z":
        return b.z().start_value()
    if axis == "t":
        return b.timestamp()


def new_bounds(bounds: STBox, axis: str, val_min, val_max) -> STBox:
    newbounds = None
    if axis == 'x':
        newbounds = STBox(
                xmax=val_max, xmin=val_min,
                ymax=bounds.ymax(), ymin=bounds.ymin(),
                zmax=bounds.zmax(), zmin=bounds.zmin(),
                tmax=bounds.tmax(), tmin=bounds.tmin()
        )
    if axis == 'y':
        newbounds = STBox(
                xmax=bounds.xmax(), xmin=bounds.xmin(),
                ymax=val_max, ymin=val_min,
                zmax=bounds.zmax(), zmin=bounds.zmin(),
                tmax=bounds.tmax(), tmin=bounds.tmin()
        )
    if axis == 'z':
        newbounds = STBox(
                xmax=bounds.xmax(), xmin=bounds.xmin(),
                ymax=bounds.ymax(), ymin=bounds.ymin(),
                zmax=val_max, zmin=val_min,
                tmax=bounds.tmax(), tmin=bounds.tmin()
        )
    if axis == 't':
        newbounds = STBox(
                xmax=bounds.xmax(), xmin=bounds.xmin(),
                ymax=bounds.ymax(), ymin=bounds.ymin(),
                zmax=bounds.zmax(), zmin=bounds.zmin(),
                tmax=val_max, tmin=val_min
        )
    return newbounds
