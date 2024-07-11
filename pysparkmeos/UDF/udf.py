from pyspark.sql import SparkSession

from pysparkmeos.UDT.MeosDatatype import *
from pymeos import *
from pymeos import TPoint

import pyspark.sql.functions as F
from pyspark.sql.types import *

from shapely import wkb, Geometry

from typing import *
from datetime import datetime


@F.udf(returnType=TGeomPointInstUDT())
def create_point_udf(
        lat: float,
        lon: float,
        time: datetime,
        utc="UTC"
) -> TGeomPointInst:
    """
    Creates a TGeomPointInst based lat, lon and timestamp.

    :param lat: Latitude of the point.
    :param lon: Longitude of the point.
    :param time: Datetime of the point.
    :param utc: The UTC.
    TGeogPointInst.
    :return:
    """
    pymeos_initialize(utc)
    point_inst = TGeomPointInst(f"Point({lon} {lat})@{str(time)}")
    return point_inst


@F.udf(returnType=IntegerType())
def create_pointseq_instants_udf(
        lats: List[float],
        lons: List[float],
        times: List[datetime],
        utc: str = "UTC"
) -> int:
    """
    Returns the number of instants from a point sequence.
    TODO: Check if really used?
    :param lats:
    :param lons:
    :param times:
    :param utc:
    :return:
    """
    pymeos_initialize(utc)
    
    # Combine lat, lon, and time into a list of tuples and sort them by time
    combined = sorted(zip(lats, lons, times), key=lambda x: x[2])
        
    points_inst_list = [TGeogPointInst(f"Point({lon} {lat})@{time}") for lat, lon, time in combined]
    point_seq = TGeogPointSeq(instant_list=points_inst_list, lower_inc=True, upper_inc=True)
    instants = point_seq.num_instants()
    return instants


@F.udf(returnType=FloatType())
def get_point_x(point: TPoint, utc: str = "UTC") -> float:
    """
    Returns the starting x value of a TPoint.
    :param point: A TPoint
    :param utc: The UTC.
    :return: The start x value.
    """
    pymeos_initialize(utc)
    return point.x().start_value()


@F.udf(returnType=FloatType())
def get_point_y(point: TPoint, utc: str = "UTC") -> float:
    """
    Returns the starting y value of a TPoint.
    :param point: A TPoint
    :param utc: The UTC.
    :return: The start y value.
    """
    pymeos_initialize(utc)
    return point.y().start_value()


@F.udf(returnType=FloatType())
def get_point_z(point: TPoint, utc: str = "UTC") -> float:
    """
    Returns the starting x value of a TPoint.
    :param point: A TPoint
    :param utc: The UTC.
    :return: The start x value.
    """
    pymeos_initialize(utc)
    return point.z().start_value()


@F.udf(returnType=TimestampType())
def get_point_timestamp(point: TPoint, utc: str = "UTC") -> datetime:
    """
    Gets the start timestamp of a TPoint.
    :param point:
    :param utc:
    :return:
    """
    pymeos_initialize(utc)
    ts = point.start_timestamp()
    return ts


@F.udf(returnType=STBoxUDT())
def bounds_as_box(
    xmin: TFloat, xmax: TFloat,
    ymin: TFloat, ymax: TFloat,
    tmin: datetime = None, tmax: datetime = None,
    zmin: TFloat = None, zmax: TFloat = None,
    utc: str = "UTC"
) -> STBox:
    """
    Gets the bounds of a space given the x,y, and/or z/t values.
    """
    pymeos_initialize(utc)
    if tmin and zmin:
        return STBox(
            xmax=xmax, xmin=xmin,
            ymax=ymax, ymin=ymin,
            tmin=tmin, tmax=tmax,
            zmin=zmin, zmax=zmax
        )
    elif tmin and not zmin:
        return STBox(
            xmax=xmax, xmin=xmin,
            ymax=ymax, ymin=ymin,
            tmin=tmin, tmax=tmax
        )
    elif zmin and not tmin:
        return STBox(
            xmax=xmax, xmin=xmin,
            ymax=ymax, ymin=ymin,
            zmin=zmin, zmax=zmax
        )


@F.udf(returnType=BooleanType())
def ever_touches(trip: TPoint, other: Union[Geometry, STBox]) -> bool:
    """

    :param trip:
    :param other:
    :return:
    """
    pymeos_initialize()
    return trip.ever_touches(other)


@F.udf(returnType=TsTzSpanUDT())
def tstzspan(period: str, utc: str = "UTC") -> TsTzSpan:
    """
    Function to create a tstzspan from a string.
    :param period:
    :param utc:
    :return:
    """
    pymeos_initialize(utc)
    return TsTzSpan(period)


@F.udf(returnType=GeometryUDT())
def geometry_from_hexwkb(geom: str, utc: str = "UTC") -> Geometry:
    """
    Creates a Geometry from hexa well known binary string.
    :param geom:
    :param utc:
    :return:
    """
    pymeos_initialize(utc)
    return wkb.loads(geom, hex=True)


@F.udf(returnType=TGeomPointSeqUDT())
def trip_from_hexwkb(trip: str, utc: str = "UTC") -> TGeomPointSeq:
    """
    Creates a TGeomPointSeq from hexwkb.
    :param trip:
    :param utc:
    :return:
    """
    pymeos_initialize(utc)
    return TGeomPointSeq(trip)


@F.udf(returnType=GeometryUDT())
def tpoint_at(
        trip: TPoint,
        instant: TInstant,
        return_start_value: bool = True,
        utc: str = "UTC"
) -> Geometry:
    """
    Returns the geometry of a tpoint a particular timestamp.
    :param trip:
    :param instant:
    :param return_start_value:
    :param utc:
    :return:
    """
    pymeos_initialize(utc)
    tripat = trip.at(instant.timestamp())
    if tripat:
        if return_start_value:
            return tripat.start_value()
    return tripat


@F.udf(returnType=BooleanType())
def temporally_contains(
        trip: TPoint,
        other: Union[Time, Temporal, Box],
        utc: str = "UTC"
) -> bool:
    """
    Returns true if the tpoint temporally contains other.
    :param trip:
    :param other:
    :param utc:
    :return:
    """
    pymeos_initialize(utc)
    return trip.temporally_contains(other)


@F.udf(returnType=TGeomPointSeqUDT())
def tgeompointseq_from_instant_list(
        pointgroup: List[Union[str, TGeomPointInst]],
        utc: str = "UTC"
) -> TGeomPointSeq:
    """
    Creates a TGeomPointSeq based on a list of TGeomPointInst.
    :param pointgroup:
    :param utc:
    :return:
    """
    if not pointgroup:
        return None
    pymeos_initialize(utc)
    if len(pointgroup) == 1:
        pointgroup = f'[{pointgroup[0].__str__()}]'
        return TGeomPointSeq(pointgroup)
    pointgroup = sorted(pointgroup, key=lambda x: x.timestamps()[0])
    pointseq = TGeomPointSeq(instant_list=pointgroup)
    return pointseq


@F.udf(returnType=STBoxUDT())
def point_to_stbox(tpoint: TPoint, utc: str = "UTC") -> STBox:
    """
    Returns the STBox associated to a TPoint.
    :param tpoint:
    :param utc:
    :return:
    """
    pymeos_initialize(utc)
    return tpoint.bounding_box()


@F.udf(returnType=TBoolInstUDT())
def tboolinst_from_base_time(
        base: datetime,
        value: bool = True,
        utc: str = "UTC"
) -> TBoolInst:
    """
    Creates a TBoolInst object from a base datetime.
    :param base:
    :param value:
    :param utc:
    :return:
    """
    pymeos_initialize(utc)
    return TBoolInst.from_base_time(value=value, base=base)


@F.udf(returnType=BooleanType())
def ever_intersects(
        trip: TPoint,
        other: Union[Geometry, TPoint, STBox],
        utc: str = "UTC"
) -> bool:
    """
    Returns if a TPoint ever intersects other (Geometry, TPoint or STBox).
    :param trip:
    :param other:
    :param utc:
    :return:
    """
    pymeos_initialize(utc)
    return trip.ever_intersects(other)


@F.udf(returnType=FloatType())
def min_distance(
        trip: TPoint,
        other: Union[Geometry, TPoint, STBox],
        utc: str = "UTC"
) -> Union[float, None]:
    """
    Returns the minimum distance between a TPoint, and other.
    :param trip:
    :param other:
    :param utc:
    :return:
    """
    pymeos_initialize(utc)
    dist = trip.distance(other)
    if dist:
        min_dist = dist.min_value()
        return min_dist 
    else:
        return None


@F.udf(returnType=FloatType())
def nearest_approach_distance(
        trip: TPoint,
        other: Union[Geometry, STBox, TPoint],
        utc: str = "UTC"
) -> float:
    """
    Calculates the nearest approach distance between a TPoint and other.
    :param trip:
    :param other:
    :param utc:
    :return:
    """
    pymeos_initialize(utc)
    return trip.nearest_approach_distance(other)


@F.udf(returnType=TGeomPointInstUDT())
def tgeompointinst(
        point: Point,
        instant: TInstant,
        utc: str = "UTC"
) -> TGeomPointInst:
    """
    Returns a TGeomPointInst from a Point and a TInst.
    :param point:
    :param instant:
    :param utc:
    :return:
    """
    pymeos_initialize(utc)
    return TGeomPointInst(point=point, timestamp=instant.timestamp())


@F.udf(returnType=TGeomPointSeqUDT())
def tgeompointseq(
        geom,
        period: TsTzSpan,
        utc: str = "UTC"
) -> TGeomPointSeq:
    """
    Creates a TGeomPointSeq based on a Geometry and a TsTzSpan.
    :param geom:
    :param period:
    :param utc:
    :return:
    """
    pymeos_initialize(utc)
    return TGeomPointSeq.from_base_time(
        value=geom,
        base=period,
        interpolation=TInterpolation.DISCRETE
    )


@F.udf(returnType=TGeomPointSeqSetUDT())
def at_geom(trip: TPoint, geom: Geometry, utc: str = "UTC") -> TGeomPointSeqSet:
    pymeos_initialize(utc)
    if trip is None:
        return None
    at = trip.at(geom)
    if at:
        return at


@F.udf(returnType=TGeomPointSeqSetUDT())
def at_period(trip: TPoint, period: TsTzSpan, utc: str = "UTC") -> TGeomPointSeqSet:
    pymeos_initialize(utc)
    if trip is None:
        return None
    at = trip.at(period)
    if at:
        return at


@F.udf(returnType=BooleanType())
def contains_stbox_stbox(
        stbox: STBox,
        other: Union[Geometry, STBox, Temporal, Time],
        utc: str = "UTC"
) -> bool:
    """
    Returns True if the stbox contains other.
    :param stbox:
    :param other:
    :param utc:
    :return:
    """
    pymeos_initialize(utc)
    return stbox.contains(other)


@F.udf(returnType=BooleanType())
def temporally_overlaps(
        temporal: Temporal,
        other: Union[Time, Temporal, Box],
        utc: str = "UTC"
) -> bool:
    """
    Returns if a temporal temporally overlaps other.
    :param temporal:
    :param other:
    :param utc:
    :return:
    """
    pymeos_initialize(utc)
    return temporal.temporally_overlaps(other)


@F.udf(returnType=TBoolInstUDT())
def datetime_to_tinstant(
        instant: datetime,
        value: bool = True,
        utc: str = "UTC"
) -> TBoolInst:
    """
    Converts a datetime to a TBool.
    :param instant:
    :param value:
    :param utc:
    :return:
    """
    pymeos_initialize(utc)
    return TBoolInst.from_base_time(value=value, base=instant)


@F.udf(returnType=ArrayType(TimestampType()))
def timestamps(trajectory, utc="UTC"):
    pymeos_initialize(utc)
    return trajectory.timestamps()


@F.udf(returnType=TGeomPointSeqSetUDT())
def tgeompointseqset(trip: str, utc: str = "UTC") -> TGeomPointSeqSet:
    """
    Parses a string to TGeomPointSeqSet.
    :param trip:
    :param utc:
    :return:
    """
    pymeos_initialize(utc)
    return TGeomPointSeqSet(trip)


@F.udf(returnType=ArrayType(GeometryUDT()))
def geometry_values(tpoint: TPoint, utc="UTC"):
    pymeos_initialize(utc)
    return tpoint.values()


@F.udf(returnType=IntegerType())
def num_instants(traj: TPoint, utc="UTC"):
    pymeos_initialize(utc)
    return traj.num_instants()


@F.udf(returnType=FloatType())
def length(traj: TPoint, utc="UTC"):
    pymeos_initialize(utc)
    return traj.length()


@F.udf(returnType=TsTzSpanUDT())
def tstzspan_from_values(lower, upper, utc="UTC"):
    pymeos_initialize(utc)
    span = None
    if lower >= upper:
        return None
    try:
        span = TsTzSpan(lower=lower, upper=upper)
    except:
        pass
    return span


@F.udf(returnType=ArrayType(FloatType()))
def spatial_values(traj, dim, utc='UTC'):
    pymeos_initialize(utc)
    if dim == 'x':
        return traj.x().values()
    if dim == 'y':
        return traj.y().values()
    if dim == 'z':
        return traj.z().values()


def main():
    # Initialize PyMEOS
    pymeos_initialize("UTC")

    # Initialize a Spark session
    spark = SparkSession.builder \
        .appName("UDF Test Environment") \
        .master("local[1]") \
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "100") \
        .getOrCreate()

    spark.udf.register("tgeompointseqset", tgeompointseqset)

    trips = spark.read.csv("../mobilitydb-berlinmod-sf0.1/trips_sample_pymeos.csv", header=True) \
        .withColumn("trip", tgeompointseqset("trip"))

    trips.printSchema()


if __name__ == "__main__":
    main()
