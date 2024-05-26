from pysparkmeos.UDT.MeosDatatype import *
from pymeos import *
from pymeos import TPoint

import pyspark.sql.functions as F
from pyspark.sql.types import *

from datetime import datetime

from shapely import wkb, box, from_wkb

# Define the UDF for creating a TGeogPointInst
@F.udf(returnType=TGeomPointInstUDT())
def create_point_udf(lat, lon, time):
    pymeos_initialize("UTC")
    point_inst = TGeomPointInst(f"Point({lon} {lat})@{str(time)}")
    point_inst_str = point_inst.__str__()
    # print(point_inst)
    #pymeos_finalize()
    return point_inst
    
@F.udf(returnType=IntegerType())
def create_pointseq_instants_udf(lats, lons, times):
    # Initialize PyMEOS
    pymeos_initialize("UTC")
    
    # Combine lat, lon, and time into a list of tuples and sort them by time
    combined = sorted(zip(lats, lons, times), key=lambda x: x[2])
        
    points_inst_list = [TGeogPointInst(f"Point({lon} {lat})@{time}") for lat, lon, time in combined]
    point_seq = TGeogPointSeq(instant_list=points_inst_list, lower_inc=True, upper_inc=True)
    instants = point_seq.num_instants()
    return instants

@F.udf(returnType=FloatType())
def get_point_x(point: TPoint):
    pymeos_initialize("UTC")
    return point.x().start_value()

@F.udf(returnType=FloatType())
def get_point_y(point: TPoint):
    pymeos_initialize("UTC")
    return point.y().start_value()

@F.udf(returnType=FloatType())
def get_point_z(point: TPoint):
    pymeos_initialize("UTC")
    return point.z().start_value()
        
@F.udf(returnType=TimestampType())
def get_point_timestamp(point: TPoint):
    pymeos_initialize("UTC")
    ts = point.start_timestamp()
    return ts

@F.udf(returnType=STBoxUDT())
def bounds_as_box(xmin: TFloat, xmax: TFloat, ymin: TFloat, ymax: TFloat, tmin: datetime=None, tmax: datetime=None, zmin: TFloat=None, zmax: TFloat=None):
    pymeos_initialize("UTC")
    if tmin and zmin:
        return STBox(xmax=xmax, xmin=xmin, ymax=ymax, ymin=ymin, tmin=tmin, tmax=tmax, zmin=zmin, zmax=zmax)
    elif tmin and not zmin:
        return STBox(xmax=xmax, xmin=xmin, ymax=ymax, ymin=ymin, tmin=tmin, tmax=tmax)
    elif zmin and not tmin:
        return STBox(xmax=xmax, xmin=xmin, ymax=ymax, ymin=ymin, zmin=zmin, zmax=zmax)

@F.udf(returnType=BooleanType())
def ever_touches(trip, instant):
    pymeos_initialize()
    return trip.ever_touches(instant)

@F.udf(returnType=TsTzSpanUDT())
def tstzspan(period):
    pymeos_initialize()
    return TsTzSpan(period)

@F.udf(returnType=GeometryUDT())
def geometry_from_hexwkb(geom):
    pymeos_initialize()
    return wkb.loads(geom, hex=True)

@F.udf(returnType=TGeomPointSeqUDT())
def trip_from_hexwkb(trip: StringType()):
    pymeos_initialize()
    # Tweak to avoid MEOS bug, should be fixed once PyMEOS is updated.
    #tseq = TGeogPointSeq(trip)
    return TGeomPointSeq(trip)
    #return TGeogPointSeq.from_hexwkb(trip)
    
@F.udf(returnType=GeometryUDT())
def tpoint_at(trip: TGeomPointSeq, instant):
    pymeos_initialize()
    tripat = trip.at(instant)
    if tripat:
        return tripat.start_value()

@F.udf(returnType=BooleanType())
def temporally_contains(trip, instant):
    pymeos_initialize()
    return trip.temporally_contains(instant)
