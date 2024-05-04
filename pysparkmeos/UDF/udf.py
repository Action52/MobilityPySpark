from pysparkmeos.UDT.MeosDatatype import *
from pymeos import *

from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, FloatType, TimestampType

from datetime import datetime

# Define the UDF for creating a TGeogPointInst
@udf(returnType=TGeogPointInstUDT())
def create_point_udf(lat, lon, time):
    pymeos_initialize("UTC")
    point_inst = TGeogPointInst(f"Point({lon} {lat})@{str(time)}")
    point_inst_str = point_inst.__str__()
    # print(point_inst)
    #pymeos_finalize()
    return point_inst
    
@udf(returnType=IntegerType())
def create_pointseq_instants_udf(lats, lons, times):
    # Initialize PyMEOS
    pymeos_initialize("UTC")
    
    # Combine lat, lon, and time into a list of tuples and sort them by time
    combined = sorted(zip(lats, lons, times), key=lambda x: x[2])
        
    points_inst_list = [TGeogPointInst(f"Point({lon} {lat})@{time}") for lat, lon, time in combined]
    point_seq = TGeogPointSeq(instant_list=points_inst_list, lower_inc=True, upper_inc=True)
    instants = point_seq.num_instants()
    return instants

@udf(returnType=FloatType())
def get_point_x(point: TPointInst):
    pymeos_initialize("UTC")
    return point.x().start_value()

@udf(returnType=FloatType())
def get_point_y(point: TPointInst):
    pymeos_initialize("UTC")
    return point.y().start_value()

@udf(returnType=FloatType())
def get_point_z(point: TPointInst):
    pymeos_initialize("UTC")
    return point.z().start_value()
        
@udf(returnType=TimestampType())
def get_point_timestamp(point: TPointInst):
    pymeos_initialize("UTC")
    ts = point.start_timestamp()
    return ts

@udf(returnType=STBoxUDT())
def bounds_as_box(xmin: TFloat, xmax: TFloat, ymin: TFloat, ymax: TFloat, tmin: datetime=None, tmax: datetime=None, zmin: TFloat=None, zmax: TFloat=None):
    pymeos_initialize("UTC")
    if tmin and zmin:
        return STBox(xmax=xmax, xmin=xmin, ymax=ymax, ymin=ymin, tmin=tmin, tmax=tmax, zmin=zmin, zmax=zmax)
    elif tmin and not zmin:
        return STBox(xmax=xmax, xmin=xmin, ymax=ymax, ymin=ymin, tmin=tmin, tmax=tmax)
    elif zmin and not tmin:
        return STBox(xmax=xmax, xmin=xmin, ymax=ymax, ymin=ymin, zmin=zmin, zmax=zmax)

