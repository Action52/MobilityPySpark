from pysparkmeos.UDT.MeosDatatype import *
from pymeos import pymeos_initialize, pymeos_finalize
from pymeos import TGeogPointInst, TGeogPointSeq

from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

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