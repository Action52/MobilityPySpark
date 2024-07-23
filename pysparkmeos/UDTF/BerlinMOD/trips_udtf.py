from pymeos import *
from pysparkmeos.UDT.MeosDatatype import *
from pysparkmeos.UDTF.base_partition_udtf import BasePartitionUDTF
from pyspark.sql.types import *
import pyspark.sql.functions as F


"""
    Expected output schema for Trips.
"""
schema = StructType(
    [
        StructField("vehid", IntegerType()),
        StructField("day", IntegerType()),
        StructField("seqno", IntegerType()),
        StructField("sourcenode", IntegerType()),
        StructField("targetnode", StringType()),
        StructField("trajectory", GeometryUDT()),
        StructField("license", StringType()),
        StructField("movingobjectid", StringType()),
        StructField("tileid", IntegerType()),
        StructField("movingobject", TGeomPointSeqSetUDT()),
    ]
)


@F.udtf(returnType=schema)
class TripsUDTF(BasePartitionUDTF):
    """
    UDTF for Trips table.
    """

    def __init__(self):
        response_extra_cols = [
            "vehid",
            "day",
            "seqno",
            "sourcenode",
            "targetnode",
            "trajectory",
            "licence",
        ]
        check_function = lambda traj, tile: traj.at(tile)
        super().__init__(
            response_extra_cols=response_extra_cols,
            check_function=check_function,
            return_full_traj=False,
        )

    def eval(self, row: Row):
        for val in super().eval_wrap(row):
            yield val
