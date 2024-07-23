from pymeos import *
from pysparkmeos.UDT.MeosDatatype import *
from pysparkmeos.UDTF.base_partition_udtf import BasePartitionUDTF
from pyspark.sql.types import *
import pyspark.sql.functions as F


schema = StructType(
    [
        StructField("regionid", IntegerType()),
        StructField("tileid", IntegerType()),
        StructField("geom", GeometryUDT()),
    ]
)


@F.udtf(returnType=schema)
class RegionsUDTF(BasePartitionUDTF):
    """
    UDTF for Regions table.
    """

    def __init__(self):
        check_function = lambda geom, tile: tile.overlaps(geom)
        super().__init__(
            response_extra_cols=[], check_function=check_function, return_full_traj=True
        )

    def eval(self, row: Row):
        for val in super().eval_wrap(row):
            yield val
