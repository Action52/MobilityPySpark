from pymeos import *
from pysparkmeos.UDT.MeosDatatype import *
from pysparkmeos.partitions.mobilityrdd import BasePartitionUDTF
from pyspark.sql.types import *
import pyspark.sql.functions as F


schema = StructType([
    StructField("instantid", IntegerType()),
    StructField("tileid", IntegerType()),
    StructField("instant", TBoolInstUDT())
])


@F.udtf(returnType=schema)
class InstantsUDTF(BasePartitionUDTF):
    def __init__(self):
        check_function = lambda instant, tile: instant.temporally_overlaps(tile)
        super().__init__(
            response_extra_cols=[],
            check_function=check_function,
            return_full_traj=True
        )

    def eval(self, row: Row):
        for val in super().eval_wrap(row):
            yield val
