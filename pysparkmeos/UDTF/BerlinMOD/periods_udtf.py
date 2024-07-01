from pymeos import *
from pysparkmeos.UDT.MeosDatatype import *
from pysparkmeos.UDTF.base_partition_udtf import BasePartitionUDTF
from pyspark.sql.types import *
import pyspark.sql.functions as F


schema = StructType([
    StructField("beginp", TimestampType()),
    StructField("endp", TimestampType()),
    StructField("periodid", IntegerType()),
    StructField("tileid", IntegerType()),
    StructField("period", TsTzSpanUDT())
])


@F.udtf(returnType=schema)
class PeriodsUDTF(BasePartitionUDTF):
    def __init__(self):
        check_function = lambda period, tile: period.overlaps(tile)
        super().__init__(
            response_extra_cols=['beginp', 'endp'],
            check_function=check_function,
            return_full_traj=True
        )

    def eval(self, row: Row):
        for val in super().eval_wrap(row):
            yield val
