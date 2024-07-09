from pyspark.sql.types import *
import pyspark.sql.functions as F
from pymeos import *

from pysparkmeos.UDT.MeosDatatype import *

from typing import *


@F.udtf(returnType=StructType([
    StructField("tileid", IntegerType()),
    StructField("tile", STBoxUDT())
]))
class GridUDTF:
    """
    Class to create a Spark dataframe containing the tiles and the tileids.
    """
    def eval(
            self,
            tilesstr: List[str],
            col_dict={},
            utc_time: str = "UTC"
    ):
        """
        :param tilesstr: List of STBox strings representing the tiles
        :param col_dict: Dictionary to handle column names when using rows.
        :param utc_time: UTC Time to use in PyMEOS.
        :return: The new PySpark DataFrame with the schema:
            - tileid (int)
            - tile (STBox)
        """
        pymeos_initialize(utc_time)
        for tileid, tile in enumerate(tilesstr):
            if tile is not None:
                yield tileid, STBox(tile)