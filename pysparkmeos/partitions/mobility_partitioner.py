from abc import abstractmethod, ABC

from pyspark.rdd import Partitioner
import pyspark.sql.functions as F
from pyspark.sql.types import *

from pymeos import *
from typing import *

from pysparkmeos.UDT.MeosDatatype import *
from pysparkmeos.UDTF.grid_udtf import GridUDTF

from shapely import Geometry


class MobilityPartitioner(Partitioner):
    tilesstr = None

    """
    Defines a partition strategy. Follows the definition from official
    Spark Partitioner.
    """

    @abstractmethod
    def num_partitions(self) -> int:
        """Return the total number of partitions."""
        pass

    def as_spark_table(self):
        """
        Calls the TileSparkCreator class to return a PySpark DataFrame with the
        tileid, tile to use. Useful when we need to partition multiple
        tables based on the same tiling.

        :return: A PySpark Dataframe with the tileid, tile from the partitioner.
        """
        tilesstr = self.tilesstr  # Do this so that spark doesn't break
        return GridUDTF(F.lit(tilesstr))

    @staticmethod
    def get_partition(
        movingobject: Union[TGeomPoint, TGeogPoint, STBox, Geometry],
        tiles: List[STBox],
        utc_time: str = "UTC",
    ) -> int:
        """
        Function to partition a moving object and return the partition index(es)
        of the object, given a list of partition tiles. In practice,
        this function is replaced by the UDTF generators.

        :param movingobject: The PyMEOS datatype to partition.
        :param tiles: STBoxes representing the partition space.
        :param utc_time: UTC time to use with PyMEOS.
        :return: Yields the partition indexes, belonging in the partition space.
            If no tile matches the movingobject, or a part of it, it is assigned
            -1.
        """
        pymeos_initialize(utc_time)

        for idx, tile in enumerate(tiles):
            stbox: STBox = STBox(tile).set_srid(0)
            if type(movingobject) == Geometry:
                if tile.overlaps(movingobject) is not None:
                    yield idx
            if type(movingobject) == STBox:
                if tile.intersection(movingobject) is not None:
                    yield idx
            if movingobject.at(stbox) is not None:
                yield idx

        yield -1

    def plot(self, how: str = "xy", **kwargs):
        """
        Plots the tiles.

        :param how: Indicate how to plot. Options are: 'xy', 'xt', 'yt'.
        :param kwargs: Arguments to pass to the plotter.
        :return:
        """
        for tile in self.tilesstr:
            stbox = STBox(tile)
            if how == "xy":
                stbox.plot_xy(**kwargs)
            elif how == "xt":
                stbox.plot_xt(**kwargs)
            elif how == "yt":
                stbox.plot_yt(**kwargs)
