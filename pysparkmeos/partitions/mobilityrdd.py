from abc import abstractmethod, ABC

from pyspark.rdd import Partitioner
import pyspark.sql.functions as F
from pyspark.sql.types import *

from pymeos import *
from typing import *

from pysparkmeos.UDT.MeosDatatype import *

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
        return TilePartitionedTable(F.lit(tilesstr))

    @staticmethod
    def get_partition(
        movingobject: Union[TGeomPoint, TGeogPoint, STBox, Geometry],
        tiles: List[STBox],
        utc_time: str = "UTC"
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

    def plot(self, how: str = 'xy', **kwargs):
        """
        Plots the tiles.

        :param how: Indicate how to plot. Options are: 'xy', 'xt', 'yt'.
        :param kwargs: Arguments to pass to the plotter.
        :return:
        """
        for tile in self.tilesstr:
            stbox = STBox(tile)
            if how == 'xy':
                stbox.plot_xy(**kwargs)
            elif how == 'xt':
                stbox.plot_xt(**kwargs)
            elif how == 'yt':
                stbox.plot_yt(**kwargs)


partition_schema = StructType(
    [
        StructField("movingobjectid", StringType()),
        StructField("tileid", IntegerType()),
        StructField("movingobject", TGeomPointSeqSetUDT()),
    ]
)


class BasePartitionUDTF:
    def __init__(
            self,
            response_extra_cols: Iterable = None,
            check_function: Callable = None,
            return_full_traj: bool = False
    ):
        """
        Useful to inherit this class to other UDTFs that have the same behavior
        but need to yield extra tables.

        :param response_extra_cols: Iterable containing the column names to
        yield along with the predefined schema.
        :param check_function: If passed, this function is used to tile the data.
        :param return_full_traj: If True, returns the full movingobject instead
        of the partition corresponding to the tile.
        """

        self.response_extra_cols = response_extra_cols
        self.check_function = check_function
        self.return_full_traj = return_full_traj

    def eval_wrap(self, row: Row):
        """
        Caller function to yield values into the new table.
        Basic behavior expects a row with the following values:
            - trajectory_id (int)
            - trajectory (PyMEOS type)
            - tiles (List[PyMEOS type])
            - tileids (List[int])

        This class and function should be inherited and overriden to fit
        each use case, for instance, to add more columns to the output schema.

        Any UDTF in PySpark should be registered to the SparkContext as:
            spark.udtf.register("PartitionUDTF", PartitionUDTF)

        This is already handled by PySparkMEOS for the included classes.

        Example call:

            SELECT *
            FROM PartitionUDTF(
                TABLE(
                        SELECT
                            trajectory_id,
                            trajectory,
                            (SELECT collect_list(tile) FROM grid) AS tiles,
                            (SELECT collect_list(tileid) FROM grid) AS tileids
                        FROM trips
                )
            )

        :param row: The Row type with the values. Automatically passed by
            Pyspark.

        :return: A new dataframe with the partitioned data, with the schema:
            - predefined columns (if provided).
            - mobilityobjectid (str)
            - tileid (int)
            - movingobject (PySparkMEOSUDT)
        """
        pymeos_initialize()

        trajectory_id = row.trajectory_id
        trajectory = row.movingobject
        tiles = row.tiles
        tileids = row.tileids

        if not self.check_function:
            partitioned = [
                (key, trajectory.at(tile))
                for key, tile in zip(tileids, tiles)
            ]
        else:
            partitioned = [
                (key, self.check_function(trajectory, tile))
                for key, tile in zip(tileids, tiles)
            ]
        count = 0
        for partition_key, partition_traj in partitioned:
            count += 1
            if partition_traj is None or partition_traj is False:
                continue
            else:
                response = [
                    row[col]
                    for col in self.response_extra_cols if
                    self.response_extra_cols
                ]
                if self.return_full_traj:
                    response.extend(
                        [trajectory_id, partition_key, trajectory]
                    )
                else:
                    response.extend(
                        [trajectory_id, partition_key, partition_traj]
                    )
                yield response


@F.udtf(returnType=partition_schema)
class PartitionUDTF:
    """
    Sample class for partitioning mobility data with PySpark and PyMEOS.
    Should be inherited and overriden to fit the schema at hand.
    """
    def __init__(self, response_extra_cols: Iterable = None):
        """
        Useful to inherit this class to other UDTFs that have the same behavior
        but need to yield extra tables.

        :param response_extra_cols: Iterable containing the column names to
        yield along with the predefined schema.
        """
        self.response_extra_cols = response_extra_cols

    def eval(self, row: Row):
        """
        Caller function to yield values into the new table.
        Basic behavior expects a row with the following values:
            - trajectory_id (int)
            - trajectory (PyMEOS type)
            - tiles (List[PyMEOS type])
            - tileids (List[int])

        This class and function should be inherited and overriden to fit
        each use case, for instance, to add more columns to the output schema.

        Any UDTF in PySpark should be registered to the SparkContext as:
            spark.udtf.register("PartitionUDTF", PartitionUDTF)

        This is already handled by PySparkMEOS for the included classes.

        Example call:

            SELECT *
            FROM PartitionUDTF(
                TABLE(
                        SELECT
                            trajectory_id,
                            trajectory,
                            (SELECT collect_list(tile) FROM grid) AS tiles,
                            (SELECT collect_list(tileid) FROM grid) AS tileids
                        FROM trips
                )
            )

        :param row: The Row type with the values. Automatically passed by
            Pyspark.

        :return: A new dataframe with the partitioned data, with the schema:
            - predefined columns (if provided).
            - mobilityobjectid (str)
            - tileid (int)
            - movingobject (PySparkMEOSUDT)
        """
        pymeos_initialize()

        trajectory_id = row['trajectory_id']
        trajectory = row['trajectory']
        tiles = row['tiles']
        tileids = row['tileids']

        partitioned = [
            (key, trajectory.at(tile))
            for key, tile in zip(tileids, tiles)
        ]
        count = 0
        response = [
            row[col]
            for col in self.response_extra_cols if self.response_extra_cols
        ]
        for partition_key, partition_traj in partitioned:
            count += 1
            if partition_traj is None:
                continue
            else:
                response.extend([trajectory_id, partition_key, partition_traj])
                yield response


@F.udtf(returnType=StructType([
    StructField("tileid", IntegerType()),
    StructField("tile", STBoxUDT())
]))
class TilePartitionedTable:
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
            yield tileid, STBox(tile)
