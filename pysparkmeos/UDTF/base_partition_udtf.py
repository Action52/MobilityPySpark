from typing import *
from pyspark import Row
from pymeos import *

from pyspark.sql.types import *
from pysparkmeos.UDT.MeosDatatype import *

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
