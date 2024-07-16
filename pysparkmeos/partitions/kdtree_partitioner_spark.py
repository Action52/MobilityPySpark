from pysparkmeos.UDT import STBoxWrap
from pysparkmeos.partitions.mobility_partitioner import MobilityPartitioner
from pysparkmeos.utils.utils import new_bounds_from_axis, from_axis
from typing import *
from pymeos import *
from pymeos import TPoint

import pyspark.sql.functions as F


class KDTreePartitionSpark(MobilityPartitioner):
    """
    Class to partition data using a KDTree.
    """

    def __init__(
            self,
            spark,
            df,
            instantstablename,
            colname,
            index_colname,
            bounds: STBox,
            dimensions: tuple,
            max_depth=12,
            utc="UTC"
    ):
        """
        :param moving_objects: A list containing the moving objects (or a sample)
        to create the partition structure with.
        :param bounds: STBox with the precalculated bounds of the space.
        :param dimensions: Dimensions to partition by in each recursion.
        For instance: ['x', 'y'], or ['x', 'y', 't'], or ['x'].
        :param max_depth: Max depth of the tree. Default=12
        (accounts for 2^12=4096 partitions).
        """
        self.max_depth = max_depth
        self.bounds = bounds
        self.grid = self._generate_grid(spark, df, instantstablename, colname,
                                        index_colname, bounds, dimensions, 0,
                                        max_depth,
                                        utc)
        self.numPartitions = len(self.grid)
        self.tilesstr = [tile.__str__() for tile in self.grid]
        super().__init__(self.numPartitions, self.get_partition)

    @staticmethod
    def map_at(rows, traj_colname, index_colname, bounds, utc):
        pymeos_initialize(utc)
        response = []
        for row in rows:
            at = row[traj_colname].at(bounds)
            if at:
                response.append((
                    row[index_colname], row[traj_colname], row['x'],
                    row['y'], row['t'], row['rowNo']))
        return response

    @staticmethod
    def _generate_grid(spark, instants, instantstablename, traj_colname,
                       index_colname, bounds, dims, depth,
                       max_depth, utc):
        pymeos_initialize(utc)
        dim = dims[depth % len(dims)]
        tiles = []

        instants_at = spark.sql(f"""
        SELECT * 
        FROM IsAtUDTF(
            TABLE(
                    SELECT * FROM {instantstablename}
            ),
            '{bounds.__str__()}'
        )
        """).cache()

        if instants_at.isEmpty():
            return tiles

        if depth == max_depth:
            tiles.append(bounds)
        else:
            if dim == 't':
                dim = 'ts'
            # mediandim = instants_at.agg(F.median("ts")).collect()[0]['median(ts)']
            # mediandim = instants_at.approxQuantile("rowNo", [0.5], 1)[0]
            rdd = instants_at.rdd.mapPartitionsWithIndex(
                    lambda i, x: KDTreePartitionSpark.partition_median_index(i,
                                                                             x,
                                                                             dim=dim,
                                                                             rowcol='rowNo')
            )
            medians = [median for median in rdd.toLocalIterator() if
                           median[1]]
            medians = sorted(medians, key=lambda x: x[1])
            median_index = len(medians) // 2
            median = medians[median_index]
            medianrow = median[1][1]
            # print(f"Found median row: {medianrow} for dim {dim} level {depth}.")
            lower = instants_at.where(f'rowNo<{medianrow}')
            upper = instants_at.where(f'rowNo>{medianrow}')
            midrow = median[1][2][traj_colname]
            if dim == 'ts':
                dim = 't'
            bboxleft = STBoxWrap(
                new_bounds_from_axis(bounds, dim, midrow,
                                     "left").__str__())
            bboxright = STBoxWrap(
                new_bounds_from_axis(bounds, dim, midrow,
                                     "right").__str__())

            left = KDTreePartitionSpark._generate_grid(spark, lower,
                                                       instantstablename,
                                                       traj_colname,
                                                       index_colname, bboxleft,
                                                       dims,
                                                       depth + 1, max_depth,
                                                       utc)
            right = KDTreePartitionSpark._generate_grid(spark, upper,
                                                        instantstablename,
                                                        traj_colname,
                                                        index_colname,
                                                        bboxright, dims,
                                                        depth + 1, max_depth,
                                                        utc)

            tiles.extend(left)
            tiles.extend(right)

        return tiles

    @staticmethod
    def partition_median_index(idx, rows, dim, rowcol):
        median_index = 0
        values = []
        for i, row in enumerate(rows):
            median_index = i // 2
            values.append((row[dim], row.rowNo, row))
        if values:
            values = sorted(values, key=lambda x: x[0])
            return [(idx, values[median_index])]
        else:
            return [(idx, None, None,)]

    def num_partitions(self) -> int:
        """Return the total number of partitions."""
        return self.numPartitions