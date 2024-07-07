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
            df,
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
        self.grid = self._generate_grid(df, colname, index_colname, bounds, dimensions, 0, max_depth,
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
    def _generate_grid(instants, traj_colname, index_colname, bounds, dims, depth,
               max_depth, utc):
        pymeos_initialize(utc)
        dim = dims[depth % len(dims)]
        tiles = []

        instants_at = instants.rdd.mapPartitions(
            lambda x: KDTreePartitionSpark.map_at(x, traj_colname, index_colname, bounds, utc)
        ).toDF([index_colname, traj_colname, 'x', 'y', 't', 'rowNo'])

        num_instants = instants_at.count()

        if depth == max_depth or num_instants <= 1:
            tiles.append(bounds)
        else:
            if dim == 't':
                instants_at = instants_at.withColumn("ts", F.unix_timestamp(
                    dim)).orderBy("ts").withColumn('rowNo',
                                                   F.monotonically_increasing_id())
                # mediandim = instants_at.agg(F.median("ts")).collect()[0]['median(ts)']
                mediandim = instants_at.approxQuantile("rowNo", [0.5], 0.0)[0]
                lower = instants_at.where(f'rowNo<={mediandim}')
                upper = instants_at.where(f'rowNo>{mediandim}')
                midrow = instants_at.where(f'rowNo={mediandim}').first()
            else:
                instants_at = instants_at.orderBy(dim).withColumn('rowNo',
                                                                  F.monotonically_increasing_id())
                # mediandim = instants_at.agg(F.median(dim)).collect()[0][f'median({dim})']
                mediandim = instants_at.approxQuantile("rowNo", [0.5], 0.0)[0]
                lower = instants_at.where(f'rowNo<={mediandim}')
                upper = instants_at.where(f'rowNo>{mediandim}')
                midrow = instants_at.where(f'rowNo={mediandim}').first()

            bboxleft = STBoxWrap(
                new_bounds_from_axis(bounds, dim, midrow[traj_colname],
                                     "left").__str__())
            bboxright = STBoxWrap(
                new_bounds_from_axis(bounds, dim, midrow[traj_colname],
                                     "right").__str__())

            left = KDTreePartitionSpark._generate_grid(lower, traj_colname, index_colname, bboxleft, dims,
                          depth + 1, max_depth, utc)
            right = KDTreePartitionSpark._generate_grid(upper, traj_colname, index_colname, bboxright, dims,
                           depth + 1, max_depth, utc)

            tiles.extend(left)
            tiles.extend(right)

        return tiles

    def num_partitions(self) -> int:
        """Return the total number of partitions."""
        return self.numPartitions
