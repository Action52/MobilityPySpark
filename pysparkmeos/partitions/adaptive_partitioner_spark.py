import math

from pysparkmeos.UDT import STBoxWrap
from pysparkmeos.partitions.grid.grid_partitioner import GridPartition
from pysparkmeos.partitions.mobility_partitioner import MobilityPartitioner
from pysparkmeos.utils.utils import new_bounds_from_axis, from_axis, new_bounds
from typing import *
from pymeos import *
from pymeos import TPoint
import numpy as np
import pandas as pd
import itertools


class AdaptiveBinsPartitionerSpark(MobilityPartitioner):
    def __init__(
            self,
            spark,
            bounds,
            df,
            colname,
            num_tiles,
            dimensions=["x", "y", "t"],
            utc="UTC"
    ):
        self.grid = [
            tile for tile in self._generate_grid(
                spark, bounds, df, colname, num_tiles, dimensions, utc
            )
        ]
        self.tilesstr = [tile.__str__() for tile in self.grid]
        self.numPartitions = len(self.tilesstr)
        super().__init__(self.numPartitions, self.get_partition)

    @staticmethod
    def _generate_grid(
            spark,
            bounds: STBox,
            df,
            colname,
            num_tiles: int,
            dimensions=["x", "y", "t"],
            utc: str = "UTC"
    ):
        pymeos_initialize(utc)

        unchecked_dims = dimensions
        new_tiles = {}
        while (unchecked_dims):
            cur_dim = unchecked_dims.pop()
            new_tiles[
                cur_dim] = AdaptiveBinsPartitionerSpark._generate_grid_dim(
                spark, bounds, df, colname, cur_dim, num_tiles, utc)

        tiles = []
        for combination in itertools.product(*new_tiles.values()):
            combination_dict = dict(zip(new_tiles.keys(), combination))
            intersection_box = None
            for dim, box in combination_dict.items():
                if not intersection_box:
                    intersection_box = box
                else:
                    intersection_box = intersection_box.intersection(box)
            if intersection_box:
                tiles.append(intersection_box)
        return tiles

    @staticmethod
    def _generate_grid_dim(
            spark,
            bounds: STBox,
            df,
            colname,
            dim: str,
            num_tiles: int,
            utc: str
    ):
        pymeos_initialize(utc)

        df.createOrReplaceTempView('trajectories')

        dims_query = f"""
        WITH 
        Vals(dim) AS (
            SELECT explode(spatial_values({colname}, '{dim}'))
            FROM trajectories
            ORDER BY 1 
        ),
        MaxDimVals(MaxDim) AS (
            SELECT MAX(dim) 
            FROM Vals
        ),
        Bins1(BinId, dim) AS (
            SELECT NTILE({num_tiles}) OVER(ORDER BY dim), dim
            FROM Vals
        ),
        Bins2(BinId, dim, RowNo) AS (
            SELECT BinId, dim, ROW_NUMBER() OVER (PARTITION BY BinId ORDER BY dim)
            FROM Bins1
        )
        SELECT 
            BinId, 
            RowNo, 
            dim AS minbound,
            LEAD(dim, 1) OVER(ORDER BY dim) AS maxbound
        FROM Bins2, MaxDimVals
        WHERE RowNo = 1;
        """
        if dim == 't':
            dims_query = f"""
                WITH 
                Times(T) AS (
                    SELECT explode(timestamps(PointSeq))
                    FROM trajectories
                    ORDER BY 1 
                ),
                MaxTime(MaxT) AS (
                    SELECT MAX(T) 
                    FROM Times 
                ),
                Bins1(BinId, T) AS (
                    SELECT NTILE({num_tiles}) OVER(ORDER BY T), T
                    FROM Times 
                ),
                Bins2(BinId, T, RowNo) AS (
                    SELECT BinId, T, ROW_NUMBER() OVER (PARTITION BY BinId ORDER BY T)
                    FROM Bins1
                )
                SELECT 
                    BinId, 
                    T AS minbound, 
                    LEAD(T, 1) OVER(ORDER BY T) AS maxbound
                FROM Bins2, MaxTime
                WHERE RowNo = 1;
            """
        binrows = spark.sql(dims_query).collect()
        tiles = []
        for i, row in enumerate(binrows):
            minbound = row.minbound
            maxbound = row.maxbound
            if i == 0:
                if dim == 'x':
                    minbound = min(bounds.xmin(), row.minbound)
                if dim == 'y':
                    minbound = min(bounds.ymin(), row.minbound)
                if dim == 'z':
                    minbound = min(bounds.zmin(), row.minbound)
                if dim == 't':
                    minbound = row.minbound.replace(tzinfo=bounds.tmin().tzinfo)
                    minbound = min(bounds.tmin(), minbound)
            if i == len(binrows) - 1:
                if dim == 'x':
                    maxbound = bounds.xmax()
                if dim == 'y':
                    maxbound = bounds.ymax()
                if dim == 'z':
                    maxbound = bounds.zmax()
                if dim == 't':
                    maxbound = bounds.tmax()
            tiles.append(STBoxWrap(
                new_bounds(bounds, dim, minbound, maxbound).__str__()))
        return tiles

    def num_partitions(self) -> int:
        """Return the total number of partitions."""
        return self.numPartitions
