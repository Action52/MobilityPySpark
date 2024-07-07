import math

from pysparkmeos.UDT import STBoxWrap
from pysparkmeos.partitions.grid.grid_partitioner import GridPartition
from pysparkmeos.partitions.mobility_partitioner import MobilityPartitioner
from pysparkmeos.utils.utils import new_bounds_from_axis, from_axis, new_bounds, \
    bounds_calculate_map, bounds_calculate_reduce
from typing import *
from pymeos import *
from pymeos import TPoint
import numpy as np
import pandas as pd
import itertools

from pyspark.ml.feature import StandardScaler
from pyspark.ml.clustering import BisectingKMeans
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler

import pyspark.sql.functions as F

class BisectingKMeansPartitioner(MobilityPartitioner):
    def __init__(
            self,
            bounds,
            instants,
            traj_colname,
            num_tiles,
            dimensions=['x', 'y', 't'],
            utc="UTC",
            seed=3
    ):
        self.grid = [
            tile for tile in self._generate_grid(
                bounds, instants, traj_colname, num_tiles, dimensions, utc, seed
            )
        ]
        self.tilesstr = [tile.__str__() for tile in self.grid]
        self.numPartitions = len(self.tilesstr)
        super().__init__(self.numPartitions, self.get_partition)

    @staticmethod
    def _generate_grid(
            bounds: STBox,
            instants,
            traj_colname,
            num_tiles: int,
            dimensions=["x", "y", "t"],
            utc: str = "UTC",
            seed=None
    ):
        pymeos_initialize(utc)
        if 't' in dimensions:
            instants = instants.withColumn("ts", F.unix_timestamp('t'))

        dims = [dim for dim in dimensions if dim != 't']
        dims.append('ts')

        vt = VectorAssembler(inputCols=dims, outputCol='vecfeatures')
        scaler = StandardScaler(withStd=True, withMean=True)
        scaler.setInputCol("vecfeatures")
        scaler.setOutputCol("features")
        if seed:
            bkm = BisectingKMeans(k=num_tiles, seed=seed)
        else:
            bkm = BisectingKMeans(k=num_tiles, seed=seed)

        pipeline = Pipeline(stages=[vt, scaler, bkm])
        model = pipeline.fit(instants)
        prediction = model.transform(instants).select(traj_colname,
                                                      'prediction')

        tiles = [
            prediction.where(f'prediction = {cluster_idx}').rdd.mapPartitions(
                lambda x: bounds_calculate_map(x, traj_colname, utc)
            ).reduce(
                lambda x, y: bounds_calculate_reduce(x, y, utc)
            )
            for cluster_idx in range(num_tiles)
        ]
        return tiles

    def num_partitions(self) -> int:
        """Return the total number of partitions."""
        return self.numPartitions
