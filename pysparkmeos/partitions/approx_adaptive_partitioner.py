from datetime import datetime
import math

from pyspark import Row

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


class ApproximateAdaptiveBinsPartitioner(MobilityPartitioner):
    """
    Class to partition data using an AdaptiveBinsPartitioner approximated with
    Spark Map-Reduce as engine.
    """

    def __init__(
            self,
            spark,
            df,
            colname,
            bounds,
            num_tiles,
            dimensions=["x", "y", "t"],
            utc="UTC",
            tablename="trips"
    ):
        if df is None:
            df = spark.table(tablename)
        self.grid = self.spark_generate_grid(
            df,
            colname,
            bounds,
            num_tiles,
            dimensions,
            utc
        )
        self.tilesstr = [tile.__str__() for tile in self.grid]
        self.numPartitions = len(self.tilesstr)
        super().__init__(self.numPartitions, self.get_partition)

    @staticmethod
    def spark_generate_grid(
            df,
            colname,
            bounds: STBox,
            num_tiles: int,
            dimensions=["x", "y", "t"],
            utc: str = "UTC"
    ):
        pymeos_initialize(utc)
        unchecked_dims = dimensions
        new_tiles = {}
        while (unchecked_dims):
            cur_dim = unchecked_dims.pop()
            reduced_bounds = df.select(colname).rdd.mapPartitions(
                lambda x: ApproximateAdaptiveBinsPartitioner.map_generate_grid_dim(
                    x, colname, bounds, cur_dim, num_tiles, utc)
            ).reduceByKey(
                lambda x, y: ApproximateAdaptiveBinsPartitioner.reduce_generate_grid_dim(x, y)
            ).collect()
            new_tiles[cur_dim] = ApproximateAdaptiveBinsPartitioner.make_bounds_contiguous(
                reduced_bounds, bounds, cur_dim
            )
            # print(new_tiles[cur_dim])

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
    def map_generate_grid_dim(
            movingobjects,
            colname,
            bounds: STBox,
            dim: str,
            num_tiles: int,
            utc: str
    ):
        pymeos_initialize(utc)

        movingobjects = [mo[colname] for mo in movingobjects if type(mo) == Row]

        pointsat = [point.at(bounds) for point in movingobjects if
                    point.at(bounds)]
        values = None

        # Extract the values from the corresponding dimension
        if dim == 'x':
            values = [value for traj in pointsat for value in traj.x().values()]
        if dim == 'y':
            values = [value for traj in pointsat for value in traj.y().values()]
        if dim == 'z':
            values = [value for traj in pointsat for value in traj.z().values()]
        if dim == 't':
            values = [
                timestamp
                for traj in pointsat
                for timestamp in traj.timestamps()
            ]
        num_points = len(values)
        if num_points <= 1:
            return []
        values = sorted(values)
        max_value = max(values)
        # print(values)

        if dim == 't':
            valuest = sorted(
                list({value.timestamp(): value for value in values}.values()))
        else:
            valuest = values
        # print(valuest)
        bins = pd.qcut(valuest, q=num_tiles, labels=False)
        df = pd.DataFrame({
            'Value': valuest,
            'BinId': bins
        })
        df['RowNo'] = df.groupby('BinId').cumcount() + 1
        df = df.where(df.RowNo == 1).sort_values('Value').reset_index(drop=True)
        df['Lead'] = df['Value'].shift(-1)
        df['Lead'] = df['Lead'].fillna(max_value)
        # print(values)
        # print(df)

        binvals = [
            (min_val, max_val)
            for min_val, max_val in zip(
                df["Value"].dropna(), df["Lead"].dropna()
            )
        ]

        tiles = [
            STBoxWrap(new_bounds(bounds, dim, bin[0], bin[1]).__str__())
            for bin in binvals if bin[0] != bin[1]
        ]
        return [(i, (bin[0], bin[1])) for i, bin in enumerate(binvals) if
                bin[0] != bin[1]]

    @staticmethod
    def reduce_generate_grid_dim(
            val1, val2):
        def average_datetimes(dt1, dt2):
            """ Compute the average of two datetime objects. """
            # Convert datetime objects to timestamps (seconds since the epoch)
            timestamp1 = dt1.timestamp()
            timestamp2 = dt2.timestamp()
            # Calculate the average timestamp and convert it back to datetime
            mean_timestamp = (timestamp1 + timestamp2) / 2
            meandt = datetime.fromtimestamp(mean_timestamp)
            meandt = meandt.replace(tzinfo=dt1.tzinfo)
            return meandt

            # Check if the first item in the tuples are instances of datetime

        if isinstance(val1[0], datetime) and isinstance(val2[0], datetime):
            meanmin = average_datetimes(val1[0], val2[0])
            meanmax = average_datetimes(val1[1], val2[1])
        else:
            # Assuming the values are numeric (int or float)
            meanmin = round((val1[0] + val2[0]) / 2, 2)
            meanmax = round((val1[1] + val2[1]) / 2, 2)

        return (meanmin, meanmax)

    @staticmethod
    def map_generate_stbox_dim(
            key,
            vals,
            utc,
            dim,
            bounds,
            num_tiles
    ):
        pymeos_initialize(utc)
        minval = vals[0]
        maxval = vals[1]
        if key == 0:
            if dim == 'x':
                minval = min(bounds.xmin(), vals[0])
            if dim == 'y':
                minval = min(bounds.ymin(), vals[0])
            if dim == 'z':
                minval = min(bounds.zmin(), vals[0])
            if dim == 't':
                # mint = vals[0].astimezone(utc)
                minval = min(bounds.tmin(), vals[0])
        if key == num_tiles - 1:
            if dim == 'x':
                maxval = max(bounds.xmax(), vals[1])
            if dim == 'y':
                maxval = max(bounds.ymax(), vals[1])
            if dim == 'z':
                maxval = max(bounds.zmax(), vals[1])
            if dim == 't':
                # maxt = vals[1].astimezone(utc)
                maxval = max(bounds.tmax(), vals[1])
        return STBoxWrap(new_bounds(bounds, dim, minval, maxval).__str__())

    @staticmethod
    def make_bounds_contiguous(bounds_list, real_bounds, dim):
        sorted_bounds = sorted(bounds_list, key=lambda x: x[1][0])
        contiguous_bounds = []

        current_max = sorted_bounds[0][1][0]  # Start with the first min_bound
        processed = set()
        for i, (index, (min_bound, max_bound)) in enumerate(sorted_bounds):
            if (min_bound, max_bound) in processed:
                continue
            adjusted_min_bound = current_max
            adjusted_max_bound = max_bound
            contiguous_bounds.append(
                (i, (adjusted_min_bound, adjusted_max_bound)))
            current_max = adjusted_max_bound
            processed.add((min_bound, max_bound))
        final_bounds = []
        for key, (min_bound, max_bound) in contiguous_bounds:
            minval = min_bound
            maxval = max_bound
            if key == 0:
                if dim == 'x':
                    minval = min(real_bounds.xmin(), minval)
                if dim == 'y':
                    minval = min(real_bounds.ymin(), minval)
                if dim == 'z':
                    minval = min(real_bounds.zmin(), minval)
                if dim == 't':
                    minval = min(real_bounds.tmin(), minval)
            if key == len(contiguous_bounds) - 1:
                if dim == 'x':
                    maxval = max(real_bounds.xmax(), maxval)
                if dim == 'y':
                    maxval = max(real_bounds.ymax(), maxval)
                if dim == 'z':
                    maxval = max(real_bounds.zmax(), maxval)
                if dim == 't':
                    maxval = max(real_bounds.tmax(), maxval)
            box = STBoxWrap(
                new_bounds(real_bounds, dim, minval, maxval).__str__())
            final_bounds.append(box)
        return final_bounds

    def num_partitions(self) -> int:
        """Return the total number of partitions."""
        return self.numPartitions


def gentestdata(read_as='TInst'):
    import pandas as pd

    testtrips = pd.read_csv(
        "./minimini_sample_pymeos.csv",
        index_col=0
    )
    testtrips['trip'] = testtrips['trip'].apply(lambda x: TGeomPointSeqSet(x))
    bounds = TemporalPointExtentAggregator().aggregate(testtrips['trip'])
    seqs1 = []
    seqs = []
    if read_as == 'TInst':
        seqs1 = [trip.instants() for trip in testtrips['trip']]
        for seq in seqs1:
            seqs.extend(seq)
        return seqs, bounds
    if read_as == 'TSeq':
        seqs1 = [trip.sequences() for trip in testtrips['trip']]
        for seq in seqs1:
            seqs.extend(seq)
        return seqs, bounds
    if read_as == 'TSeqSet':
        return list(testtrips['trip']), bounds


def main():
    pymeos_initialize()
    testtrips, bounds = gentestdata(read_as='TSeqSet')
    mdtpart = ApproximateAdaptiveBinsPartitioner(
        bounds=bounds,
        num_tiles=3,
        dimensions=["x", "y"],
        utc="UTC"
    )
    print(sorted(mdtpart.grid))
    print(len(mdtpart.tilesstr))


if __name__ == '__main__':
    main()
