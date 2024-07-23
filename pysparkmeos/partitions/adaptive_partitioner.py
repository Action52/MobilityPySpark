from pysparkmeos.partitions.mobility_partitioner import MobilityPartitioner
from pysparkmeos.utils.utils import new_bounds
from typing import *
from pymeos import *
from pymeos import TPoint
import pandas as pd
import itertools


class AdaptiveBinsPartitioner(MobilityPartitioner):
    """
    Class to partition data using a KDTree.
    """

    def __init__(
            self,
            bounds,
            movingobjects,
            num_tiles,
            dimensions=["x", "y", "t"],
            utc="UTC"
    ):
        self.grid = [
            tile for tile in self._generate_grid(
                bounds, movingobjects, num_tiles, dimensions, utc
            )
        ]
        self.tilesstr = [tile.__str__() for tile in self.grid]
        self.numPartitions = len(self.tilesstr)
        super().__init__(self.numPartitions, self.get_partition)


    @staticmethod
    def _generate_grid(
            bounds: STBox,
            movingobjects: List[TPoint],
            num_tiles: int,
            dimensions = ["x", "y", "t"],
            utc: str = "UTC"
    ):
        pymeos_initialize(utc)

        unchecked_dims = dimensions
        new_tiles = {}
        while(unchecked_dims):
            cur_dim = unchecked_dims.pop()
            new_tiles[cur_dim] = AdaptiveBinsPartitioner._generate_grid_dim(
                            bounds,
                            movingobjects,
                            cur_dim,
                            num_tiles,
                            utc
            )

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
            bounds: STBox,
            movingobjects: List[TPoint],
            dim: str,
            num_tiles: int,
            utc: str
    ):
        pymeos_initialize(utc)

        pointsat = [point.at(bounds) for point in movingobjects if point.at(bounds)]
        values = None

        # Extract the values from the corresponding dimension
        if dim == 'x':
            values = [
                instant.x().start_value()
                for traj in pointsat
                    for instant in traj.instants()
            ]
        if dim == 'y':
            values = [
                instant.y().start_value()
                for traj in pointsat
                    for instant in traj.instants()
            ]
        if dim == 'z':
            values = [
                instant.z().start_value()
                for traj in pointsat
                    for instant in traj.instants()
            ]
        if dim == 't':
            values = [
                timestamp
                for traj in pointsat
                    for timestamp in traj.timestamps()
            ]
        num_points = len(values)
        values = sorted(values)
        max_value = max(values)
        if len(values) == 1:
            return [new_bounds(bounds, dim, values[0], max_value)]
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
        #print(values)
        #print(df)

        binvals = [
            (min_val, max_val)
            for min_val, max_val in zip(
                df["Value"].dropna(), df["Lead"].dropna()
            )
        ]

        tiles = [
            new_bounds(bounds, dim, bin[0], bin[1])
            for bin in binvals if bin[0] != bin[1]
        ]
        return tiles

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
    mdtpart = AdaptiveBinsPartitioner(
        bounds=bounds,
        movingobjects=testtrips,
        num_tiles=3,
        dimensions=["x", "y"],
        utc="UTC"
    )
    print(sorted(mdtpart.grid))
    print(len(mdtpart.tilesstr))


if __name__ == '__main__':
    main()
