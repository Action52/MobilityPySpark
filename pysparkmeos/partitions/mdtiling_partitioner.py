import math

from pysparkmeos.partitions.grid.grid_partitioner import GridPartition
from pysparkmeos.partitions.mobility_partitioner import MobilityPartitioner
from pysparkmeos.utils.utils import new_bounds_from_axis
from typing import *
from pymeos import *
from pymeos import TPoint


class MDTilingPartitioner(MobilityPartitioner):
    """
    Class to partition data using a KDTree.
    """

    def __init__(
            self,
            num_executors,
            d,
            moving_objects
    ):
        self.num_executors = num_executors
        self.d = d
        self.grid = self._multidimensional_tiles(
            moving_objects=moving_objects,
            num_executors=num_executors,
            d=d
        )
        self.tilesstr = [tile.__str__() for tile in self.grid]
        self.numPartitions = len(self.tilesstr)
        super().__init__(self.numPartitions, self.get_partition)

    @staticmethod
    def _multidimensional_tiles(
            moving_objects: Iterable[TPoint],
            num_executors,
            d,
            utc: str = "UTC"
    ):
        pymeos_initialize(utc)
        G: STBox = TemporalPointExtentAggregator().aggregate(moving_objects)

        def get_tpoints_at_stbox(mov_objects: List[TPoint], stbox: STBox):
            tpoints = [
                tpoint.at(stbox)
                for tpoint in mov_objects if tpoint.at(stbox) is not None
            ]
            return tpoints, sum(
                tp.num_instants()
                for seqset in tpoints
                    for tp in seqset.sequences()
            )
        # Get sequences and num_points in space
        moving_objects , num_points = get_tpoints_at_stbox(moving_objects, G)
        tao = num_points / num_executors

        print(f"Points in space: {num_points}. Tao: {tao}.")

        unchecked = [G]
        checked = []
        tmax = math.inf
        point_counts = None
        while(tmax > tao):
            while unchecked:
                g = unchecked.pop()
                moving_objects, num_points = get_tpoints_at_stbox(moving_objects, g)
                if num_points > tao:
                    print("Entering ", num_points, tao)
                    # Divide the grid tile into 2d tiles and partition trajectories T into segments: Ts 1, Ts 2, . . ., Ts k according to the spatiotemporal bounds of the new grid tiles Gi
                    regular_partitioner = GridPartition(2, g)
                    unchecked.extend(regular_partitioner.grid)
                    point_counts = [
                        get_tpoints_at_stbox(moving_objects, tile)[1] for tile
                        in regular_partitioner.grid]
                    print(point_counts)
                else:
                    print(f"{g} already below tao ({num_points}), skipping")
                checked.append(g)
                if point_counts:
                    tmax = max(point_counts)
                print(tmax, tao)

        # Encode

        return checked

    @staticmethod
    def _merge_grid_tiles(tilesG, tao, m):
        pass

    def num_partitions(self) -> int:
        """Return the total number of partitions."""
        return self.numPartitions


def gentestdata(read_as='TInst'):
    import pandas as pd

    testtrips = pd.read_csv(
        "../mobilitydb-berlinmod-sf0.1/trips_sample_pymeos.csv",
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
    mdtpart = MDTilingPartitioner(
        num_executors=4,
        d=3,
        moving_objects=testtrips
    )
    print(mdtpart.tilesstr)


if __name__ == '__main__':
    main()
