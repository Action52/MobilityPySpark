from pysparkmeos.partitions.mobility_partitioner import MobilityPartitioner
from pysparkmeos.utils.utils import new_bounds_from_axis
from typing import *
from pymeos import *
from pymeos import TPoint


class KMeansPartition(MobilityPartitioner):
    """
    Class to partition data using a KMeans.
    """
    def __init__(
            self,
            k,
            movingobjects,
            utc
    ):
        self.grid = [
            tile for tile in self._generate_grid(
                movingobjects, k, utc
            )
        ]
        self.tilesstr = [tile.__str__() for tile in self.grid]
        self.numPartitions = len(self.tilesstr)
        super().__init__(self.numPartitions, self.get_partition)

    @staticmethod
    def _generate_grid(
            movingobjects,
            k,
            utc
    ):
        return []

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
    testtrips, bounds = gentestdata(read_as='TSeq')
    pass


if __name__ == '__main__':
    main()
