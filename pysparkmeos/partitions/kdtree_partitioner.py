from pysparkmeos.partitions.mobility_partitioner import MobilityPartitioner
from pysparkmeos.utils.utils import new_bounds_from_axis, from_axis
from typing import *
from pymeos import *
from pymeos import TPoint


class KDTreePartition(MobilityPartitioner):
    """
    Class to partition data using a KDTree.
    """

    def __init__(
            self,
            moving_objects: List[TPoint],
            bounds: STBox,
            dimensions: tuple,
            max_depth=12
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
        self.grid = self._generate_grid(bounds, moving_objects, 0, dimensions, max_depth)
        self.numPartitions = len(self.grid)
        self.tilesstr = [tile.__str__() for tile in self.grid]
        super().__init__(self.numPartitions, self.get_partition)

    @staticmethod
    def _generate_grid(
        bounds: STBox,
        movingobjects: List[TPoint],
        depth: int = 0,
        dimensions: tuple = ("x", "y", "t"),
        max_depth=12,
    ):
        """
        Generates the tiles using a KDTree. Partitions using the median of the
        sorted TPoints within the space.

        :param bounds: STBox representing the initial bounds of the space.
        :param movingobjects: List of TPoints to create the partitions with.
        :param depth: Initial or current depth in the tree. Default=0 (root).
        :param dimensions: Dimensions to partition by. Default = x, y, and t.
        :param max_depth: Max depth of the tree. Default: 12.

        :return: A list of STBox representing the partition tiles.
        """
        axis = dimensions[depth % len(dimensions)]
        tiles = []

        pointsat = [point.at(bounds) for point in movingobjects]

        if type(pointsat[0]) == TGeomPointSeq:
            convert = [
                instant for point in pointsat for instant in point.instants()
            ]
            movingobjects = convert
        if type(pointsat[0]) == TGeomPointSeqSet:
            convert = [
                instant
                for seqset in pointsat
                for seq in seqset.sequences()
                for instant in seq.instants()
            ]
            movingobjects = convert

        # Sort the generator
        movingobjects = sorted(movingobjects, key=lambda p: from_axis(p, axis))
        num_points = len(movingobjects)

        if depth == max_depth or num_points <= 1:
            tiles.append(bounds)
        else:
            median = movingobjects[int(num_points / 2)]

            bboxleft = new_bounds_from_axis(bounds, axis, median, "left")
            bboxright = new_bounds_from_axis(bounds, axis, median, "right")

            # print("Median is ", median, " splitted by axis: ", axis)

            left = KDTreePartition._generate_grid(
                bboxleft,
                movingobjects[: int(num_points / 2)],
                depth + 1,
                dimensions,
                max_depth,
            )
            right = KDTreePartition._generate_grid(
                bboxright,
                movingobjects[int(num_points / 2) :],
                depth + 1,
                dimensions,
                max_depth,
            )
            tiles.extend(left)
            tiles.extend(right)
        return tiles

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
    print(bounds)
    kdpart = KDTreePartition(
        moving_objects=testtrips,
        dimensions=('x', 'y'),
        bounds=bounds,
        max_depth=3
    )
    print(kdpart.tilesstr)


if __name__ == '__main__':
    main()
