from pymeos import *
from typing import *

from pysparkmeos.partitions.mobilityrdd import MobilityPartitioner
from pysparkmeos.UDT.MeosDatatype import *

from pyspark.sql.types import *
from pyspark.sql import Row


class GridPartition(MobilityPartitioner):
    """
    Implements a regular grid partitioner in STBoxes of the same size per
    dimension.
    """

    def __init__(self, cells_per_side: int, bounds: STBox, geodetic=False):
        """
        Initialize the class.

        :param cells_per_side: Integer representing the number of partitions
        per side to create.
        For instance, if cells_per_side=3, the partitioner will split the
        bounds into 3 cells per dimension, resulting in d^3 partition tiles.
        :param bounds: The STBox representing the spatiotemporal space to
        partition.
        :param geodetic: True if data is geodetic, else False.
        """
        grid = [
            tile.set_srid(0)
            for tile in self._generate_grid(bounds, cells_per_side, geodetic=geodetic)
        ]
        self.tilesstr = [str(tile) for tile in grid]
        self.total_partitions = len(grid)
        super().__init__(self.total_partitions, self.get_partition)

    @staticmethod
    def _generate_grid(
        bounds: STBox, n_cells: int, geodetic: bool = True
    ) -> List[STBox]:
        """
        Creates the grid.

        :param bounds: STBox with the overall bounds of the space to partition.
        :param n_cells: Integer representing the number of cells to
            partition by dimension.
        :param geodetic: Boolean, True if data is geodetic, else False.
        :return: List of STBoxes representing the partition tiles.
        """
        xtilesize = (bounds.xmax() - bounds.xmin()) / n_cells
        xact = bounds.xmin()
        xtilebounds = []
        for n in range(n_cells):
            xtilebounds.append((xact, xact + xtilesize))
            xact += xtilesize

        ytilesize = (bounds.ymax() - bounds.ymin()) / n_cells
        yact = bounds.ymin()
        ytilebounds = []
        for n in range(n_cells):
            ytilebounds.append((yact, yact + ytilesize))
            yact += ytilesize

        ztilesize = (
            (bounds.zmax() - bounds.zmin()) / n_cells if bounds.has_z() else None
        )
        zact = bounds.zmin() if bounds.has_z() else None
        ztilebounds = [] if bounds.has_z() else None
        if bounds.has_z():
            for n in range(n_cells):
                ztilebounds.append((zact, zact + ztilesize))
                zact += ztilesize

        ttilesize = (
            bounds.to_tstzspan().duration() / n_cells if bounds.has_t() else None
        )
        tact = bounds.tmin() if bounds.has_t() else None
        ttilebounds = [] if bounds.has_t() else None
        if bounds.has_t():
            for n in range(n_cells):
                ttilebounds.append((tact, tact + ttilesize))
                tact += ttilesize

        tiles = None
        if bounds.has_t() and bounds.has_z():
            tiles = [
                STBox(
                    xmin=xi[0],
                    xmax=xi[1],
                    ymin=yi[0],
                    ymax=yi[1],
                    zmin=zi[0],
                    zmax=zi[1],
                    tmin=ti[0],
                    tmax=ti[1],
                    geodetic=geodetic,
                )
                for xi in xtilebounds
                for yi in ytilebounds
                for ti in ttilebounds
                for zi in ztilebounds
            ]
        if bounds.has_t() and not bounds.has_z():
            tiles = [
                STBox(
                    xmin=xi[0],
                    xmax=xi[1],
                    ymin=yi[0],
                    ymax=yi[1],
                    tmin=ti[0],
                    tmax=ti[1],
                    geodetic=geodetic,
                )
                for xi in xtilebounds
                for yi in ytilebounds
                for ti in ttilebounds
            ]
        if bounds.has_z() and not bounds.has_t():
            tiles = [
                STBox(
                    xmin=xi[0],
                    xmax=xi[1],
                    ymin=yi[0],
                    ymax=yi[1],
                    zmin=zi[0],
                    zmax=zi[1],
                    geodetic=geodetic,
                )
                for xi in xtilebounds
                for yi in ytilebounds
                for zi in ztilebounds
            ]
        return tiles

    @staticmethod
    def get_partition(point: Any, grid: list, utc_time: str = "UTC", **kwargs) -> int:
        pymeos_initialize()
        if type(point) == str:
            point = TGeogPointInst(point)
        # Little tweak to avoid pymeos bug
        point = point.set_srid(0)
        point = STBox(point.bounding_box().__str__().strip("GEOD"))
        for idx, partition in enumerate(grid):
            stbox: STBox = STBox(partition).set_srid(0)
            if point.is_contained_in(stbox):
                return idx
        return -1

    def num_partitions(self) -> int:
        """Return the total number of partitions."""
        return self.total_partitions


def main():
    """
    Simple test, outdated.
    TODO: UPDATE test.
    :return:
    """
    pymeos_initialize()

    bounds = STBox(
        "STBOX XT(((-177.02969360351562,-46.421356201171875),(177.816650390625,70.29727935791016)),[2022-06-27 00:00:00+00, 2022-06-27 00:15:00+00])",
        geodetic=True,
    )
    tpoint = TGeogPointInst(
        "POINT(40.87294006347656 1.9229736328125)@2022-06-27 00:00:00+00", srid=0
    )
    tpoint = tpoint.set_srid(0)

    gp = GridPartition(3, bounds)
    print("*******GLOBAL BOUNDS*******")
    print(bounds)
    print("***************************")
    print("***********POINT***********")
    print(tpoint)
    print("***************************")
    print("***********TILES***********")
    for tile in gp.gridstr:
        print(tile)
    print("***************************")
    print(gp.get_partition(tpoint, gp.gridstr))
    # gp.grid[0].plot_xt()

    tpoint.values()

    pymeos_finalize()


if __name__ == "__main__":
    main()
