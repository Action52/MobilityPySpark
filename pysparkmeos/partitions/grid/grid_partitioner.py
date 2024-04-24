from math import floor

from pymeos import TPoint, STBox
from pymeos import STBox, pymeos_initialize, pymeos_finalize, TGeogPointInst
from pymeos_cffi.functions import stbox_tile_list
import logging
from typing import Tuple

from pysparkmeos.partitions.partition import MobilityPartition
import numpy as np
from shapely.geometry import Point


class GridPartition(MobilityPartition):
    def __init__(self, cells_per_side: int, bounds: STBox):
        self.bounds = bounds
        self.grid = self._generate_grid(bounds, cells_per_side)
        self.gridstr = [str(tile) for tile in self.grid]
        self.total_partitions = len(self.grid)

    @staticmethod
    def _generate_grid2(bounds: STBox, n_cells):
        xtilesize = (bounds.xmax() - bounds.xmin()) / (n_cells + 1)
        xstart = bounds.xmin()
        xtilebounds = [()]

        ytilesize = (bounds.ymax() - bounds.ymin()) / (n_cells + 1)
        ystart = bounds.ymin()

        ztilesize = (bounds.zmax() - bounds.zmin()) / (n_cells + 1) if bounds.has_z() else None
        zstart = bounds.zmin() if bounds.has_z() else None

        ttilesize = bounds.to_tstzspan().duration() / (n_cells + 1) if bounds.has_t() else None
        tstart = bounds.tmin() if bounds.has_t() else None


        xtilebounds = []
    @staticmethod
    def _generate_grid(bounds, n_cells):
        xinc = (bounds.xmax() - bounds.xmin()) / n_cells + 1
        xstart = bounds.xmin() - 1
        xint = [(x, x + xinc) for x in
                range(int(floor(xstart)), int(bounds.xmax() + 1), int(xinc))]

        yinc = (bounds.ymax() - bounds.ymin()) / n_cells + 1
        ystart = bounds.ymin() - 1
        yint = [(y, y + yinc) for y in
                range(int(floor(ystart)), int(bounds.ymax() + 1), int(yinc))]

        zinc = zstart = zint = None
        if bounds.has_z():
            zinc = (bounds.zmax() - bounds.zmin()) / n_cells + 1
            zstart = bounds.zmin() - 1
            zint = [(z, z + zinc) for z in
                    range(int(floor(zstart)), int(bounds.zmax() + 1), int(zinc))]

        tinc = tstart = tint = None
        if bounds.has_t():
            tinc = bounds.to_tstzspan().duration() / n_cells  # Time increment
            tstart = bounds.tmin()  # Starting time
            tint = [(tact, tact + tinc) for tact in
                    [tstart + i * tinc for i in range(n_cells)]]

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
                    tmax=ti[1])
                for xi in xint for yi in yint for ti in tint for zi in zint
            ]
        if bounds.has_t() and not bounds.has_z():
            tiles = [
                STBox(
                    xmin=xi[0],
                    xmax=xi[1],
                    ymin=yi[0],
                    ymax=yi[1],
                    tmin=ti[0],
                    tmax=ti[1])
                for xi in xint for yi in yint for ti in tint
            ]
        if bounds.has_z() and not bounds.has_t():
            tiles = [
                STBox(
                    xmin=xi[0],
                    xmax=xi[1],
                    ymin=yi[0],
                    ymax=yi[1],
                    zmin=zi[0],
                    zmax=zi[1])
                for xi in xint for yi in yint for zi in zint
            ]
        return tiles

    @staticmethod
    def get_partition(value: Tuple, utc_time="UTC", **kwargs) -> int:
        logging.debug(value)
        key, point, grid = value
        point = TGeogPointInst(point)
        # Little tweak to avoid pymeos bug
        point = point.set_srid(0)
        point = STBox(point.bounding_box().__str__().strip("GEOD"))
        for idx, partition in enumerate(grid):
            stbox: STBox = STBox(partition).set_srid(0)
            if point.is_contained_in(stbox):
                return idx
        return 1  # Return a special index if no suitable partition is found

    def num_partitions(self) -> int:
        return self.total_partitions


def main():
    pymeos_initialize()

    bounds = STBox(
        "STBOX XT(((-177.02969360351562,-46.421356201171875),(177.816650390625,70.29727935791016)),[2022-06-27 00:00:00+00, 2022-06-27 00:15:00+00])",
        geodetic=True)
    tpoint = TGeogPointInst(
        "POINT(40.87294006347656 1.9229736328125)@2022-06-27 00:00:00+00",
        srid=0)
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
    print(gp.get_partition((tpoint.__str__(), tpoint.__str__(), gp.gridstr)))

    gp.grid[0].plot_xt()

    pymeos_finalize()


if __name__ == "__main__":
    main()
