from math import floor

from pymeos import TPoint, STBox
from pymeos import STBox, pymeos_initialize, pymeos_finalize, TGeogPointInst
from pymeos_cffi.functions import stbox_tile_list
import logging
from typing import Tuple, Any, Callable

from pysparkmeos.partitions.mobilityrdd import MobilityPartitioner
import numpy as np
from shapely.geometry import Point
from pysparkmeos.UDT.MeosDatatype import *
from pyspark.sql.functions import udf, udtf, lit
from pyspark.sql.types import *


class GridPartition(MobilityPartitioner):
    def __init__(self, cells_per_side: int, bounds: STBox, geodetic=False):
        grid = [tile.set_srid(0) for tile in self._generate_grid(bounds, cells_per_side, geodetic=geodetic)]
        self.gridstr = [str(tile) for tile in grid]
        self.total_partitions = len(grid)
        super().__init__(self.total_partitions, self.get_partition)

    def as_spark_table(self):
        gridstr = self.gridstr  # Do this so that spark doesn't break
        return GridSparkCreator(lit(gridstr))

    @staticmethod
    def _generate_grid(bounds: STBox, n_cells, geodetic=True):
        xtilesize = (bounds.xmax() - bounds.xmin()) / (n_cells)
        xact = bounds.xmin()
        xtilebounds = []
        for n in range(n_cells):
            xtilebounds.append((xact, xact+xtilesize))
            xact+=xtilesize

        ytilesize = (bounds.ymax() - bounds.ymin()) / (n_cells)
        yact = bounds.ymin()
        ytilebounds = []
        for n in range(n_cells):
            ytilebounds.append((yact, yact+ytilesize))
            yact+=ytilesize

        ztilesize = (bounds.zmax() - bounds.zmin()) / (n_cells) if bounds.has_z() else None
        zact = bounds.zmin() if bounds.has_z() else None
        ztilebounds = [] if bounds.has_z() else None
        if bounds.has_z():
            for n in range(n_cells):
                ztilebounds.append((zact, zact+ztilesize))
                zact+=ztilesize

        ttilesize = bounds.to_tstzspan().duration() / (n_cells) if bounds.has_t() else None
        tact = bounds.tmin() if bounds.has_t() else None
        ttilebounds = [] if bounds.has_t() else None
        if bounds.has_t():
            for n in range(n_cells):
                ttilebounds.append((tact, tact+ttilesize))
                tact+=ttilesize

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
                    geodetic=geodetic
                )
                for xi in xtilebounds for yi in ytilebounds for ti in ttilebounds for zi in ztilebounds
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
                    geodetic=geodetic
                )
                for xi in xtilebounds for yi in ytilebounds for ti in ttilebounds
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
                    geodetic=geodetic
                )
                for xi in xtilebounds for yi in ytilebounds for zi in ztilebounds
            ]
        return tiles

    @staticmethod
    def _generate_grid2(bounds, n_cells, geodetic=True):
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
                    tmax=ti[1],
                    geodetic=geodetic
                )
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
                    tmax=ti[1],
                    geodetic=geodetic
                )
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
                    zmax=zi[1],
                    geodetic=geodetic
                )
                for xi in xint for yi in yint for zi in zint
            ]
        return tiles

    @staticmethod
    def get_partition(
            point: Any,
            grid: list,
            utc_time: str = "UTC",
            **kwargs
    ) -> int:
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
        return self.numPartitions


@udtf(returnType=StructType([
    StructField("tileid", IntegerType()),
    StructField("tile", STBoxUDT())
]))
class GridSparkCreator:
    def eval(self, gridstr):
        pymeos_initialize()
        for tileid, tile in enumerate(gridstr):
            yield tileid, STBox(tile)


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
    print(gp.get_partition(tpoint, gp.gridstr))
    # gp.grid[0].plot_xt()

    tpoint.values()

    pymeos_finalize()


if __name__ == "__main__":
    main()
