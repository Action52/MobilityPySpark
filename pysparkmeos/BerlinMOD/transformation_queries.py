transinstants = """
    SELECT * 
    FROM InstantsUDTF(
        TABLE(
                SELECT 
                    instantid AS trajectory_id,
                    tboolinst_from_base_time(instant, TRUE) AS movingobject, 
                    (SELECT collect_list(tile) FROM grid) AS tiles, 
                    (SELECT collect_list(tileid) FROM grid) AS tileids
                FROM instantsRawNoCache
        )
    )
"""

transperiod = """
    SELECT * 
    FROM PeriodsUDTF(
        TABLE(
                SELECT 
                    periodid AS trajectory_id,
                    beginp,
                    endp,
                    tstzspan(period) AS movingobject,
                    (SELECT collect_list(tile) FROM grid) AS tiles, 
                    (SELECT collect_list(tileid) FROM grid) AS tileids
                FROM periodsRawNoCache
        )
    )
"""

transpoints = """
    SELECT * 
    FROM PointsUDTF(
        TABLE(
                SELECT 
                    pointid AS trajectory_id, 
                    posx, 
                    posy, 
                    geometry_from_hexwkb(geom) AS movingobject, 
                    (SELECT collect_list(tile) FROM grid) AS tiles, 
                    (SELECT collect_list(tileid) FROM grid) AS tileids
                FROM pointsRawNoCache
        )
    )
"""

transregions = """
    SELECT * 
    FROM RegionsUDTF(
        TABLE(
                SELECT 
                    regionid AS trajectory_id, 
                    geometry_from_hexwkb(geom) AS movingobject, 
                    (SELECT collect_list(tile) FROM grid) AS tiles, 
                    (SELECT collect_list(tileid) FROM grid) AS tileids
                FROM regionsRawNoCache
        )
    )
"""

transtrips = """
    SELECT 
        tripid, 
        vehid, 
        day, 
        seqno, 
        sourcenode, 
        targetnode, 
        trip_from_hexwkb(trip) AS trip, 
        geometry_from_hexwkb(trajectory) AS trajectory, 
        licence 
    FROM tripsRawNoCache
"""
