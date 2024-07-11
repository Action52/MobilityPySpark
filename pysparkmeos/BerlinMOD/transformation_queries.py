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

transinstantssimple = """
    SELECT 
        instantid,
        -1 AS tileid,
        tboolinst_from_base_time(instant, TRUE) AS instant
    FROM instantsRawNoCache
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

transperiodsimple = """
                SELECT 
                    periodid,
                    beginp,
                    endp,
                    tstzspan(period) AS period,
                    -1 AS tileid
                FROM periodsRawNoCache
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

transpointssimple = """
                SELECT 
                    pointid, 
                    posx, 
                    posy, 
                    geometry_from_hexwkb(geom) AS geom,
                    -1 AS tileid
                FROM pointsRawNoCache
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

transregionssimple = """
                SELECT 
                    regionid, 
                    geometry_from_hexwkb(geom) AS geom,
                    -1 AS tileid
                FROM regionsRawNoCache
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

transtripssimple = """
    SELECT 
        tripid AS movingobjectid, 
        vehid, 
        day, 
        seqno, 
        sourcenode, 
        targetnode, 
        trip_from_hexwkb(trip) AS movingobject, 
        geometry_from_hexwkb(trajectory) AS trajectory, 
        licence,
        -1 AS tileid
    FROM tripsRawNoCache
"""