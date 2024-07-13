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

transtrips2 = """
    WITH instants AS (
        SELECT 
            vehid,
            tripid,
            seqno,
            tgeompointinst(
                        geometry_from_hexwkb(point), 
                        tboolinst_from_base_time(date_trunc("second", t), TRUE)
            ) AS tpoint,
            t
        FROM tripsRawNoCache
    )
    SELECT 
        tripid, 
        vehid,
        NULL AS day,
        seqno,
        NULL AS sourcenode,
        NULL AS targetnode,
        tgeompointseq_from_tpoint_list(collect_set(tpoint)) AS trip,
        NULL AS trajectory,
        NULL AS licence
    FROM instants
    GROUP BY vehid, tripid, seqno
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

transtripssimple2 = """
    WITH instants AS (
        SELECT 
            vehid,
            tripid,
            seqno,
            tgeompointinst(
                        geometry_from_hexwkb(point), 
                        tboolinst_from_base_time(date_trunc("second", t), TRUE)
            ) AS tpoint,
            t
        FROM tripsRawNoCache
    )
    SELECT 
        tripid AS movingobjectid, 
        vehid, 
        NULL AS day, 
        seqno, 
        NULL AS sourcenode, 
        NULL AS targetnode, 
        tgeompointseq_from_tpoint_list(collect_set(tpoint)) AS movingobject, 
        NULL AS trajectory, 
        NULL AS licence,
        -1 AS tileid
    FROM instants
    GROUP BY vehid, tripid, seqno
"""

transmunsimple = """
    SELECT 
        municipalityid,
        name,
        geometry_from_wkt(geom) AS geom,
        -1 AS tileid
        FROM municipalitiesRawNoCache
"""

transmun = """
    SELECT * 
    FROM MunicipalitiesUDTF(
        TABLE(
                SELECT 
                    name,
                    municipalityid AS trajectory_id,
                    geometry_from_wkt(geom) AS movingobject, 
                    (SELECT collect_list(tile) FROM grid) AS tiles, 
                    (SELECT collect_list(tileid) FROM grid) AS tileids
                FROM municipalitiesRawNoCache
        )
    )
"""
