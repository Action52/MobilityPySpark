parttrips = """
    SELECT * 
    FROM TripsUDTF(
        TABLE(
                SELECT 
                    tripid AS trajectory_id,
                    vehid,
                    day,
                    seqno,
                    sourcenode,
                    targetnode,
                    trip AS movingobject,
                    trajectory,
                    licence,
                    (SELECT collect_list(tile) FROM grid) AS tiles, 
                    (SELECT collect_list(tileid) FROM grid) AS tileids
                FROM tripsRaw
        )
    )
"""
