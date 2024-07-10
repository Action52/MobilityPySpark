querytext1 = """
    SELECT l.licence, v.model
    FROM licences l, vehicles v
    WHERE l.licence = v.licence
"""

querytext2 = """
    SELECT COUNT(licence) AS PassengerCarCount
    FROM vehicles
    WHERE type='passenger'
"""

querytext3 = """
    WITH
    veh_w_lic AS (
        SELECT v.vehid, l.licence, v.model
        FROM licences l, vehicles v
        WHERE l.licence = v.licence
    ),
    veh_trips AS (
        SELECT t.* 
        FROM veh_w_lic vw, trips t
        WHERE t.vehid = vw.vehid
    )
    SELECT vt.vehid, vt.movingobjectid, vt.movingobject, i.instant, tpoint_at(vt.movingobject, i.instant) AS pos
    FROM veh_trips vt INNER JOIN instants i ON i.tileid=vt.tileid
    WHERE tpoint_at(vt.movingobject, i.instant) IS NOT NULL
"""

querytext4 = """
    WITH

    vehids_intersect AS (
        SELECT t.vehid
        FROM trips t INNER JOIN points p ON (t.tileid=p.tileid)
        WHERE 
            ever_intersects(t.movingobject, p.geom) = TRUE
    )
    SELECT vi.vehid, v.licence
    FROM vehids_intersect vi INNER JOIN vehicles v ON (vi.vehid=v.vehid)
"""

querytext5 = """
    WITH ql1vehids AS (
        SELECT vehicles.licence, vehicles.vehid 
        FROM vehicles, querylicences1
        WHERE vehicles.licence = querylicences1.licence   
    ),
    ql2vehids AS (
        SELECT vehicles.licence, vehicles.vehid 
        FROM vehicles, querylicences2
        WHERE vehicles.licence = querylicences2.licence   
    ),
    ql1trips AS (
        SELECT ql1vehids.licence, trips.* 
        FROM trips, ql1vehids
        WHERE trips.vehid = ql1vehids.vehid
    ),
    ql2trips AS (
        SELECT ql2vehids.licence, trips.*
        FROM trips, ql2vehids
        WHERE trips.vehid = ql2vehids.vehid
    ),
    distances AS (
        SELECT
            t1.licence AS t1licence,
            t2.licence AS t2licence,
            t1.movingobjectid AS t1tripid,
            t2.movingobjectid AS t2tripid,
            t1.tileid AS t1tileid,
            t2.tileid AS t2tileid,
            min_distance(t1.movingobject, t2.movingobject) AS min_dist
        FROM 
            ql1trips t1 INNER JOIN ql2trips t2 ON (
                t1.vehid != t2.vehid AND 
                t1.movingobjectid != t2.movingobjectid AND
                min_distance(t1.movingobject, t2.movingobject) IS NOT NULL
            )
    )
    SELECT 
        t1licence,
        t2licence,
        t1tripid,
        t2tripid,
        MIN(min_dist)
    FROM distances
    GROUP BY t1licence, t2licence, t1tripid, t2tripid
"""

querytext6 = """
    WITH trucks AS (
        SELECT vehid, licence
        FROM vehicles
        WHERE type='truck'
    ),
    trucktrips AS (
        SELECT truck.vehid, truck.licence, trips.movingobject, trips.tileid, trips.movingobjectid
        FROM trucks truck INNER JOIN trips ON (truck.vehid=trips.vehid)
    )
    SELECT t1.licence, t2.licence, t1.movingobjectid, t2.movingobjectid, t1.tileid, t2.tileid, nearest_approach_distance(t1.movingobject, t2.movingobject) AS licence_pairs
    FROM 
        trucktrips t1 INNER JOIN trucktrips t2 ON (
            t1.vehid != t2.vehid)
    WHERE nearest_approach_distance(t1.movingobject, t2.movingobject) BETWEEN 0.1 AND 10.1
    ORDER BY licence_pairs DESC
"""

querytext11 = """
    WITH atinstants AS (
        SELECT t.vehid, t.movingobjectid, tpoint_at(t.movingobject, i.instant, TRUE) AS atinstant, t.tileid
        FROM trips t INNER JOIN instants i ON (t.tileid=i.tileid)
        WHERE tpoint_at(t.movingobject, i.instant, TRUE) IS NOT NULL
    ),
    atpoints AS (
        SELECT t.vehid, t.movingobjectid, ever_intersects(t.movingobject, ati.atinstant) AS atpoint, t.tileid
        FROM trips t INNER JOIN atinstants ati ON (t.tileid=ati.tileid)
        WHERE ever_intersects(t.movingobject, ati.atinstant) = TRUE
    )
    SELECT DISTINCT vehid FROM atpoints
"""

querytext13 = """
    WITH atperiods AS (
        SELECT t.vehid, t.movingobjectid, at_period(t.movingobject, p.period) AS atperiod, t.tileid
        FROM trips t INNER JOIN periods p ON (t.tileid=p.tileid)
        WHERE at_period(t.movingobject, p.period) IS NOT NULL
    ),
    intersections AS (
        SELECT atp.vehid, atp.movingobjectid, at_geom(atp.atperiod, r.geom) AS atgeom
        FROM atperiods atp INNER JOIN regions r ON (atp.tileid=r.tileid)
        WHERE at_geom(atp.atperiod, r.geom) IS NOT NULL
    )
    SELECT DISTINCT vehid FROM intersections
"""