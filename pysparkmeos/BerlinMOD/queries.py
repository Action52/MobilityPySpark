querydesc1 = "Query 1: What are the models of the vehicles with licence plate numbers from QueryLicences?"
querytext1 = """
    SELECT l.licence, v.model
    FROM querylicences1 l, vehicles v
    WHERE l.licence = v.licence
"""

querydesc2 = "Query 2: How many vehicles exist that are 'passenger' cars?"
querytext2 = """
    SELECT COUNT(licence) AS PassengerCarCount
    FROM vehicles
    WHERE type='passenger'
"""

querydesc3 = "Query 3: Where have the vehicles with licences from QueryLicences1 been at each of the instants from QueryInstants1?"
querytext3 = """
    WITH
    veh_w_lic AS (
        SELECT v.vehid, l.licence, v.model
        FROM querylicences1 l, vehicles v
        WHERE l.licence = v.licence
    ),
    veh_trips AS (
        SELECT t.vehid, t.tileid, t.movingobjectid, t.movingobject, vw.licence, vw.model
        FROM veh_w_lic vw INNER JOIN trips t ON (t.vehid=vw.vehid)
    )
    SELECT vt.vehid, licence, i.instantid, tpoint_at(vt.movingobject, i.instant) AS pos
    FROM veh_trips vt RIGHT JOIN instants i ON i.tileid=vt.tileid
    WHERE vehid IS NOT NULL AND tpoint_at(vt.movingobject, i.instant) IS NOT NULL
    ORDER BY vehid, instantid
"""

querydesc4 = "Query 4: Which licence plate numbers belong to vehicles that have passed the points from QueryPoints?"
querytext4 = """
    WITH vehids_intersect AS (
        SELECT t.vehid
        FROM trips t INNER JOIN points p ON (t.tileid=p.tileid)
        WHERE 
            ever_intersects(t.movingobject, p.geom) = TRUE
    )
    SELECT vi.vehid, v.licence
    FROM vehids_intersect vi INNER JOIN vehicles v ON (vi.vehid=v.vehid)
"""

querydesc5 = "Query 5: What is the minimum distance between places, where a vehicle with a licence from QueryLicences1 and a vehicle with a licence from QueryLicences2 have been?"
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
        SELECT ql1vehids.licence, trips.movingobject, trips.movingobjectid, trips.vehid, trips.tileid
        FROM trips, ql1vehids
        WHERE trips.vehid = ql1vehids.vehid
    ),
    ql2trips AS (
        SELECT ql2vehids.licence, trips.movingobject, trips.movingobjectid, trips.vehid, trips.tileid
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
        MIN(min_dist)
    FROM distances
    GROUP BY t1licence, t2licence
"""

querydesc6 = "Query 6: What are the pairs of licence plate numbers of “trucks”, that have ever been as close as 10m or less to each other?"
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
            t1.vehid != t2.vehid AND t1.tileid = t2.tileid)
    WHERE nearest_approach_distance(t1.movingobject, t2.movingobject) BETWEEN 0.1 AND 10.1
    ORDER BY licence_pairs DESC
"""

querydesc11 = "Query 11: Query 11 Which vehicles passed a point from QueryPoints1 at one of the instants from QueryInstants1?"
querytext11 = """
    WITH atinstantspoints AS (
        SELECT t.vehid, p.pointid, i.instantid, t.movingobjectid, tpoint_at(t.movingobject, i.instant, TRUE) AS atinstant, t.tileid
        FROM trips t INNER JOIN instants i ON (t.tileid=i.tileid) INNER JOIN points p ON (t.tileid = p.tileid)
        WHERE 
            tpoint_at(t.movingobject, i.instant, TRUE) IS NOT NULL AND 
            tpoint_at(t.movingobject, i.instant, TRUE) = p.geom
    )
    SELECT instantid, pointid, collect_list(DISTINCT vehid) AS vehids
    FROM atinstantspoints
    GROUP BY instantid, pointid
    ORDER BY instantid, pointid
"""


querydesc12 = "Query 12: Which vehicles met at a point from QueryPoints1 at an instant from QueryInstants1?"
querytext12 = """
    WITH instants_ts AS (
        SELECT instants.instantid, timestamps(instants.instant)[0] AS ts, instants.tileid
        FROM instants
    ),
    atinstants AS (
        SELECT t.vehid, t.movingobjectid, explode(geometry_values(at_period(t.movingobject, i.ts))) AS atinstant, t.tileid
        FROM trips t INNER JOIN instants_ts i ON (t.tileid=i.tileid)
        WHERE at_period(t.movingobject, i.ts) IS NOT NULL
    )
    SELECT DISTINCT vehid
    FROM atinstants ai INNER JOIN points p ON (p.tileid = ai.tileid)
    WHERE ai.atinstant = p.geom
"""

querydesc13 = "Query 13: Which vehicles travelled within one of the regions from QueryRegions1 during the periods from QueryPeriods1?"
querytext13 = """
    WITH atperiods AS (
        SELECT t.vehid, p.periodid, t.movingobjectid, at_period(t.movingobject, p.period) AS atperiod, t.tileid
        FROM trips t RIGHT JOIN periods p ON (t.tileid=p.tileid)
    ),
    intersections AS (
        SELECT r.regionid, atp.periodid, atp.vehid, atp.movingobjectid, at_geom(atp.atperiod, r.geom) AS atgeom
        FROM atperiods atp RIGHT JOIN regions r ON (atp.tileid=r.tileid)
        WHERE at_geom(atp.atperiod, r.geom) IS NOT NULL
    )
    SELECT regionid, periodid, collect_list(DISTINCT vehid) AS vehids 
    FROM intersections
    GROUP BY regionid, periodid
    ORDER BY regionid, periodid
"""

querydesc15 = "Query 15: Which vehicles passed a point from QueryPoints1 during a period from QueryPeriods1?"
querytext15 = """
    WITH atperiods AS (
        SELECT t.vehid, t.movingobjectid, explode(geometry_values(at_period(t.movingobject, p.period))) AS atperiod, t.tileid
        FROM trips t INNER JOIN periods p ON (t.tileid=p.tileid)
        WHERE at_period(t.movingobject, p.period) IS NOT NULL
    )
    SELECT DISTINCT vehid 
    FROM atperiods ap INNER JOIN points po ON (ap.tileid = po.tileid)
    WHERE atperiod = po.geom
"""

## KNN Queries


querydesc18 = "Query 18: For each vehicle with a licence plate number from QueryLicences1 and each instant from QueryInstants1: Which are the 10 vehicles that have been closest to that vehicle at the given instant?"
querytext18 = """
    WITH trip_instants AS (
        SELECT t.vehid, t.tileid, t.movingobjectid, at_period(t.movingobject, timestamps(i.instant)[0]) AS tripatinstant
        FROM trips t INNER JOIN instants i ON (i.tileid = t.tileid)
        WHERE at_period(t.movingobject, timestamps(i.instant)[0]) IS NOT NULL
    ),
    distances AS (
        SELECT 
            ti1.vehid AS v1, ti2.vehid AS v2, ti1.movingobjectid AS mv1, ti2.movingobjectid AS mv2, 
            ti1.tripatinstant AS tai1, ti2.tripatinstant tai2, nearest_approach_distance(ti1.tripatinstant, ti2.tripatinstant) AS nad
        FROM trip_instants ti1 INNER JOIN trip_instants ti2 ON (
            ti1.vehid != ti2.vehid
        )
        WHERE
            nearest_approach_distance(ti1.tripatinstant, ti2.tripatinstant) != -1.0
    ),
    RankedDistances AS (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY v1 ORDER BY nad) as rank
        FROM distances
    )
    SELECT v1, v2, tai1, tai2, nad, rank
    FROM RankedDistances
    WHERE rank <= 10
    ORDER BY v1, rank   
"""


querydesc20 = "Query 20: For each region from QueryRegions1 and period from QueryPeriods1: What are the licences of the 10 vehicles that are closest to that region during the given observation period?"
querytext20 = """
    WITH trip_periods AS (
        SELECT 
            t.vehid, 
            t.tileid, 
            p.periodid,
            t.movingobjectid, 
            at_period(t.movingobject, p.period) AS tripatperiod
        FROM trips t LEFT JOIN periods p ON (t.tileid = p.tileid)
    ),
    distances AS (
        SELECT
            tp.tileid,
            vehid, 
            periodid, 
            regionid, 
            movingobjectid AS mvid, 
            geom,
            tripatperiod,
            nearest_approach_distance(tripatperiod, geom) AS nad
        FROM trip_periods tp LEFT JOIN regions r ON (tp.tileid = r.tileid)
    ),
    GroupedDistances AS (
        SELECT regionid, periodid, vehid, MIN(nad) AS nad
        FROM distances d
        GROUP BY regionid, periodid, vehid
        HAVING 
            nad IS NOT NULL AND 
            regionid IS NOT NULL AND
            periodid IS NOT NULL
        ORDER BY regionid, periodid, nad
    ),
    RankedDistances AS (
        SELECT gd.*, v.licence, RANK(gd.vehid) OVER (PARTITION BY gd.regionid, periodid ORDER BY nad) AS rank
        FROM GroupedDistances gd INNER JOIN vehicles v ON (gd.vehid = v.vehid)
    )
    SELECT * FROM RankedDistances
    WHERE rank <= 10
"""


#q13, q13stats, plan13 = query_exec(querytext13, spark, explain=True)
