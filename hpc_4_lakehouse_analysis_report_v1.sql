-- This query retrieves individual cost fact records from lakefact_x, along with their associated plan, year, field --cluster, and global cluster names, ordered by year, plan, global cluster, and field cluster.

select
	f.id as factId,
	f.sourceid as sourceid,
	p.name AS plan,
    y.name AS year,
    fc.name AS field_cluster,
    gc.name AS global_cluster,    
    f.valueNum AS total_cost
FROM lake.lakefact_x f
LEFT JOIN lake.lakedim_fieldcluster fc ON f.fieldClusterId = fc.id
LEFT JOIN lake.lakedim_globalcluster gc ON f.globalClusterId = gc.id
LEFT JOIN lake.lakedim_plan p ON f.planId = p.id
LEFT JOIN lake.lakedim_year y ON f.yearId = y.id
WHERE f.factType = 'Cost'
ORDER BY y.name, p.name,  gc.name, fc.name;

-- This query will return one row per combination of year and global cluster, with the total aggregated cost.
SELECT
    y.name AS year,
    gc.name AS global_cluster,
    SUM(f.valueNum) AS total_cost
FROM lake.lakefact_x f
LEFT JOIN lake.lakedim_globalcluster gc ON f.globalClusterId = gc.id
LEFT JOIN lake.lakedim_year y ON f.yearId = y.id
WHERE f.factType = 'Cost'
GROUP BY y.name, gc.name
ORDER BY y.name, gc.name;

-- Sum of all cost by cluster
SELECT   
    gc.name AS global_cluster,
    SUM(f.valueNum) AS total_cost
FROM lake.lakefact_x f
LEFT JOIN lake.lakedim_globalcluster gc ON f.globalClusterId = gc.id
LEFT JOIN lake.lakedim_year y ON f.yearId = y.id
WHERE f.factType = 'Cost'
GROUP BY gc.name
ORDER BY total_cost DESC;

