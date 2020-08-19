SELECT
    state_name,
    SUM(CASE WHEN age_tranche in ('36', '48') THEN pse_eligible ELSE 0 END) as pse_eligible_3_4,
    SUM(CASE WHEN age_tranche = '60' THEN pse_eligible ELSE 0 END) as pse_eligible_4_5,
    SUM(CASE WHEN age_tranche = '72' THEN pse_eligible ELSE 0 END) as pse_eligible_5_6
FROM agg_child_health
left join awc_location_local ON (
agg_child_health.state_id=awc_location_local.state_id AND
agg_child_health.aggregation_level=awc_location_local.aggregation_level AND
agg_child_health.aggregation_level=1
)
where agg_child_health.aggregation_level=1 and month='2020-07-01' and agg_child_health.state_is_test<>1
group by state_name;
/*
 GroupAggregate  (cost=22.15..22.21 rows=2 width=34)
   Group Key: awc_location_local.state_name
   ->  Sort  (cost=22.15..22.16 rows=2 width=31)
         Sort Key: awc_location_local.state_name
         ->  Hash Right Join  (cost=2.51..22.14 rows=2 width=31)
               Hash Cond: ((awc_location_local.aggregation_level = agg_child_health.aggregation_level) AND (awc_location_local.state_id = agg_child_health.state_id))
               Join Filter: (agg_child_health.aggregation_level = 1)
               ->  Index Scan using awc_location_local_aggregation_level_idx on awc_location_local  (cost=0.42..19.73 rows=41 width=47)
                     Index Cond: (aggregation_level = 1)
               ->  Hash  (cost=2.05..2.05 rows=2 width=58)
                     ->  Append  (cost=0.00..2.05 rows=2 width=58)
                           ->  Seq Scan on agg_child_health  (cost=0.00..0.00 rows=1 width=72)
                                 Filter: ((state_is_test <> 1) AND (aggregation_level = 1) AND (month = '2020-07-01'::date))
                           ->  Index Scan using staging_agg_child_health_aggregation_level_age_tranche_idx18 on "agg_child_health_2020-07-01"  (cost=0.44..2.04 rows=1 width=43)
                                 Index Cond: (aggregation_level = 1)
                                 Filter: ((state_is_test <> 1) AND (month = '2020-07-01'::date))
(16 rows)
*/





SELECT
    state_name,
    SUM(CASE WHEN caste='st' AND pregnant=1 THEN 1 else 0 END) as pregnant_st,
    SUM(CASE WHEN caste='sc' AND pregnant=1 THEN 1 else 0 END) as pregnant_sc,
    SUM(CASE WHEN caste='st' AND lactating=1 THEN 1 else 0 END) as lactating_st,
    SUM(CASE WHEN caste='sc' AND lactating=1 THEN 1 else 0 END) as lactating_sc
FROM ccs_record_monthly
inner join awc_location on (
ccs_record_monthly.supervisor_id=awc_location.supervisor_id AND
ccs_record_monthly.awc_id = awc_location.doc_id AND
awc_location.aggregation_level=5
)
WHERE awc_location.state_is_test<>1 and month='2020-07-01'
group by state_name;

/*

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 HashAggregate  (cost=0.00..0.00 rows=0 width=0)
   Group Key: remote_scan.state_name
   ->  Custom Scan (Citus Real-Time)  (cost=0.00..0.00 rows=0 width=0)
         Task Count: 64
         Tasks Shown: One of 64
         ->  Task
               Node: host=100.71.184.232 port=6432 dbname=icds_ucr
               ->  GroupAggregate  (cost=211212.04..211212.26 rows=5 width=42)
                     Group Key: awc_location.state_name
                     ->  Sort  (cost=211212.04..211212.05 rows=5 width=22)
                           Sort Key: awc_location.state_name
                           ->  Gather  (cost=1001.11..211211.98 rows=5 width=22)
                                 Workers Planned: 5
                                 ->  Nested Loop  (cost=1.10..210211.48 rows=1 width=22)
                                       ->  Parallel Index Scan using crm_supervisor_person_month_idx_102712 on ccs_record_monthly_102712 ccs_record_monthly  (cost=0.56..173777.23 rows=24945 width=78)
                                             Index Cond: (month = '2020-07-01'::date)
                                       ->  Index Scan using awc_location_indx6_102840 on awc_location_102840 awc_location  (cost=0.55..1.45 rows=1 width=73)
                                             Index Cond: (doc_id = ccs_record_monthly.awc_id)
                                             Filter: ((state_is_test <> 1) AND (aggregation_level = 5) AND (ccs_record_monthly.supervisor_id = supervisor_id))
(19 rows)
*/






SELECT
    state_name,
    SUM(CASE WHEN caste='st' AND valid_in_month=1 THEN 1 else 0 END) as children_st,
    SUM(CASE WHEN caste='sc' AND valid_in_month=1 THEN 1 else 0 END) as children_sc
FROM child_health_monthly
inner join  awc_location on (
child_health_monthly.supervisor_id=awc_location.supervisor_id AND
child_health_monthly.awc_id = awc_location.doc_id AND
awc_location.aggregation_level=5
)
WHERE awc_location.state_is_test<>1 and month='2020-07-01' and child_health_monthly.valid_in_month=1
group by state_name;
/*
                                                                                      QUERY PLAN
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 HashAggregate  (cost=0.00..0.00 rows=0 width=0)
   Group Key: remote_scan.state_name
   ->  Custom Scan (Citus Real-Time)  (cost=0.00..0.00 rows=0 width=0)
         Task Count: 64
         Tasks Shown: One of 64
         ->  Task
               Node: host=100.71.184.232 port=6432 dbname=icds_ucr
               ->  Finalize GroupAggregate  (cost=231436.44..231441.06 rows=33 width=26)
                     Group Key: awc_location.state_name
                     ->  Gather Merge  (cost=231436.44..231440.49 rows=32 width=26)
                           Workers Planned: 4
                           ->  Partial GroupAggregate  (cost=230436.38..230436.62 rows=8 width=26)
                                 Group Key: awc_location.state_name
                                 ->  Sort  (cost=230436.38..230436.40 rows=8 width=18)
                                       Sort Key: awc_location.state_name
                                       ->  Nested Loop  (cost=0.55..230436.26 rows=8 width=18)
                                             ->  Parallel Append  (cost=0.00..76409.85 rows=211939 width=74)
                                                   ->  Parallel Seq Scan on "child_health_monthly_2020-07-01_1159420" child_health_monthly  (cost=0.00..75350.15 rows=211939 width=74)
                                                         Filter: ((month = '2020-07-01'::date) AND (valid_in_month = 1))
                                             ->  Index Scan using awc_location_indx6_102840 on awc_location_102840 awc_location  (cost=0.55..0.72 rows=1 width=73)
                                                   Index Cond: (doc_id = child_health_monthly.awc_id)
                                                   Filter: ((state_is_test <> 1) AND (aggregation_level = 5) AND (child_health_monthly.supervisor_id = supervisor_id))
(22 rows)


*/
