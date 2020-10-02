-- EXPLANATION of the QUERY
-- - JOIN
--     agg_child_health is joined to get the two fields (height_eligible, height_measured_in_month)
--     which are not there in the agg_awc
--     join is done on the three fields
--         - month, aggregation_level, state_id (because it's a monthly aggregated table)' ||
--     Where clauses
--         - aggregation_level=1 (we need total numbers so we can take the topmost aggregation level state level so that no of rows would be less)
--         - month (month range)
--         - state_is_test (remove test states)
--     GROUP BY
--         - month (we need monthly numbers)
SELECT
    "agg_awc".month,
    sum("agg_awc".num_launched_awcs) as num_launched_awcs_sum,
    sum("agg_awc".cases_child_health) + sum("agg_awc".cases_ccs_pregnant) + sum("agg_awc".cases_ccs_lactating) as no_of_beneficiary,
    SUM("agg_child_health"."height_eligible") AS "height_eligible",
    SUM("agg_child_health"."height_measured_in_month") AS "height_measured_in_month",
    sum("agg_awc".wer_weighed) as weighed,
    sum("agg_awc".wer_eligible) as weight_eligible,
    sum("agg_awc".num_awcs_conducted_cbe) as awcs_conducted_cbe,
    sum("agg_awc".num_awcs_conducted_vhnd) as awcs_conducted_vhnd,
    sum("agg_awc".valid_visits) as valid_visits,
    sum("agg_awc".expected_visits) as expected_visits,
    sum("agg_awc".num_mother_thr_21_days) + sum("agg_awc".thr_rations_21_plus_distributed_child) as  thr_given_21_days,
    sum("agg_awc".num_mother_thr_eligible) + sum("agg_awc".thr_eligible_child) as thr_eligible
    FROM "public"."agg_awc" "agg_awc"
    LEFT JOIN agg_child_health on (
        ("agg_child_health"."month" = "agg_awc"."month") AND
        ("agg_child_health"."state_id" = "agg_awc"."state_id") AND
        ("agg_child_health"."aggregation_level" = "agg_awc"."aggregation_level") AND
        ("agg_child_health"."aggregation_level" = 1)
    )
    WHERE "agg_awc".aggregation_level=1 AND "agg_awc".month>='2020-01-01' AND "agg_awc".month<'2020-09-01'
    AND "agg_awc".state_is_test <> 1
    GROUP BY "agg_awc".month
    
    
-- 
-- ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--  Finalize GroupAggregate  (cost=216993.93..217006.45 rows=65 width=100)
--    Group Key: agg_awc_1.month
--    ->  Gather Merge  (cost=216993.93..217002.43 rows=64 width=132)
--          Workers Planned: 4
--          ->  Partial GroupAggregate  (cost=215993.87..215994.75 rows=16 width=132)
--                Group Key: agg_awc_1.month
--                ->  Sort  (cost=215993.87..215993.91 rows=16 width=68)
--                      Sort Key: agg_awc_1.month
--                      ->  Parallel Hash Left Join  (cost=215941.99..215993.55 rows=16 width=68)
--                            Hash Cond: ((agg_awc_1.aggregation_level = "agg_child_health_2020-07-01".aggregation_level) AND (agg_awc_1.month = "agg_child_health_2020-07-01".month) AND (agg_awc_1.state_id = "agg_child_health_2020-07-01".state_id))
--                            ->  Parallel Append  (cost=0.00..25.29 rows=14 width=97)
--                                  ->  Parallel Seq Scan on "agg_awc_2020-01-01_1" agg_awc_1  (cost=0.00..5.39 rows=17 width=97)
--                                        Filter: ((month >= '2020-01-01'::date) AND (month < '2020-09-01'::date) AND (state_is_test <> 1) AND (aggregation_level = 1))
--                                  ->  Parallel Seq Scan on "agg_awc_2020-02-01_1" agg_awc_2  (cost=0.00..5.39 rows=17 width=97)
--                                        Filter: ((month >= '2020-01-01'::date) AND (month < '2020-09-01'::date) AND (state_is_test <> 1) AND (aggregation_level = 1))
--                                  ->  Parallel Seq Scan on "agg_awc_2020-04-01_1" agg_awc_4  (cost=0.00..2.41 rows=1 width=96)
--                                        Filter: ((month >= '2020-01-01'::date) AND (month < '2020-09-01'::date) AND (state_is_test <> 1) AND (aggregation_level = 1))
--                                  ->  Parallel Seq Scan on "agg_awc_2020-05-01_1" agg_awc_5  (cost=0.00..2.41 rows=1 width=96)
--                                        Filter: ((month >= '2020-01-01'::date) AND (month < '2020-09-01'::date) AND (state_is_test <> 1) AND (aggregation_level = 1))
--                                  ->  Parallel Seq Scan on "agg_awc_2020-06-01_1" agg_awc_6  (cost=0.00..2.41 rows=1 width=96)
--                                        Filter: ((month >= '2020-01-01'::date) AND (month < '2020-09-01'::date) AND (state_is_test <> 1) AND (aggregation_level = 1))
--                                  ->  Parallel Seq Scan on "agg_awc_2020-07-01_1" agg_awc_7  (cost=0.00..2.41 rows=1 width=96)
--                                        Filter: ((month >= '2020-01-01'::date) AND (month < '2020-09-01'::date) AND (state_is_test <> 1) AND (aggregation_level = 1))
--                                  ->  Parallel Seq Scan on "agg_awc_2020-08-01_1" agg_awc_8  (cost=0.00..2.41 rows=1 width=96)
--                                        Filter: ((month >= '2020-01-01'::date) AND (month < '2020-09-01'::date) AND (state_is_test <> 1) AND (aggregation_level = 1))
--                                  ->  Parallel Seq Scan on "agg_awc_2020-03-01_1" agg_awc_3  (cost=0.00..2.39 rows=1 width=96)
--                                        Filter: ((month >= '2020-01-01'::date) AND (month < '2020-09-01'::date) AND (state_is_test <> 1) AND (aggregation_level = 1))
--                                  ->  Parallel Seq Scan on agg_awc  (cost=0.00..0.00 rows=1 width=96)
--                                        Filter: ((month >= '2020-01-01'::date) AND (month < '2020-09-01'::date) AND (state_is_test <> 1) AND (aggregation_level = 1))
--                            ->  Parallel Hash  (cost=215854.91..215854.91 rows=4976 width=49)
--                                  ->  Parallel Append  (cost=0.00..215854.91 rows=4976 width=49)
--                                        ->  Index Scan using staging_agg_child_health_aggregation_level_age_tranche_idx19 on "agg_child_health_2020-07-01"  (cost=0.44..457.50 rows=655 width=49)
--                                              Index Cond: (aggregation_level = 1)
--                                        ->  Index Scan using staging_agg_child_health_aggregation_level_age_tranche_idx22 on "agg_child_health_2020-09-01"  (cost=0.44..361.09 rows=519 width=49)
--                                              Index Cond: (aggregation_level = 1)
--                                        ->  Seq Scan on agg_child_health  (cost=0.00..0.00 rows=1 width=48)
--                                              Filter: (aggregation_level = 1)
--                                        ->  Parallel Index Scan using staging_agg_child_health_aggregation_level_age_tranche_idx4 on "agg_child_health_2019-12-01"  (cost=0.56..652.34 rows=537 width=49)
--                                              Index Cond: (aggregation_level = 1)
--                                        ->  Parallel Index Scan using staging_agg_child_health_aggregation_level_age_tranche_idx on "agg_child_health_2019-11-01"  (cost=0.56..152.77 rows=152 width=49)
--                                              Index Cond: (aggregation_level = 1)
--                                        ->  Parallel Index Scan using "agg_child_health_2019-09-01_idx_3" on "agg_child_health_2019-09-01"  (cost=0.56..360.44 rows=294 width=49)
--                                              Index Cond: (aggregation_level = 1)
--                                        ->  Parallel Index Scan using "agg_child_health_2019-08-01_idx_3" on "agg_child_health_2019-08-01"  (cost=0.56..278.52 rows=218 width=49)
--                                              Index Cond: (aggregation_level = 1)
--                                        ->  Parallel Index Scan using staging_agg_child_health_aggregation_level_state_id_idx6 on "agg_child_health_2020-01-01"  (cost=0.56..2.29 rows=1 width=49)
--                                              Index Cond: (aggregation_level = 1)
--                                        ->  Parallel Index Scan using staging_agg_child_health_aggregation_level_age_tranche_idx8 on "agg_child_health_2020-02-01"  (cost=0.44..2.03 rows=1 width=49)
--                                              Index Cond: (aggregation_level = 1)
--                                        ->  Parallel Index Scan using staging_agg_child_health_aggregation_level_age_tranche_idx11 on "agg_child_health_2020-03-01"  (cost=0.44..2.03 rows=1 width=49)
--                                              Index Cond: (aggregation_level = 1)
--                                        ->  Parallel Index Scan using staging_agg_child_health_aggregation_level_age_tranche_idx13 on "agg_child_health_2020-04-01"  (cost=0.44..2.03 rows=1 width=49)
--                                              Index Cond: (aggregation_level = 1)
--                                        ->  Parallel Index Scan using staging_agg_child_health_aggregation_level_gender_idx15 on "agg_child_health_2020-05-01"  (cost=0.44..2.03 rows=1 width=49)
--                                              Index Cond: (aggregation_level = 1)
--                                        ->  Parallel Index Scan using staging_agg_child_health_aggregation_level_age_tranche_idx16 on "agg_child_health_2020-06-01"  (cost=0.44..2.03 rows=1 width=49)
--                                              Index Cond: (aggregation_level = 1)
--                                        ->  Parallel Index Scan using staging_agg_child_health_aggregation_level_age_tranche_idx20 on "agg_child_health_2020-08-01"  (cost=0.44..2.03 rows=1 width=49)
--                                              Index Cond: (aggregation_level = 1)
--                                        ->  Parallel Index Scan using staging_agg_child_health_aggregation_level_gender_idx2 on "agg_child_health_2019-10-01"  (cost=0.43..342.18 rows=278 width=49)
--                                              Index Cond: (aggregation_level = 1)
--                                        ->  Parallel Seq Scan on "agg_child_health_2017-04-01"  (cost=0.00..103918.90 rows=626 width=49)
--                                              Filter: (aggregation_level = 1)
--                                        ->  Parallel Seq Scan on "agg_child_health_2017-03-01"  (cost=0.00..83848.79 rows=584 width=49)
--                                              Filter: (aggregation_level = 1)
--                                        ->  Parallel Seq Scan on "agg_child_health_2017-02-01"  (cost=0.00..18848.20 rows=523 width=49)
--                                              Filter: (aggregation_level = 1)
--                                        ->  Parallel Seq Scan on "agg_child_health_2017-01-01"  (cost=0.00..3523.21 rows=464 width=49)
--                                              Filter: (aggregation_level = 1)
--                                        ->  Parallel Seq Scan on "agg_child_health_2016-11-01"  (cost=0.00..1201.19 rows=331 width=49)
--                                              Filter: (aggregation_level = 1)
--                                        ->  Parallel Seq Scan on "agg_child_health_2016-12-01"  (cost=0.00..1156.60 rows=286 width=49)
--                                              Filter: (aggregation_level = 1)
--                                        ->  Parallel Seq Scan on "agg_child_health_2016-10-01"  (cost=0.00..377.01 rows=238 width=49)
--                                              Filter: (aggregation_level = 1)
--                                        ->  Parallel Seq Scan on "agg_child_health_2019-06-01_1"  (cost=0.00..46.43 rows=275 width=49)
--                                              Filter: (aggregation_level = 1)
--                                        ->  Parallel Seq Scan on "agg_child_health_2019-07-01_1"  (cost=0.00..44.46 rows=277 width=49)
--                                              Filter: (aggregation_level = 1)
--                                        ->  Parallel Seq Scan on "agg_child_health_2019-05-01_1"  (cost=0.00..41.29 rows=263 width=49)
--                                              Filter: (aggregation_level = 1)
--                                        ->  Parallel Seq Scan on "agg_child_health_2019-04-01_1"  (cost=0.00..39.05 rows=244 width=49)
--                                              Filter: (aggregation_level = 1)
--                                        ->  Parallel Seq Scan on "agg_child_health_2019-02-01_1"  (cost=0.00..35.77 rows=222 width=49)
--                                              Filter: (aggregation_level = 1)
--                                        ->  Parallel Seq Scan on "agg_child_health_2019-03-01_1"  (cost=0.00..34.71 rows=217 width=49)
--                                              Filter: (aggregation_level = 1)
--                                        ->  Parallel Seq Scan on "agg_child_health_2019-01-01_1"  (cost=0.00..30.37 rows=189 width=49)
--                                              Filter: (aggregation_level = 1)
--                                        ->  Parallel Seq Scan on "agg_child_health_2018-12-01_1"  (cost=0.00..30.35 rows=188 width=49)
--                                              Filter: (aggregation_level = 1)
--                                        ->  Parallel Seq Scan on "agg_child_health_2018-03-01_1"  (cost=0.00..28.60 rows=128 width=49)
--                                              Filter: (aggregation_level = 1)
--                                        ->  Parallel Seq Scan on "agg_child_health_2018-02-01_1"  (cost=0.00..25.48 rows=118 width=49)
--                                              Filter: (aggregation_level = 1)
--                                        ->  Parallel Seq Scan on "agg_child_health_2018-01-01_1"  (cost=0.00..25.47 rows=118 width=49)
--                                              Filter: (aggregation_level = 1)
--                                        ->  Parallel Seq Scan on "agg_child_health_2018-11-01_1"  (cost=0.00..23.85 rows=148 width=49)
--                                              Filter: (aggregation_level = 1)
--                                        ->  Parallel Seq Scan on "agg_child_health_2018-08-01_1"  (cost=0.00..22.82 rows=146 width=49)
--                                              Filter: (aggregation_level = 1)
--                                        ->  Parallel Seq Scan on "agg_child_health_2018-06-01_1"  (cost=0.00..21.71 rows=136 width=49)
--                                              Filter: (aggregation_level = 1)
--                                        ->  Parallel Seq Scan on "agg_child_health_2018-05-01_1"  (cost=0.00..21.68 rows=135 width=49)
--                                              Filter: (aggregation_level = 1)
--                                        ->  Parallel Seq Scan on "agg_child_health_2018-10-01_1"  (cost=0.00..21.68 rows=135 width=49)
--                                              Filter: (aggregation_level = 1)
--                                        ->  Parallel Seq Scan on "agg_child_health_2018-04-01_1"  (cost=0.00..21.68 rows=135 width=49)
--                                              Filter: (aggregation_level = 1)
--                                        ->  Parallel Seq Scan on "agg_child_health_2018-07-01_1"  (cost=0.00..21.68 rows=134 width=49)
--                                              Filter: (aggregation_level = 1)
--                                        ->  Parallel Seq Scan on "agg_child_health_2018-09-01_1"  (cost=0.00..20.57 rows=126 width=49)
--                                              Filter: (aggregation_level = 1)
--                                        ->  Parallel Seq Scan on "agg_child_health_2017-12-01_1"  (cost=0.00..18.45 rows=116 width=49)
--                                              Filter: (aggregation_level = 1)
--                                        ->  Parallel Seq Scan on "agg_child_health_2017-07-01_1"  (cost=0.00..18.43 rows=114 width=49)
--                                              Filter: (aggregation_level = 1)
--                                        ->  Parallel Seq Scan on "agg_child_health_2017-06-01_1"  (cost=0.00..17.42 rows=114 width=49)
--                                              Filter: (aggregation_level = 1)
--                                        ->  Parallel Seq Scan on "agg_child_health_2017-11-01_1"  (cost=0.00..17.42 rows=114 width=49)
--                                              Filter: (aggregation_level = 1)
--                                        ->  Parallel Seq Scan on "agg_child_health_2017-08-01_1"  (cost=0.00..17.40 rows=112 width=49)
--                                              Filter: (aggregation_level = 1)
--                                        ->  Parallel Seq Scan on "agg_child_health_2017-09-01_1"  (cost=0.00..17.39 rows=111 width=49)
--                                              Filter: (aggregation_level = 1)
--                                        ->  Parallel Seq Scan on "agg_child_health_2017-10-01_1"  (cost=0.00..17.39 rows=111 width=49)
--                                              Filter: (aggregation_level = 1)
--                                        ->  Parallel Seq Scan on "agg_child_health_2017-05-01_1"  (cost=0.00..16.32 rows=106 width=49)
--                                              Filter: (aggregation_level = 1)



