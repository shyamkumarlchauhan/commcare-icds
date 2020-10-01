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
    GROUP BY "agg_awc".month
