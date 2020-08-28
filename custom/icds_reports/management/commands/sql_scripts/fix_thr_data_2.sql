UPDATE agg_child_health
    SET
        rations_21_plus_distributed = ut.rations_21_plus_distributed,
        days_ration_given_child = ut.days_ration_given_child
from (
    SELECT
        month,
        supervisor_id,
        gender,
        age_tranche,
        sum(rations_21_plus_distributed) as rations_21_plus_distributed,
        sum(days_ration_given_child) as days_ration_given_child
    from agg_child_health
        WHERE aggregation_level=5 AND month = '{month}' GROUP BY month, supervisor_id, gender, age_tranche
) ut
    WHERE
        agg_child_health.month=ut.month AND
        agg_child_health.supervisor_id=ut.supervisor_id AND
        agg_child_health.gender=ut.gender AND
        agg_child_health.age_tranche=ut.age_tranche AND
        agg_child_health.aggregation_level=4 AND
        agg_child_health.month = '{month}';


UPDATE agg_child_health
    SET
        rations_21_plus_distributed = ut.rations_21_plus_distributed,
        days_ration_given_child = ut.days_ration_given_child
from (
    SELECT
        month,
        block_id,
        gender,
        age_tranche,
        sum(rations_21_plus_distributed) as rations_21_plus_distributed,
        sum(days_ration_given_child) as days_ration_given_child
    from agg_child_health
        WHERE aggregation_level=4 AND month = '{month}' GROUP BY month, block_id, gender, age_tranche
) ut
    WHERE
        agg_child_health.month=ut.month AND
        agg_child_health.block_id=ut.block_id AND
        agg_child_health.gender=ut.gender AND
        agg_child_health.age_tranche=ut.age_tranche AND
        agg_child_health.aggregation_level=3 AND
        agg_child_health.month = '{month}';


UPDATE agg_child_health
    SET
        rations_21_plus_distributed = ut.rations_21_plus_distributed,
        days_ration_given_child = ut.days_ration_given_child
from (
    SELECT
        month,
        district_id,
        gender,
        age_tranche,
        sum(rations_21_plus_distributed) as rations_21_plus_distributed,
        sum(days_ration_given_child) as days_ration_given_child
    from agg_child_health
        WHERE aggregation_level=3 AND month = '{month}' GROUP BY month, district_id, gender, age_tranche
) ut
    WHERE
        agg_child_health.month=ut.month AND
        agg_child_health.district_id=ut.district_id AND
        agg_child_health.gender=ut.gender AND
        agg_child_health.age_tranche=ut.age_tranche AND
        agg_child_health.aggregation_level=2;


UPDATE agg_child_health
    SET
        rations_21_plus_distributed = ut.rations_21_plus_distributed,
        days_ration_given_child = ut.days_ration_given_child
from (
    SELECT
        month,
        state_id,
        gender,
        age_tranche,
        sum(rations_21_plus_distributed) as rations_21_plus_distributed,
        sum(days_ration_given_child) as days_ration_given_child
    from agg_child_health
        WHERE aggregation_level=2 AND month = '{month}' GROUP BY month, state_id, gender, age_tranche
) ut
    WHERE
        agg_child_health.month=ut.month AND
        agg_child_health.state_id=ut.state_id AND
        agg_child_health.gender=ut.gender AND
        agg_child_health.age_tranche=ut.age_tranche AND
        agg_child_health.aggregation_level=1
