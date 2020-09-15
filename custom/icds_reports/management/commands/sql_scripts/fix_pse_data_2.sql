DROP TABLE IF EXISTS temp_chm;
CREATE UNLOGGED TABLE temp_chm As select state_id, awc_id, supervisor_id, month, gender, age_tranche, caste, lunch_count_21_days, disabled, minority, resident from agg_child_health where 1=0;
SELECT create_distributed_table('temp_chm', 'supervisor_id');
INSERT INTO "temp_chm" (
  state_id, awc_id, supervisor_id, month, gender, age_tranche, caste, lunch_count_21_days, disabled,
  minority, resident
) (
  SELECT
      chm.state_id, hm.awc_id, chm.supervisor_id, chm.month, chm.sex, chm.age_tranche, chm.caste,
      COUNT(*) FILTER (WHERE chm.lunch_count >= 21) as lunch_count_21_days,
      COALESCE(chm.disabled, 'no') as coalesce_disabled,
      COALESCE(chm.minority, 'no') as coalesce_minority,
      COALESCE(chm.resident, 'no') as coalesce_resident
    FROM
      "child_health_monthly" chm
    WHERE
      chm.month = '{start_date}' AND state_id='{state_id}'
    GROUP BY
      chm.awc_id,
      chm.supervisor_id,
      chm.month,
      chm.sex,
      chm.age_tranche,
      chm.caste,
      coalesce_disabled,
      coalesce_minority,
      coalesce_resident
);
DROP TABLE IF EXISTS temp_chm_local;
CREATE TABLE temp_chm_local AS SELECT * from temp_chm;
UPDATE agg_child_health
  SET
    lunch_count_21_days = ut.lunch_count_21_days
  FROM temp_chm_local ut
  WHERE (
    agg_child_health.state_id = ut.state_id AND
    agg_child_health.supervisor_id = ut.supervisor_id AND
    agg_child_health.awc_id=ut.awc_id and
    agg_child_health.month=ut.month and
    agg_child_health.gender=ut.gender and
    agg_child_health.age_tranche=ut.age_tranche and
    agg_child_health.caste=ut.caste and
    agg_child_health.disabled=ut.disabled and
    agg_child_health.minority=ut.minority and
    agg_child_health.resident=ut.resident and
    agg_child_health.aggregation_level = 5 AND
    agg_child_health.month='{month}' AND
    agg_child_health.state_id='{state_id}'
  )

