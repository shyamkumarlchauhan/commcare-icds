UPDATE
  agg_child_health
SET
  lunch_count_21_days = ut.lunch_count_21_days
FROM
  (
    SELECT
      month,
      supervisor_id,
      gender,
      age_tranche,
      SUM(lunch_count_21_days) as lunch_count_21_days
    FROM
      agg_child_health
      WHERE aggregation_level=5 AND month = '{month}' GROUP BY month, supervisor_id, gender, age_tranche
  ) ut
WHERE
  agg_child_health.month=ut.month
  AND agg_child_health.supervisor_id = ut.supervisor_id
  AND agg_child_health.gender = ut.gender
  AND agg_child_health.age_tranche = ut.age_tranche
  AND agg_child_health.aggregation_level=4
  AND agg_child_health.month = '{month}';

UPDATE
  agg_child_health
SET
  lunch_count_21_days = ut.lunch_count_21_days
FROM
  (
    SELECT
      month,
      block_id,
      gender,
      age_tranche,
      SUM(lunch_count_21_days) as lunch_count_21_days
    FROM
      agg_child_health
      WHERE aggregation_level=4 AND month = '{month}' GROUP BY month, block_id, gender, age_tranche
    ) ut
WHERE
    agg_child_health.month=ut.month
    AND agg_child_health.block_id = ut.block_id
    AND agg_child_health.gender = ut.gender
    AND agg_child_health.age_tranche = ut.age_tranche
    AND agg_child_health.aggregation_level=3
    AND agg_child_health.month = '{month}';

UPDATE
  agg_child_health
SET
  lunch_count_21_days = ut.lunch_count_21_days
FROM
  (
    SELECT
      month,
      district_id,
      gender,
      age_tranche,
      SUM(lunch_count_21_days) as lunch_count_21_days
    FROM
      WHERE aggregation_level=3 AND month = '{month}' GROUP BY month, district_id, gender, age_tranche
) ut
WHERE
    agg_child_health.month=ut.month
    AND agg_child_health.district_id = ut.district_id
    AND agg_child_health.gender = ut.gender
    AND agg_child_health.age_tranche = ut.age_tranche
    AND agg_child_health.aggregation_level=2;

UPDATE
  agg_child_health
SET
  lunch_count_21_days = ut.lunch_count_21_days
FROM
  (
    SELECT
      month,
      state_id,
      gender,
      age_tranche,
      SUM(lunch_count_21_days) as lunch_count_21_days
    FROM
      agg_child_health
      WHERE aggregation_level=2 AND month = '{month}' GROUP BY month, state_id, gender, age_tranche
  ) ut
WHERE
  agg_child_health.month=ut.month
  AND agg_child_health.state_id = ut.state_id
  AND agg_child_health.gender = ut.gender
  AND agg_child_health.age_tranche = ut.age_tranche
  AND agg_child_health.aggregation_level=1
