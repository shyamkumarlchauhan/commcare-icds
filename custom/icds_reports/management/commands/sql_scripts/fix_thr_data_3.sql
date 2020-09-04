UPDATE child_health_monthly
    set num_rations_distributed = CASE WHEN thr_eligible=1 THEN COALESCE(thr.days_ration_given_child, 0) ELSE NULL END,
        days_ration_given_child = thr.days_ration_given_child
FROM (
    SELECT DISTINCT ON (child_health_case_id)
            LAST_VALUE(supervisor_id) over w AS supervisor_id,
            '{month}'::date AS month,
            child_health_case_id AS case_id,
            MAX(timeend) over w AS latest_time_end_processed,
            CASE WHEN SUM(days_ration_given_child) over w > 32767 THEN 32767 ELSE SUM(days_ration_given_child) over w END AS days_ration_given_child
          FROM "ucr_icds-cas_static-dashboard_thr_forms_b8bca6ea"
          WHERE state_id = {state_id} AND timeend >= '{month}' AND timeend < '{next_month}' AND
                child_health_case_id IS NOT NULL
    WINDOW w AS (PARTITION BY supervisor_id, child_health_case_id)
    ) thr
    where
    child_health_monthly.month = thr.month AND
    child_health_monthly.case_id = thr.case_id AND
    child_health_monthly.supervisor_id = thr.supervisor_id AND
    child_health_monthly.state_id = thr.state_id AND
    child_health_monthly.month = '{month}';
