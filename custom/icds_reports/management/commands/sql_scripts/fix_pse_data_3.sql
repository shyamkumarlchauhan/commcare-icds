UPDATE child_health_monthly
SET
      lunch_count = NULL
WHERE month='{start_date}' AND pse_eligible <> 1
