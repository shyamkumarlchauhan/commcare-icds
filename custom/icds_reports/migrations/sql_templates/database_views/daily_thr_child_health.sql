DROP VIEW IF EXISTS daily_thr_child_health_view CASCADE;
CREATE VIEW daily_thr_child_health_view AS
    SELECT
        "daily_child_health_thr"."doc_id" AS "doc_id",
        "child_health"."awc_id" AS "awc_id",
        "awc_location_months"."district_id" AS "district_id",
        "awc_location_months"."block_id" AS "block_id",
        "daily_child_health_thr"."state_id" AS "state_id",
        "daily_child_health_thr"."supervisor_id" AS "supervisor_id",
        "daily_child_health_thr"."month" AS "month",
        "daily_child_health_thr"."case_id" AS "case_id",
        "child_health"."person_name" AS "person_name",
        "daily_child_health_thr".latest_time_end_processed AS "submitted_on",
        "daily_child_health_thr".photo_thr_packets_distributed AS "photo_thr_packets_distributed"

    FROM "public"."icds_dashboard_daily_child_health_thr_forms" "daily_child_health_thr"
    inner join "public"."child_health_monthly" "child_health"  on (
            ("child_health"."case_id" = "daily_child_health_thr"."case_id") AND
            ("child_health"."month" = "daily_child_health_thr"."month") AND
            ("child_health"."supervisor_id" = "daily_child_health_thr"."supervisor_id")
    ) join "public"."awc_location_months" "awc_location_months" on (
        ("awc_location_months"."month" = "child_health"."month") AND
        ("awc_location_months"."awc_id" = "child_health"."awc_id")
    );
