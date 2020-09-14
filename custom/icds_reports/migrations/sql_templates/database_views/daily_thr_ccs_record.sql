DROP VIEW IF EXISTS daily_thr_ccs_record_view CASCADE;
CREATE VIEW daily_thr_ccs_record_view AS
    SELECT
        "daily_ccs_record_thr"."doc_id" AS "doc_id",
        "ccs_record_monthly"."awc_id" AS "awc_id",
        "awc_location_months"."district_id" AS "district_id",
        "awc_location_months"."block_id" AS "block_id",
        "daily_ccs_record_thr"."state_id" AS "state_id",
        "daily_ccs_record_thr"."supervisor_id" AS "supervisor_id",
        "daily_ccs_record_thr"."month" AS "month",
        "daily_ccs_record_thr"."case_id" AS "case_id",
        "ccs_record_monthly"."person_name" AS "person_name",
        "daily_ccs_record_thr".latest_time_end_processed AS "submitted_on",
        "daily_ccs_record_thr".photo_thr_packets_distributed AS "photo_thr_packets_distributed"

    FROM "public"."icds_dashboard_daily_ccs_record_thr_forms" "daily_ccs_record_thr"
    inner join "public"."ccs_record_monthly" "ccs_record_monthly"  on (
            ("ccs_record_monthly"."case_id" = "daily_ccs_record_thr"."case_id") AND
            ("ccs_record_monthly"."month" = "daily_ccs_record_thr"."month") AND
            ("ccs_record_monthly"."supervisor_id" = "daily_ccs_record_thr"."supervisor_id")
    ) join "public"."awc_location_months" "awc_location_months" on (
        ("awc_location_months"."month" = "ccs_record_monthly"."month") AND
        ("awc_location_months"."awc_id" = "ccs_record_monthly"."awc_id")
    );
