DROP VIEW IF EXISTS inactive_aww_view CASCADE;
CREATE VIEW inactive_aww_view AS
    SELECT
        "loc"."doc_id" AS "awc_id",
        "loc"."awc_name" AS "awc_name",
        "loc"."awc_site_code" AS "awc_site_code",
        "loc"."supervisor_id" AS "supervisor_id",
        "loc"."supervisor_name" AS "supervisor_name",
        "loc"."block_id" AS "block_id",
        "loc"."block_name" AS "block_name",
        "loc"."district_id" AS "district_id",
        "loc"."district_name" AS "district_name",
        "loc"."state_id" AS "state_id",
        "loc"."state_name" AS "state_name",
        "aww"."first_submission" AS "first_submission",
        "aww"."last_submission" AS "last_submission",
        "aww"."no_of_days_since_start" AS "no_of_days_since_start",
        "aww"."no_of_days_inactive" AS "no_of_days_inactive"
    FROM "public"."icds_reports_aggregateinactiveaww" "aww"
    LEFT OUTER JOIN "public"."awc_location_local" "loc" ON (
        ("loc"."doc_id" = "aww"."awc_id")
    ) WHERE "aww"."awc_id" IS NULL AND "loc"."doc_id" != 'All'
