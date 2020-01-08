DROP VIEW IF EXISTS gov_vhnd_view CASCADE;
CREATE VIEW gov_vhnd_view AS
SELECT
"awc_location_local"."state_name" AS "state_name",
"awc_location_local"."district_name" AS "district_name",
"awc_location_local"."block_name" AS "block_name",
"awc_location_local"."supervisor_name" AS "supervisor_name",
"awc_location_local"."awc_name" AS "awc_name",
agg_awc.month as month,
COALESCE("icds_dashboard_gov_vhnd_forms"."vhsnd_date_past_month",0) as "vhsnd_date_past_month"
COALESCE("icds_dashboard_gov_vhnd_forms"."anm_mpw_present",0) as "anm_mpw_present"
COALESCE("icds_dashboard_gov_vhnd_forms"."asha_present",0) as "asha_present"
COALESCE("icds_dashboard_gov_vhnd_forms"."child_immu",0) as "child_immu"
COALESCE("icds_dashboard_gov_vhnd_forms"."anc_today",0) as "anc_today"

FROM "awc_local_local"

LEFT join agg_awc on (
        ("awc_local_local"."awc_id" = "agg_awc"."awc_id") and
        ("agg_awc"."is_launched" = 'yes') and
        ("agg_awc"."aggregation_level" = 5)
        )

LEFT join icds_dashboard_gov_vhnd_forms on (
    ("icds_dashboard_gov_vhnd_forms"."awc_id" = "agg_awc"."awc_id")
)

where awc_location_local.state_is_test<>1 and awc_location_local.district_is_test<>1 and
 awc_location_local.block_is_test<>1 and awc_location_local.supervisor_is_test<>1 and awc_location_local.awc_is_test<>1
 and awc_location_local.aggregation_level = 5;
