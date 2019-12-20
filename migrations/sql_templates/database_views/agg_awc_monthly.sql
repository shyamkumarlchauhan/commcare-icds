DROP VIEW IF EXISTS agg_awc_monthly CASCADE;
CREATE VIEW agg_awc_monthly AS
    SELECT
        "awc_location_months"."awc_id" AS "awc_id",
        "awc_location_months"."awc_name" AS "awc_name",
        "awc_location_months"."awc_site_code" AS "awc_site_code",
        "awc_location_months"."supervisor_id" AS "supervisor_id",
        "awc_location_months"."supervisor_name" AS "supervisor_name",
        "awc_location_months"."supervisor_site_code" AS "supervisor_site_code",
        "awc_location_months"."block_id" AS "block_id",
        "awc_location_months"."block_name" AS "block_name",
        "awc_location_months"."block_site_code" AS "block_site_code",
        "awc_location_months"."district_id" AS "district_id",
        "awc_location_months"."district_name" AS "district_name",
        "awc_location_months"."district_site_code" AS "district_site_code",
        "awc_location_months"."state_id" AS "state_id",
        "awc_location_months"."state_name" AS "state_name",
        "awc_location_months"."state_site_code" AS "state_site_code",
        "awc_location_months"."aggregation_level" AS "aggregation_level",
        "awc_location_months"."block_map_location_name" AS "block_map_location_name",
        "awc_location_months"."district_map_location_name" AS "district_map_location_name",
        "awc_location_months"."state_map_location_name" AS "state_map_location_name",
        "awc_location_months"."month" AS "month",
        "awc_location_months"."aww_name" AS "aww_name",
        "awc_location_months"."contact_phone_number" AS "contact_phone_number",
        "agg_awc"."is_launched" AS "is_launched",
        "agg_awc"."num_awcs" AS "num_awcs",
        "agg_awc"."num_launched_states" AS "num_launched_states",
        "agg_awc"."num_launched_districts" AS "num_launched_districts",
        "agg_awc"."num_launched_blocks" AS "num_launched_blocks",
        "agg_awc"."num_launched_supervisors" AS "num_launched_supervisors",
        "agg_awc"."num_launched_awcs" AS "num_launched_awcs",
        "agg_awc"."awc_days_open" AS "awc_days_open",
        "agg_awc"."awc_days_pse_conducted" AS "awc_days_pse_conducted",
        "agg_awc"."awc_num_open" AS "awc_num_open",
        "agg_awc"."wer_weighed" AS "wer_weighed",
        "agg_awc"."wer_weighed_0_2" AS "wer_weighed_0_2",
        "agg_awc"."wer_eligible_0_2" AS "wer_eligible_0_2",
        "agg_awc"."wer_eligible" AS "wer_eligible",
        "agg_awc"."num_anc_visits" AS "num_anc_visits",
        "agg_awc"."num_children_immunized" AS "num_children_immunized",
        COALESCE("agg_awc"."cases_household", 0) AS "cases_household",
        COALESCE("agg_awc"."cases_person", 0) AS "cases_person",
        COALESCE("agg_awc"."cases_person_all", 0) AS "cases_person_all",
        COALESCE("agg_awc"."cases_person_adolescent_girls_11_14", 0) AS "cases_person_adolescent_girls_11_14",
        COALESCE("agg_awc"."cases_person_adolescent_girls_15_18", 0) AS "cases_person_adolescent_girls_15_18",
        COALESCE("agg_awc"."cases_person_adolescent_girls_11_14_all", 0) AS "cases_person_adolescent_girls_11_14_all",
        COALESCE("agg_awc"."cases_person_adolescent_girls_15_18_all", 0) AS "cases_person_adolescent_girls_15_18_all",
        COALESCE("agg_awc"."cases_person_adolescent_girls_11_14_out_of_school", 0) AS "cases_person_adolescent_girls_11_14_out_of_school",
        COALESCE("agg_awc"."cases_person_adolescent_girls_11_14_all_v2", 0) AS "cases_person_adolescent_girls_11_14_all_v2",
        COALESCE("agg_awc"."cases_person_referred", 0) AS "cases_person_referred",
        COALESCE("agg_awc"."cases_ccs_pregnant", 0) AS "cases_ccs_pregnant",
        COALESCE("agg_awc"."cases_ccs_lactating", 0) AS "cases_ccs_lactating",
        COALESCE("agg_awc"."cases_child_health", 0) AS "cases_child_health",
        COALESCE("agg_awc"."cases_ccs_pregnant_all", 0) AS "cases_ccs_pregnant_all",
        COALESCE("agg_awc"."cases_ccs_lactating_all", 0) AS "cases_ccs_lactating_all",
        COALESCE("agg_awc"."cases_child_health_all", 0) AS "cases_child_health_all",
        COALESCE("agg_awc"."usage_num_pse", 0) AS "usage_num_pse",
        COALESCE("agg_awc"."usage_num_gmp", 0) AS "usage_num_gmp",
        COALESCE("agg_awc"."usage_num_thr", 0) AS "usage_num_thr",
        COALESCE("agg_awc"."usage_num_home_visit", 0) AS "usage_num_home_visit",
        COALESCE("agg_awc"."usage_num_bp_tri1", 0) AS "usage_num_bp_tri1",
        COALESCE("agg_awc"."usage_num_bp_tri2", 0) AS "usage_num_bp_tri2",
        COALESCE("agg_awc"."usage_num_bp_tri3", 0) AS "usage_num_bp_tri3",
        COALESCE("agg_awc"."usage_num_pnc", 0) AS "usage_num_pnc",
        COALESCE("agg_awc"."usage_num_ebf", 0) AS "usage_num_ebf",
        COALESCE("agg_awc"."usage_num_cf", 0) AS "usage_num_cf",
        COALESCE("agg_awc"."usage_num_delivery", 0) AS "usage_num_delivery",
        COALESCE("agg_awc"."usage_num_due_list_ccs", 0) AS "usage_num_due_list_ccs",
        COALESCE("agg_awc"."usage_num_due_list_child_health", 0) AS "usage_num_due_list_child_health",
        COALESCE("agg_awc"."usage_awc_num_active", 0) AS "usage_awc_num_active",
        "agg_awc"."infra_last_update_date" AS "infra_last_update_date",
        "agg_awc"."infra_type_of_building" AS "infra_type_of_building",
        "agg_awc"."infra_clean_water" AS "infra_clean_water",
        "agg_awc"."infra_functional_toilet" AS "infra_functional_toilet",
        "agg_awc"."infra_baby_weighing_scale" AS "infra_baby_weighing_scale",
        "agg_awc"."infra_flat_weighing_scale" AS "infra_flat_weighing_scale",
        "agg_awc"."infra_adult_weighing_scale" AS "infra_adult_weighing_scale",
        "agg_awc"."infra_infant_weighing_scale" AS "infra_infant_weighing_scale",
        "agg_awc"."infra_cooking_utensils" AS "infra_cooking_utensils",
        "agg_awc"."infra_medicine_kits" AS "infra_medicine_kits",
        "agg_awc"."infra_adequate_space_pse" AS "infra_adequate_space_pse",
        "agg_awc"."num_awc_infra_last_update" AS "num_awc_infra_last_update",
        COALESCE("agg_awc"."usage_num_hh_reg", 0) AS "usage_num_hh_reg",
        COALESCE("agg_awc"."usage_num_add_person", 0) AS "usage_num_add_person",
        COALESCE("agg_awc"."usage_num_add_pregnancy", 0) AS "usage_num_add_pregnancy",
        COALESCE("agg_awc"."cases_person_has_aadhaar_v2", 0) AS "cases_person_has_aadhaar_v2",
        COALESCE("agg_awc"."cases_person_beneficiary_v2", 0) AS "cases_person_beneficiary_v2",
        "agg_awc"."infantometer" AS "infantometer",
        "agg_awc"."stadiometer" AS "stadiometer"
    FROM "public"."awc_location_months_local" "awc_location_months"
    LEFT JOIN "public"."agg_awc" "agg_awc" ON (
        ("awc_location_months"."month" = "agg_awc"."month") AND
        ("awc_location_months"."aggregation_level" = "agg_awc"."aggregation_level") AND
        ("awc_location_months"."state_id" = "agg_awc"."state_id") AND
        ("awc_location_months"."district_id" = "agg_awc"."district_id") AND
        ("awc_location_months"."block_id" = "agg_awc"."block_id") AND
        ("awc_location_months"."supervisor_id" = "agg_awc"."supervisor_id") AND
        ("awc_location_months"."awc_id" = "agg_awc"."awc_id")
    );
