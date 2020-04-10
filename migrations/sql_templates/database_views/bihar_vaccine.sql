DROP VIEW IF EXISTS bihar_vaacine_view CASCADE;
CREATE VIEW bihar_vaacine_view AS
    SELECT
        "bihar_vaccine"."month" AS "month",
        "bihar_vaccine"."case_id" AS "person_id",
        "bihar_vaccine"."time_birth" AS "time_birth",
        "bihar_vaccine"."child_alive" AS "child_alive",
        "bihar_vaccine"."father_name" AS "father_name",
        "bihar_vaccine"."mother_name" AS "mother_name",
        "bihar_vaccine"."father_id" AS "father_id",
        "bihar_vaccine"."mother_id" AS "mother_id",
        "bihar_vaccine"."dob" AS "dob",
        "child_vaccines"."due_list_date_1g_dpt_1" as "due_list_date_1g_dpt_1",
        "child_vaccines"."due_list_date_2g_dpt_2" as "due_list_date_2g_dpt_2",
        "child_vaccines"."due_list_date_3g_dpt_3" as "due_list_date_3g_dpt_3",
        "child_vaccines"."due_list_date_5g_dpt_booster" as "due_list_date_5g_dpt_booster",
        "child_vaccines"."due_list_date_7gdpt_booster_2" as "due_list_date_7gdpt_booster_2",
        "child_vaccines"."due_list_date_0g_hep_b_0" as "due_list_date_0g_hep_b_0",
        "child_vaccines"."due_list_date_1g_hep_b_1" as "due_list_date_1g_hep_b_1",
        "child_vaccines"."due_list_date_2g_hep_b_2" as "due_list_date_2g_hep_b_2",
        "child_vaccines"."due_list_date_3g_hep_b_3" as "due_list_date_3g_hep_b_3",
        "child_vaccines"."due_list_date_3g_ipv" as "due_list_date_3g_ipv",
        "child_vaccines"."due_list_date_4g_je_1" as "due_list_date_4g_je_1",
        "child_vaccines"."due_list_date_5g_je_2" as "due_list_date_5g_je_2",
        "child_vaccines"."due_list_date_5g_measles_booster" as "due_list_date_5g_measles_booster",
        "child_vaccines"."due_list_date_4g_measles" as "due_list_date_4g_measles",
        "child_vaccines"."due_list_date_0g_opv_0" as "due_list_date_0g_opv_0",
        "child_vaccines"."due_list_date_1g_opv_1" as "due_list_date_1g_opv_1",
        "child_vaccines"."due_list_date_2g_opv_2" as "due_list_date_2g_opv_2",
        "child_vaccines"."due_list_date_3g_opv_3" as "due_list_date_3g_opv_3",
        "child_vaccines"."due_list_date_5g_opv_booster" as "due_list_date_5g_opv_booster",
        "child_vaccines"."due_list_date_1g_penta_1" as "due_list_date_1g_penta_1",
        "child_vaccines"."due_list_date_2g_penta_2" as "due_list_date_2g_penta_2",
        "child_vaccines"."due_list_date_3g_penta_3" as "due_list_date_3g_penta_3",
        "child_vaccines"."due_list_date_1g_rv_1" as "due_list_date_1g_rv_1",
        "child_vaccines"."due_list_date_2g_rv_2" as "due_list_date_2g_rv_2",
        "child_vaccines"."due_list_date_3g_rv_3" as "due_list_date_3g_rv_3",
        "child_vaccines"."due_list_date_4g_vit_a_1" as "due_list_date_4g_vit_a_1",
        "child_vaccines"."due_list_date_5g_vit_a_2" as "due_list_date_5g_vit_a_2",
        "child_vaccines"."due_list_date_6g_vit_a_3" as "due_list_date_6g_vit_a_3",
        "child_vaccines"."due_list_date_6g_vit_a_4" as "due_list_date_6g_vit_a_4",
        "child_vaccines"."due_list_date_6g_vit_a_5" as "due_list_date_6g_vit_a_5",
        "child_vaccines"."due_list_date_6g_vit_a_6" as "due_list_date_6g_vit_a_6",
        "child_vaccines"."due_list_date_6g_vit_a_7" as "due_list_date_6g_vit_a_7",
        "child_vaccines"."due_list_date_6g_vit_a_8" as "due_list_date_6g_vit_a_8",
        "child_vaccines"."due_list_date_7g_vit_a_9" as "due_list_date_7g_vit_a_9",
        "child_vaccines"."due_list_date_1g_bcg" as "due_list_date_1g_bcg"

    FROM "public"."bihar_api_child_vaccine" "bihar_vaccine"
    LEFT JOIN "public"."child_vaccines" "child_vaccines"
    ON (
        ("child_vaccines"."child_health_case_id" = "bihar_vaccine"."case_id")
    );
