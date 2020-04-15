from custom.icds_reports.models.views import BiharDemographicsView, BiharVaccineView
from custom.icds_reports.const import CAS_API_PAGE_SIZE
from custom.icds_reports.cache import icds_quickcache


# cache for 2 hours because it wont change atleast for 24 hours.
# This API will be hit in a loop of something and they should be able to scrape all
# the records in 2 hours.
@icds_quickcache(['month', 'state_id'], timeout=60 * 60 * 2)
def get_total_records_count(month, state_id):
    return BiharDemographicsView.objects.filter(
        month=month,
        state_id=state_id
    ).count()


@icds_quickcache(['month', 'state_id', 'last_person_case_id'], timeout=30 * 60)
def get_api_demographics_data(month, state_id, last_person_case_id):
    demographics_data_query = BiharDemographicsView.objects.filter(
        month=month,
        state_id=state_id,
        person_id__gt=last_person_case_id
    ).order_by('person_id').values(
        'state_name',
        'state_site_code',
        'district_name',
        'district_site_code',
        'block_name',
        'block_site_code',
        'supervisor_name',
        'supervisor_site_code',
        'awc_name',
        'awc_site_code',
        'ward_number',
        'household_id',
        'household_name',
        'hh_reg_date',
        'hh_num',
        'hh_gps_location',
        'hh_caste',
        'hh_bpl_apl',
        'hh_minority',
        'hh_religion',
        'hh_member_number',
        'person_id',
        'person_name',
        'has_adhaar',
        'bank_account_number',
        'ifsc_code',
        'age_at_reg',
        'dob',
        'gender',
        'blood_group',
        'disabled',
        'disability_type',
        'referral_status',
        'migration_status',
        'resident',
        'registered_status',
        'rch_id',
        'mcts_id',
        'phone_number',
        'date_death',
        'site_death',
        'closed_on',
        'reason_closure'
    )

    # To apply pagination on database query with data size length
    limited_demographics_data = list(demographics_data_query[:CAS_API_PAGE_SIZE])
    return limited_demographics_data,  get_total_records_count(month, state_id)


@icds_quickcache(['month'], timeout=60 * 60 * 2)
def get_vaccine_total_records_count(month, state_id):
    return BiharVaccineView.objects.filter(
        month=month,
        state_id=state_id
    ).count()


@icds_quickcache(['state_id', 'month', 'last_person_case_id'], timeout=30 * 60)
def get_api_vaccine_data(state_id, month, last_person_case_id):
    vaccine_data_query = BiharVaccineView.objects.filter(
        month=month,
        state_id=state_id,
        person_id__gt=last_person_case_id
    ).order_by('person_id').values(
        'month',
        'person_id',
        'time_birth',
        'child_alive',
        'father_name',
        'mother_name',
        'father_id',
        'mother_id',
        'dob',
        'private_admit',
        'primary_admit',
        'date_last_private_admit',
        'date_return_private',
        'due_list_date_1g_dpt_1',
        'due_list_date_2g_dpt_2',
        'due_list_date_3g_dpt_3',
        'due_list_date_5g_dpt_booster',
        'due_list_date_7gdpt_booster_2',
        'due_list_date_0g_hep_b_0',
        'due_list_date_1g_hep_b_1',
        'due_list_date_2g_hep_b_2',
        'due_list_date_3g_hep_b_3',
        'due_list_date_3g_ipv',
        'due_list_date_4g_je_1',
        'due_list_date_5g_je_2',
        'due_list_date_5g_measles_booster',
        'due_list_date_4g_measles',
        'due_list_date_0g_opv_0',
        'due_list_date_1g_opv_1',
        'due_list_date_2g_opv_2',
        'due_list_date_3g_opv_3',
        'due_list_date_5g_opv_booster',
        'due_list_date_1g_penta_1',
        'due_list_date_2g_penta_2',
        'due_list_date_3g_penta_3',
        'due_list_date_1g_rv_1',
        'due_list_date_2g_rv_2',
        'due_list_date_3g_rv_3',
        'due_list_date_4g_vit_a_1',
        'due_list_date_5g_vit_a_2',
        'due_list_date_6g_vit_a_3',
        'due_list_date_6g_vit_a_4',
        'due_list_date_6g_vit_a_5',
        'due_list_date_6g_vit_a_6',
        'due_list_date_6g_vit_a_7',
        'due_list_date_6g_vit_a_8',
        'due_list_date_7g_vit_a_9',
        'due_list_date_1g_bcg'
    )

    # To apply pagination on database query with data size length
    limited_vaccine_data = list(vaccine_data_query[:CAS_API_PAGE_SIZE])
    return limited_vaccine_data, get_vaccine_total_records_count(month, state_id)
