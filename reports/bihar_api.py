from custom.icds_reports.models.views import BiharDemographicsView
from custom.icds_reports.const import CAS_API_PAGE_SIZE
from custom.icds_reports.cache import icds_quickcache
from dateutil.relativedelta import relativedelta
from dimagi.utils.dates import force_to_date

# cache for 2 hours because it wont change atleast for 24 hours.
# This API will be hit in a loop of something and they should be able to scrape all
# the records in 2 hours.
@icds_quickcache(['month', 'state_id'], timeout=60 * 60 * 2)
def get_total_records_count(month, state_id):
    return BiharDemographicsView.objects.filter(
        month=month,
        state_id=state_id
    ).count()


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


@icds_quickcache(['month', 'state_id'], timeout=60 * 60 * 2)
def get_total_school_records_count(month, state_id, month_end_11yr, month_start_14yr):

    return BiharDemographicsView.objects.filter(
        month=month,
        state_id=state_id,
        dob__lt=month_end_11yr,
        dob__gte=month_start_14yr,
        gender='F',
    ).count()


def get_api_school_data(month, state_id, last_person_case_id):
    month_start = force_to_date(month).replace(day=1)
    month_end = month_start + relativedelta(months=1, seconds=-1)
    month_end_11yr = month_end - relativedelta(years=11)
    month_start_14yr = month_start - relativedelta(years=14)

    school_data_query = BiharDemographicsView.objects.filter(
        month=month,
        state_id=state_id,
        dob__lt=month_end_11yr,
        dob__gte=month_start_14yr,
        gender='F',
        person_id__gt=last_person_case_id
    ).order_by('person_id').values(
        'person_id',
        'person_name',
        'out_of_school_status',
        'last_class_attended_ever'
    )

    # To apply pagination on database query with data size length
    limited_school_data = list(school_data_query[:CAS_API_PAGE_SIZE])
    return limited_school_data, get_total_school_records_count(month, state_id,
                                                                month_end_11yr, month_start_14yr)
