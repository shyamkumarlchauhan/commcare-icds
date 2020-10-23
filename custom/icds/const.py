from pathlib import Path

ICDS_APPS_ROOT = Path(__file__).parent.parent

SUPERVISOR_APP_ID = '48cc1709b7f62ffea24cc6634a00660c'
VHND_SURVEY_XMLNS = "http://openrosa.org/formdesigner/A1C9EF1B-8B42-43AB-BA81-9484DB9D8293"

CHILDREN_WEIGHED_REPORT_ID = 'static-icds-cas-static-ls_children_weighed'
DAYS_AWC_OPEN_REPORT_ID = 'static-icds-cas-static-ls_awc_days_open'
HOME_VISIT_REPORT_ID = 'static-icds-cas-static-ls_timely_home_visits'
THR_REPORT_ID = 'static-icds-cas-static-ls_thr_30_days'
LS_CCS_RECORD_CASES_REPORT_ID = 'static-icds-cas-static-ls_ccs_record_cases'

UCR_V2_LS_TIMELY_HOME_VISITS_ALIAS = 'ucr_v2_ls_timely_home_visits'
UCR_V2_MPR_5_CCS_RECORD_ALIAS = 'ucr_v2_mpr_5_ccs_record'
UCR_V2_MPR_5_CHILD_HEALTH_PT1_ALIAS = 'ucr_v2_mpr_5_child_health_pt1'
UCR_V2_MPR_5_CHILD_HEALTH_CASES_MONTHLY_ALIAS = 'ucr_v2_mpr_5_child_health_cases_monthly'
UCR_V2_LS_DAYS_AWC_OPEN_ALIAS = 'ucr_v2_ls_days_awc_open'
UCR_V2_AG_MONTHLY_ALIAS = 'ucr_v2_ag_monthly'
UCR_V2_AG_ALIAS = 'ucr_v2_ag'
UCR_V2_CBE_LAST_MONTH_ALIAS = 'ucr_v2_cbe_last_month'

AWC_LOCATION_TYPE_CODE = 'awc'
SUPERVISOR_LOCATION_TYPE_CODE = 'supervisor'
BLOCK_LOCATION_TYPE_CODE = 'block'
DISTRICT_LOCATION_TYPE_CODE = 'district'
STATE_TYPE_CODE = 'state'

ANDHRA_PRADESH_SITE_CODE = '28'
MAHARASHTRA_SITE_CODE = ''
MADHYA_PRADESH_SITE_CODE = '23'
BIHAR_SITE_CODE = '10'
CHHATTISGARH_SITE_CODE = '22'
JHARKHAND_SITE_CODE = '20'
RAJASTHAN_SITE_CODE = '08'
UTTAR_PRADESH_SITE_CODE = 'state9'

ENGLISH = 'en'
HINDI = 'hin'
TELUGU = 'tel'
MARATHI = 'mar'

FILE_TYPE_CHOICE_ZIP = 1
FILE_TYPE_CHOICE_DOC = 2
DISPLAY_CHOICE_LIST = 1
DISPLAY_CHOICE_FOOTER = 2
DISPLAY_CHOICE_CUSTOM = 3

DATA_PULL_CACHE_KEY = "icds_custom_data_pull_in_progress"
DATA_PULL_PERMITTED_START_HOUR = 19
DATA_PULL_PERMITTED_END_HOUR = 23

MAX_SMS_REPORT_DURATION = 35
MAX_CONCURRENT_SMS_REPORTS_ALLOWED = 3

CPMU_ROLE_NAME = "CPMU"
