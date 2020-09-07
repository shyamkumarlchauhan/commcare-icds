import copy
from corehq.apps.locations.models import SQLLocation
from custom.icds_reports.utils import india_now, DATA_NOT_ENTERED
from custom.icds_reports.models.views import ServiceDeliveryReportView, ChildHealthMonthlyView
from custom.icds_reports.models.aggregate import ChildHealthMonthly
from custom.icds_reports.utils import apply_exclude
from dateutil.relativedelta import relativedelta
from custom.icds_reports.utils import get_status


class MalnutritionTrackerReport(object):
    def __init__(self, config, location, beta=False):
        self.beta = beta
        self.location = location
        self.config = config
        self.title = 'Malnutrition Tracking Report'

    @property
    def location_columns(self):
        location_names = ['state_name', 'district_name', 'block_name', 'supervisor_name', "awc_name"]
        return location_names[:self.config['aggregation_level']]

    @property
    def headers_and_calculation(self):

        get_status_args = {
            'second_part': 'Acute Malnutritioned',
            'normal_value': 'Normal',
            'color_scheme': {'severe': 'Red', 'moderate': 'Yello', 'normal': 'Green', 'no_data': DATA_NOT_ENTERED}
        }

        def _get_wasting_status(wasting_status):
            if wasting_status == 'severe':
                padding_string = ' (SAM)'
            else:
                padding_string = ' (MAM)'

            return get_status(wasting_status, **get_status_args)['value'] + padding_string

        def _get_wasting_color(wasting_status):
            return get_status(wasting_status, **get_status_args)['color']

        return [
            ('State', 'state_name'),
            ('District', 'district_name'),
            ('Block', 'block_name'),
            ('Sector', 'supervisor_name'),
            ('AWC Name', 'awc_name'),
            ('AWC Site Code', "awc_site_code"),
            ('AWW Phone Number', 'aww_phone_number'),
            ('Child Health Case ID', "case_id"),
            ('Name of the Child', "person_name"),
            ('Gender', 'sex'),
            ('Name of Mother','mother_name'),
            ('Mobile Number of Father','mother_phone_number'),
            ('Date of Birth', 'dob'),
            ('Age in Months', 'age_in_months'),
            ('Height (in cm)', 'last_recorded_height'),
            ('Weight (in kg)', 'last_recorded_weight'),
            ('Nutrition status of the child in the month', 'wasting_last_recorded', _get_wasting_status),
            ('Growth Chart Zone', 'wasting_last_recorded', _get_wasting_color),
            ('Whether referred to NRC/Hospital','last_referral_date', lambda x: 'Yes' if x else 'No'),
            ('Date of referral', 'last_referral_date'),
            ('Date of admission in NRC/Hospital', 'referral_reached_date',),
            ('Date of Discharge from NRC/Hospital', 'last_referral_discharge_date'),
            ('Home Visit 1', 'sam_mam_visit_date_1'),
            ('Home Visit 2', 'sam_mam_visit_date_2'),
            ('Home Visit 3', 'sam_mam_visit_date_3'),
            ('Home Visit 4', 'sam_mam_visit_date_4'),
            ('Panchayat 1', 'poshan_panchayat_date_1'),
            ('Panchayat 2', 'poshan_panchayat_date_2'),
            ('Panchayat 3', 'poshan_panchayat_date_3'),
            ('Panchayat 4', 'poshan_panchayat_date_4'),
        ]

    def get_excel_data(self):

        previous_month = self.config['month'] - relativedelta(months=1)
        six_months_back = self.config['month'] - relativedelta(months=6)

        def _should_include_case(case, previous_month_case_data):
            if case['wasting_last_recorded'] in (['severe', 'moderate']):
                return True
            if case['wasting_last_recorded'] == 'normal':
                if case['case_id'] not in previous_month_case_data:
                    return True
                if case['last_referral_date'] and case['last_referral_date'] >= six_months_back:
                    return True
            return False

        def get_readable_value(value):
            if value is None or value == '':
                return DATA_NOT_ENTERED
            else:
                return value

        def _evaluate_row(row):
            row_data = []

            for header_calculation in self.headers_and_calculation:
                if len(header_calculation) == 2:
                    row_data.append(get_readable_value(row[header_calculation[1]]))
                else:
                    func = header_calculation[2]
                    column = header_calculation[1]
                    row_data.append(get_readable_value(func(row[column])))
            return row_data

        filters = copy.deepcopy(self.config)
        del filters['aggregation_level']
        del filters['domain']
        filters['wer_eligible']=1

        current_month_data = ChildHealthMonthlyView.objects.filter(
            wasting_last_recorded__in=['severe', 'moderate', 'normal'],
            **filters
        ).values(
            'state_name',
            'district_name',
            'block_name',
            'supervisor_name',
            'awc_name',
            'awc_site_code',
            'aww_phone_number',
            'case_id',
            'person_name',
            'sex',
            'mother_name',
            'mother_phone_number',
            'dob',
            'age_in_months',
            'last_recorded_weight',
            'last_recorded_height',
            'wasting_last_recorded',
            'last_referral_date',
            'referral_health_problem',
            'referral_reached_date',
            'last_referral_discharge_date',
            'sam_mam_visit_date_1',
            'sam_mam_visit_date_2',
            'sam_mam_visit_date_3',
            'sam_mam_visit_date_4',
            'poshan_panchayat_date_1',
            'poshan_panchayat_date_2',
            'poshan_panchayat_date_3',
            'poshan_panchayat_date_4',
        )

        current_month_data = apply_exclude(self.config['domain'], current_month_data)

        filters['month'] = previous_month

        # Check if there is a need of running it in parallel to above query
        previous_month_data = ChildHealthMonthlyView.objects.filter(
            wasting_last_recorded__in=['severe', 'moderate'],
            **filters
        ).values(
            'case_id',
            'wasting_last_recorded'
        )
        previous_month_data = apply_exclude(self.config['domain'], previous_month_data)

        previous_month_case_data = {
            case['case_id']: case['wasting_last_recorded'] for case in previous_month_data
        }

        current_month_data_trimmed = [
            case for case in current_month_data if _should_include_case(case, previous_month_case_data)
        ]


        headers = [elem[0] for elem in self.headers_and_calculation]
        excel_rows = [headers]

        for row in current_month_data_trimmed:
            excel_rows.append(_evaluate_row(row))

        filters = [['Generated at', india_now()]]
        if self.location:
            locs = SQLLocation.objects.get(location_id=self.location).get_ancestors(include_self=True)
            for loc in locs:
                filters.append([loc.location_type.name.title(), loc.name])
        else:
            filters.append(['Location', 'National'])

        date = self.config['month']
        filters.append(['Month', date.strftime("%B")])
        filters.append(['Year', date.year])

        return [
            [
                self.title,
                excel_rows
            ],
            [
                'Export Info',
                filters
            ]
        ]
