from corehq.apps.locations.models import SQLLocation
from custom.icds_reports.models import ChildHealthMonthlyView
from custom.icds_reports.sqldata.base import IcdsSqlData
from custom.icds_reports.utils.mixins import ExportableMixin
from custom.icds_reports.utils import get_status, calculate_date_for_age, \
    current_month_stunting_column, \
    current_month_wasting_column, phone_number_function
from dateutil import relativedelta


class GrowthExport(ExportableMixin, IcdsSqlData):
    title = 'Child Growth Tracking'
    table_name = 'child_health_monthly_view'

    def get_excel_data(self, location):

        def _check_case_presence(case_id, column, data_dict):
            return data_dict[case_id][column] if case_id in data_dict.keys() else "N/A"

        filters = {
            "month": self.month,
            "aggregation_level": 5,
            "age_in_months__lte": 60,
            "valid_in_month": 1
        }
        initial_month = self.month

        if self.loc_level == 4:
            filters['supervisor_id'] = location
            order_by = ('awc_name',)
        elif self.loc_level == 3:
            filters['block_id'] = location
            order_by = ('supervisor_name', 'awc_name')
        elif self.loc_level == 2:
            filters['district_id'] = location
            order_by = ('block_name', 'supervisor_name', 'awc_name')
        elif self.loc_level == 1:
            filters['state_id'] = location
            order_by = ('district_name', 'block_name', 'supervisor_name', 'awc_name')
        else:
            order_by = ('state_name', 'district_name', 'block_name', 'supervisor_name', 'awc_name')

        query_set_1 = ChildHealthMonthlyView.objects.filter(**filters).order_by(*order_by)
        data_initial_month = query_set_1.values('state_name', 'district_name', 'block_name', 'supervisor_name',
                                                'awc_name', 'awc_site_code', 'person_name', 'dob', 'mother_name',
                                                'mother_phone_number', 'pse_days_attended', 'lunch_count',
                                                'current_month_nutrition_status',
                                                current_month_wasting_column(self.beta),
                                                current_month_stunting_column(self.beta), 'case_id')

        filters["month"] = initial_month - relativedelta(months=1)
        query_set_2 = ChildHealthMonthlyView.objects.filter(**filters).order_by(*order_by)
        data_past_month = query_set_2.values('case_id', 'pse_days_attended', 'lunch_count',
                                             'current_month_nutrition_status',
                                             current_month_wasting_column(self.beta),
                                             current_month_stunting_column(self.beta), 'case_id')
        data_past_month = {item['case_id']: item for item in data_past_month}

        filters["month"] = initial_month - relativedelta(months=2)
        query_set_3 = ChildHealthMonthlyView.objects.filter(**filters).order_by(*order_by)
        data_past_month_2 = query_set_3.values('case_id', 'pse_days_attended', 'lunch_count',
                                               'current_month_nutrition_status',
                                               current_month_wasting_column(self.beta),
                                               current_month_stunting_column(self.beta), 'case_id')
        data_past_month_2 = {item['case_id']: item for item in data_past_month_2}
        filters["month"] = initial_month

        month_1 = (initial_month - relativedelta(months=2)).strftime('%Y-%m-%d')
        month_2 = (initial_month - relativedelta(months=1)).strftime('%Y-%m-%d')
        month_3 = initial_month.strftime('%Y-%m-%d')

        headers = ['State', 'District', 'Block', 'Sector', 'Awc Name', 'Child', 'Age', 'Mother Name',
                   'Mother Phone Number', f'PSE_{month_1}', f'PSE_{month_2}', f'PSE_{month_3}', f'SN_{month_1}',
                   f'SN_{month_2}', f'SN_{month_3}', f'Stunting_{month_1}', f'Stunting_{month_2}',
                   f'Stunting_{month_3}', f'Wasting_month_1', f'Wasting_{month_2}', f'Wasting_{month_3}',
                   f'underweight_{month_1}', f'underweight_{month_2}', f'underweight_{month_3}']

        excel_rows = [headers]

        for row in data_initial_month:
            row_data = [
                row['state_name'],
                row['district_name'],
                row['block_name'],
                row['supervisor_name'],
                row['awc_name'],
                row['person_name'],
                calculate_date_for_age(row['dob'], initial_month),
                row['mother_name'],
                phone_number_function(row['mother_phone_number']),
                _check_case_presence(row['case_id'], 'pse_days_attended', data_past_month_2),
                _check_case_presence(row['case_id'], 'pse_days_attended', data_past_month),
                row['pse_days_attended'],
                _check_case_presence(row['case_id'], 'lunch_count', data_past_month_2),
                _check_case_presence(row['case_id'], 'lunch_count', data_past_month),
                row['lunch_count'],
                get_status(
                    _check_case_presence(row['case_id'], current_month_stunting_column(self.beta),
                                         data_past_month_2),
                    'stunted',
                    'Normal height for age',
                    True
                ),
                get_status(
                    _check_case_presence(row['case_id'], current_month_stunting_column(self.beta),
                                         data_past_month),
                    'stunted',
                    'Normal height for age',
                    True
                ),
                get_status(
                    row[current_month_stunting_column(self.beta)],
                    'stunted',
                    'Normal height for age',
                    True
                ),
                get_status(
                    _check_case_presence(row['case_id'], current_month_wasting_column(self.beta),
                                         data_past_month_2),
                    'wasted',
                    'Normal weight for height',
                    True
                ),
                get_status(
                    _check_case_presence(row['case_id'], current_month_wasting_column(self.beta), data_past_month),
                    'wasted',
                    'Normal weight for height',
                    True
                ),
                get_status(
                    row[current_month_wasting_column(self.beta)],
                    'wasted',
                    'Normal weight for height',
                    True
                ),
                get_status(
                    _check_case_presence(row['case_id'], 'current_month_nutrition_status', data_past_month_2),
                    'underweight',
                    'Normal weight for age',
                    True
                ),
                get_status(
                    _check_case_presence(row['case_id'], 'current_month_nutrition_status', data_past_month),
                    'underweight',
                    'Normal weight for age',
                    True
                ),
                get_status(
                    row['current_month_nutrition_status'],
                    'underweight',
                    'Normal weight for age',
                    True
                )
            ]
            excel_rows.append(row_data)
        filters = [['Generated at', india_now()]]
        if location:
            locs = SQLLocation.objects.get(location_id=location).get_ancestors(include_self=True)
            for loc in locs:
                filters.append([loc.location_type.name.title(), loc.name])
        else:
            filters.append(['Location', 'National'])

        date = self.month
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
