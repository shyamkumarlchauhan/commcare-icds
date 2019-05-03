from __future__ import unicode_literals, absolute_import, division

from custom.icds_reports.models.views import AWWIncentiveReportMonthly
from custom.icds_reports.utils import india_now, DATA_NOT_ENTERED


class IncentiveReport(object):

    def __init__(self, location, month, aggregation_level):
        self.location = location
        self.month = month
        self.aggregation_level = aggregation_level

    def get_excel_data(self):

        def _format_infrastructure_data(data):
            return data if data else DATA_NOT_ENTERED

        if self.aggregation_level == 1:
            data = AWWIncentiveReportMonthly.objects.filter(
                month=self.month, state_id=self.location
            ).order_by('-district_name', '-block_name', '-supervisor_name')
        elif self.aggregation_level == 2:
            data = AWWIncentiveReportMonthly.objects.filter(
                month=self.month, district_id=self.location
            ).order_by('-block_name', '-supervisor_name')
        else:
            data = AWWIncentiveReportMonthly.objects.filter(
                month=self.month, block_id=self.location
            ).order_by('-supervisor_name')
        data = data.values(
            'state_name', 'district_name', 'block_name', 'supervisor_name', 'awc_name', 'aww_name',
            'contact_phone_number', 'wer_weighed', 'wer_eligible', 'awc_num_open', 'valid_visits',
            'expected_visits', 'is_launched'
        )

        headers = [
            'State', 'District', 'Block', 'Supervisor', 'AWC', 'AWW Name', 'AWW Contact Number',
            'Home Visits Conducted', 'Number of Days AWC was Open', 'Weighing Efficiency', 'Eligible for Incentive'
        ]

        excel_rows = [headers]
        for row in data:
            row_data = [
                row['state_name'],
                row['district_name'],
                row['block_name'],
                row['supervisor_name'],
                row['awc_name'],
            ]

            # AWC not launched
            if row['is_launched'] != 'yes':
                AWC_NOT_LAUNCHED = 'AWC not launched'
                row_data.extend([
                    AWC_NOT_LAUNCHED,
                    AWC_NOT_LAUNCHED,
                    AWC_NOT_LAUNCHED,
                    AWC_NOT_LAUNCHED,
                    AWC_NOT_LAUNCHED,
                    AWC_NOT_LAUNCHED
                ])
            else:
                if self.month.year < 2019 or (self.month.year == 2019 and self.month.month < 4):
                    func = int
                else:
                    func = round
                home_visit_percent = row['valid_visits'] / func(row['expected_visits']) if \
                    func(row['expected_visits']) else 1
                weighing_efficiency_percent = row['wer_weighed'] / row['wer_eligible'] if \
                    row['wer_eligible'] else 1
                if home_visit_percent > 1:
                    home_visit_percent = 1
                home_visit_conducted = '{:.2%}'.format(home_visit_percent)
                if row['awc_num_open'] is None:
                    num_open = DATA_NOT_ENTERED
                else:
                    num_open = row['awc_num_open']
                weighing_efficiency = '{:.2%}'.format(weighing_efficiency_percent)
                eligible_for_incentive = 'Yes' if \
                    weighing_efficiency_percent >= 0.6 and home_visit_percent >= 0.6 else 'No'
                no_visits = row['valid_visits'] == 0 and row['expected_visits'] == 0
                no_weights = row['wer_eligible'] == 0
                if no_visits:
                    home_visit_conducted = "No expected home visits"
                if no_weights:
                    weighing_efficiency = "No expected weight measurement"
                if no_visits and no_weights:
                    eligible_for_incentive = "Yes"

                row_data.extend([
                    _format_infrastructure_data(row['aww_name']),
                    _format_infrastructure_data(row['contact_phone_number']),
                    home_visit_conducted,
                    num_open,
                    weighing_efficiency,
                    eligible_for_incentive
                ])

            excel_rows.append(row_data)

        return [
            [
                'AWW Performance Report',
                excel_rows
            ],
            [
                'Export Info',
                [
                    ['Generated at', india_now()],
                    ['Grouped By', 'AWC'],
                    ['Month', self.month.month],
                    ['Year', self.month.year],
                    ['Disclaimer', "The information in the report is based on the self-reported data entered by "
                                   "the Anganwadi Worker in ICDS-CAS mobile application and is subject to timely "
                                   "data syncs."]
                ]
            ]
        ]
