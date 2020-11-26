from datetime import date

from custom.icds_reports.models.helper import IcdsFile
from custom.icds_reports.tasks import prepare_excel_reports
from django.shortcuts import get_object_or_404

# params
include_test = False
export_format = 'xlsx'
month = 8
year = 2020
aggregation_level = 1  # this should be based on what comes from FE for each report. Can check from staging
indicator = 1  # Report type
config = {
    'aggregation_level': aggregation_level,
    'domain': 'icds-cas',
    'month': date(year, month, 1),
}

child_report = prepare_excel_reports(config, aggregation_level, include_test, False, '', 'icds-cas', export_format,
                                     indicator)
icds_file = get_object_or_404(IcdsFile, blob_id=child_report['uuid'])
fstream = icds_file.get_file_from_blobdb()

with open('child_report_aug.xlsx', 'wb') as fout:
    fout.write(fstream.read())

indicator = 2
pregnant_report = prepare_excel_reports(config, aggregation_level, include_test, False, '', 'icds-cas',
                                        export_format, indicator)
icds_file = get_object_or_404(IcdsFile, blob_id=pregnant_report['uuid'])
fstream = icds_file.get_file_from_blobdb()

with open('pregnant_report_aug.xlsx', 'wb') as fout:
    fout.write(fstream.read())

indicator = 3
demographics_report = prepare_excel_reports(config, aggregation_level, include_test, False, '', 'icds-cas',
                                            export_format, indicator)
icds_file = get_object_or_404(IcdsFile, blob_id=demographics_report['uuid'])
fstream = icds_file.get_file_from_blobdb()

with open('demographics_aug.xlsx', 'wb') as fout:
    fout.write(fstream.read())

indicator = 4
system_usage_report = prepare_excel_reports(config, aggregation_level, include_test, False, '', 'icds-cas',
                                            export_format, indicator)
icds_file = get_object_or_404(IcdsFile, blob_id=system_usage_report['uuid'])
fstream = icds_file.get_file_from_blobdb()

with open('system_usage_report_aug.xlsx', 'wb') as fout:
    fout.write(fstream.read())

indicator = 5
awc_infrastructure_report = prepare_excel_reports(config, aggregation_level, include_test, False, '', 'icds-cas',
                                                  export_format, indicator)
icds_file = get_object_or_404(IcdsFile, blob_id=awc_infrastructure_report['uuid'])
fstream = icds_file.get_file_from_blobdb()

with open('awc_infrastructure_report_aug.xlsx', 'wb') as fout:
    fout.write(fstream.read())

indicator = 12
config['beneficiary_category'] = 'pw_lw_children'
sdr_pw_lw_children_report = prepare_excel_reports(config, aggregation_level, include_test, False, '', 'icds-cas',
                                                  export_format, indicator)
icds_file = get_object_or_404(IcdsFile, blob_id=sdr_pw_lw_children_report['uuid'])
fstream = icds_file.get_file_from_blobdb()

config['beneficiary_category'] = 'children_3_6'
sdr_children_3_6_report = prepare_excel_reports(config, aggregation_level, include_test, False, '', 'icds-cas',
                                                  export_format, indicator)
icds_file = get_object_or_404(IcdsFile, blob_id=sdr_children_3_6_report['uuid'])
fstream = icds_file.get_file_from_blobdb()

with open('sdr_children_3_6_report_aug.xlsx', 'wb') as fout:
    fout.write(fstream.read())

indicator = 15
del config['beneficiary_category']
config['report_layout'] = 'comprehensive'
config['data_period'] = 'month'
config['year'] = 2020

ppr_report = prepare_excel_reports(config, aggregation_level, include_test, False, '', 'icds-cas', export_format,
                                   indicator)
icds_file = get_object_or_404(IcdsFile, blob_id=ppr_report['uuid'])
fstream = icds_file.get_file_from_blobdb()

with open('ppr_report_aug.xlsx', 'wb') as fout:
    fout.write(fstream.read())
