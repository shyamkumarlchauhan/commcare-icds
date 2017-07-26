from django.conf.urls import url

from custom.icds_reports.views import TableauView, DashboardView, IcdsDynamicTemplateView, ProgramSummaryView, \
    AwcOpenedView, PrevalenceOfUndernutritionView, LocationView, LocationAncestorsView, AwcReportsView, \
    ExportIndicatorView, ProgressReportView, PrevalenceOfSevereView, PrevalenceOfStunningView, \
    ExclusiveBreastfeedingView, NewbornsWithLowBirthWeightView, EarlyInitiationBreastfeeding, \
    ChildrenInitiatedView, InstitutionalDeliveriesView, ImmunizationCoverageView, AWCDailyStatusView

urlpatterns = [
    url(r'^tableau/(?P<workbook>\w+)/(?P<worksheet>\w+)$', TableauView.as_view(), name='icds_tableau'),
    url(r'^icds_dashboard/', DashboardView.as_view(), name='icds_dashboard'),
    url(r'^icds-ng-template/(?P<template>[\w-].+)', IcdsDynamicTemplateView.as_view(), name='icds-ng-template'),
    url(r'^program_summary/(?P<step>[\w-]+)/', ProgramSummaryView.as_view(), name='program_summary'),
    url(r'^awc_opened/(?P<step>[\w-]+)/', AwcOpenedView.as_view(), name='awc_opened'),
    url(r'^awc_reports/(?P<step>[\w-]+)/', AwcReportsView.as_view(), name='awc_reports'),
    url(
        r'^underweight_children/(?P<step>[\w-]+)/',
        PrevalenceOfUndernutritionView.as_view(),
        name='underweight_children'),
    url(
        r'^prevalence_of_severe/(?P<step>[\w-]+)/',
        PrevalenceOfSevereView.as_view(),
        name='prevalence_of_severe'),
    url(
        r'^prevalence_of_stunning/(?P<step>[\w-]+)/',
        PrevalenceOfStunningView.as_view(),
        name='prevalence_of_stunning'),
    url(
        r'^low_birth/(?P<step>[\w-]+)/',
        NewbornsWithLowBirthWeightView.as_view(),
        name='low_birth'),
    url(
        r'^early_initiation/(?P<step>[\w-]+)/',
        EarlyInitiationBreastfeeding.as_view(),
        name='early_initiation'),
    url(r'^locations$', LocationView.as_view(), name='icds_locations'),
    url(r'^locations/ancestors$', LocationAncestorsView.as_view(), name='icds_locations_ancestors'),
    url(r'^export_indicator$', ExportIndicatorView.as_view(), name='icds_export_indicator'),
    url(r'^progress_report$', ProgressReportView.as_view(), name='progress_report'),
    url(
        r'^exclusive-breastfeeding/(?P<step>[\w-]+)/',
        ExclusiveBreastfeedingView.as_view(),
        name='exclusive-breastfeeding'),
    url(
        r'^children_initiated/(?P<step>[\w-]+)/',
        ChildrenInitiatedView.as_view(),
        name='children_initiated'),
    url(
        r'^institutional_deliveries/(?P<step>[\w-]+)/',
        InstitutionalDeliveriesView.as_view(),
        name='institutional_deliveries'),
    url(
        r'^immunization_coverage/(?P<step>[\w-]+)/',
        ImmunizationCoverageView.as_view(),
        name='immunization_coverage'),
    url(
        r'^awc_daily_status/(?P<step>[\w-]+)/',
        AWCDailyStatusView.as_view(),
        name='awc_daily_status'),
]
