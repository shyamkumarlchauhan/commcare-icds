from __future__ import absolute_import
from __future__ import unicode_literals

from datetime import date

from six.moves import range

from corehq.apps.locations.models import SQLLocation

from .agg_ccs_record import AggCcsRecordAggregationDistributedHelper
from .agg_child_health import AggChildHealthAggregationDistributedHelper
from .awc_infrastructure import AwcInfrastructureAggregationDistributedHelper
from .aww_incentive import AwwIncentiveAggregationDistributedHelper
from .ls_awc_visit_form import LSAwcMgtFormAggDistributedHelper
from .ls_beneficiary_form import LSBeneficiaryFormAggDistributedHelper
from .ls_vhnd_form import LSVhndFormAggDistributedHelper
from .agg_ls_data import AggLsDistributedHelper
from .birth_preparedness_forms import BirthPreparednessFormsAggregationDistributedHelper
from .ccs_record_monthly import CcsRecordMonthlyAggregationDistributedHelper
from .child_health_monthly import ChildHealthMonthlyAggregationDistributedHelper
from .complementary_forms import ComplementaryFormsAggregationDistributedHelper
from .complementary_forms_ccs_record import ComplementaryFormsCcsRecordAggregationDistributedHelper
from .daily_feeding_forms_child_health import DailyFeedingFormsChildHealthAggregationDistributedHelper
from .delivery_forms import DeliveryFormsAggregationDistributedHelper
from .growth_monitoring_forms import GrowthMonitoringFormsAggregationDistributedHelper
from .inactive_awws import InactiveAwwsAggregationDistributedHelper
from .postnatal_care_forms_ccs_record import PostnatalCareFormsCcsRecordAggregationDistributedHelper
from .postnatal_care_forms_child_health import PostnatalCareFormsChildHealthAggregationDistributedHelper
from .thr_forms_child_health import THRFormsChildHealthAggregationDistributedHelper
from .thr_froms_ccs_record import THRFormsCcsRecordAggregationDistributedHelper
from .agg_awc import AggAwcDistributedHelper
from .agg_awc_daily import AggAwcDailyAggregationDistributedHelper
from .awc_location import LocationAggregationDistributedHelper
from .daily_attendance import DailyAttendanceAggregationDistributedHelper
# excluded due to circular dependency
# from .mbt import CcsMbtDistributedHelper, ChildHealthMbtDistributedHelper, AwcMbtDistributedHelper


def recalculate_aggregate_table(model_class):
    """Expects a class (not instance) of models.Model

    Not expected to last past 2018 (ideally past May) so this shouldn't break in 2019
    """
    state_ids = (
        SQLLocation.objects
        .filter(domain='icds-cas', location_type__name='state')
        .values_list('location_id', flat=True)
    )

    for state_id in state_ids:
        for year in (2015, 2016, 2017):
            for month in range(1, 13):
                model_class.aggregate(state_id, date(year, month, 1))

        for month in range(1, date.today().month + 1):
            model_class.aggregate(state_id, date(2018, month, 1))
