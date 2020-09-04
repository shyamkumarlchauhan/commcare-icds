from datetime import date
from operator import mul, truediv, sub

from corehq.apps.locations.models import SQLLocation
from corehq.apps.reports.datatables import DataTablesHeader, DataTablesColumn, DataTablesColumnGroup

from django.utils.translation import ugettext as _
from custom.icds_reports.sqldata.base_identification import BaseIdentification
from custom.icds_reports.sqldata.base_operationalization import BaseOperationalization, BaseOperationalizationBeta
from custom.icds_reports.sqldata.base_populations import BasePopulation, BasePopulationBeta
from custom.icds_reports.utils import ICDSMixin, MPRData, ICDSDataTableColumn
from custom.icds_reports.models.aggregate import ChildHealthMonthly, AggCcsRecord, CcsRecordMonthly, AggChildHealth, \
    AggAwc, AggServiceDeliveryReport, AggMPRAwc
from custom.icds_reports.utils import get_location_filter
from django.db.models import F
from django.db.models.aggregates import Sum
from django.db.models import Case, When, Value
from custom.icds_reports.models.views import ServiceDeliveryReportView
from custom.icds_reports.models.views import ChildHealthMonthlyView, CcsRecordMonthlyView

class MPRIdentification(BaseIdentification):

    @property
    def rows(self):
        if self.config['location_id']:
            chosen_location = SQLLocation.objects.get(
                location_id=self.config['location_id']
            ).get_ancestors(include_self=True)
            rows = []
            for loc_type in ['State', 'District', 'Block']:
                loc = chosen_location.filter(location_type__name=loc_type.lower())
                if len(loc) == 1:
                    rows.append([loc_type, loc[0].name, loc[0].site_code])
                else:
                    rows.append([loc_type, '', ''])
            return rows


class MPROperationalization(BaseOperationalization, MPRData):
    pass

class MPROperationalizationBeta(BaseOperationalizationBeta, MPRData):
    pass


class MPRSectors(object):

    title = 'c. No of Sectors'
    slug = 'sectors'
    has_sections = False
    subtitle = []
    posttitle = None

    def __init__(self, config, allow_conditional_agg=False):
        self.config = config
        self.allow_conditional_agg = allow_conditional_agg

    @property
    def headers(self):
        return []

    @property
    def rows(self):
        if self.config['location_id']:
            selected_location = SQLLocation.objects.get(
                location_id=self.config['location_id']
            )
            supervisors = selected_location.get_descendants(include_self=True).filter(
                location_type__name='supervisor'
            )
            sup_number = [
                loc for loc in supervisors
                if 'test' not in loc.metadata and loc.metadata.get('test', '').lower() != 'yes'
            ]
            return [
                [
                    "Number of Sectors",
                    len(sup_number)
                ]
            ]


class MPRPopulation(BasePopulation, MPRData):

    title = 'e. Total Population of Project'


class MPRPopulationBeta(BasePopulationBeta, MPRData):
    title = 'd. Total Population of Project'


class MPRBirthsAndDeaths(ICDSMixin, MPRData):

    title = '2. Details of Births and Deaths during the month'
    slug = 'births_and_deaths'

    @property
    def headers(self):
        return DataTablesHeader(
            DataTablesColumn(_('Si No')),
            DataTablesColumn(_('Categories')),
            DataTablesColumnGroup(
                _('Among Permanent Residents'),
                ICDSDataTableColumn(_('Girls/Women'), sortable=False, span=1),
                ICDSDataTableColumn(_('Boys'), sortable=False, span=1),
            ),
            DataTablesColumnGroup(
                _('Among Temporary Residents'),
                ICDSDataTableColumn(_('Girls/Women'), sortable=False, span=1),
                ICDSDataTableColumn(_('Boys'), sortable=False, span=1),
            )
        )

    @property
    def rows(self):
        if self.config['location_id']:
            data = self.custom_data(selected_location=self.selected_location, domain=self.config['domain'])
            rows = []
            for row in self.row_config:
                row_data = []
                for idx, cell in enumerate(row):
                    if isinstance(cell, tuple):
                        x = data.get(cell[0], 0)
                        y = data.get(cell[1], 0)
                        row_data.append(x + y)
                    else:
                        row_data.append(data.get(cell, cell if cell == '--' or idx in [0, 1] else 0))
                rows.append(row_data)
            return rows

    @property
    def row_config(self):
        return (
            (
                _('A'),
                _('No. of live births'),
                'live_F_resident_birth_count',
                'live_M_resident_birth_count',
                'live_F_migrant_birth_count',
                'live_M_migrant_birth_count'
            ),
            (
                _('B'),
                _('No. of babies born dead'),
                'still_F_resident_birth_count',
                'still_M_resident_birth_count',
                'still_F_migrant_birth_count',
                'still_M_migrant_birth_count'
            ),
            (
                _('C'),
                _('No. of babies weighed within 3 days of birth'),
                'weighed_F_resident_birth_count',
                'weighed_M_resident_birth_count',
                'weighed_F_migrant_birth_count',
                'weighed_M_migrant_birth_count'
            ),
            (
                _('D'),
                _('Out of the above, no. of low birth weight babies (< 2500 gm)'),
                'lbw_F_resident_birth_count',
                'lbw_M_resident_birth_count',
                'lbw_F_migrant_birth_count',
                'lbw_M_migrant_birth_count'
            ),
            (
                _('E'),
                _('No. of neonatal deaths (within 28 days of birth)'),
                'dead_F_resident_neo_count',
                'dead_M_resident_neo_count',
                'dead_F_migrant_neo_count',
                'dead_M_migrant_neo_count'
            ),
            (
                _('F'),
                _('No. of post neonatal deaths (between 29 days and 12 months of birth)'),
                'dead_F_resident_postneo_count',
                'dead_M_resident_postneo_count',
                'dead_F_migrant_postneo_count',
                'dead_M_migrant_postneo_count'
            ),
            (
                _('G'),
                _('Total infant deaths (E+F)'),
                ('dead_F_resident_neo_count', 'dead_F_resident_postneo_count'),
                ('dead_M_resident_neo_count', 'dead_M_resident_postneo_count'),
                ('dead_F_migrant_neo_count', 'dead_F_migrant_postneo_count'),
                ('dead_M_migrant_neo_count', 'dead_M_migrant_postneo_count'),
            ),
            (
                _('H'),
                _('Total child deaths (1- 5 years)'),
                'dead_F_resident_child_count',
                'dead_M_resident_child_count',
                'dead_F_migrant_child_count',
                'dead_M_migrant_child_count'
            ),
            (
                _('I'),
                _('No. of deaths of women'),
                'dead_F_resident_adult_count',
                '--',
                'dead_F_migrant_adult_count',
                '--'
            ),
            (
                '',
                _('a. during pregnancy'),
                'dead_preg_resident_count',
                '--',
                'dead_preg_migrant_count',
                '--'
            ),
            (
                '',
                _('b. during delivery'),
                'dead_del_resident_count',
                '--',
                'dead_del_migrant_count',
                '--'
            ),
            (
                '',
                _('c. within 42 days of delivery'),
                'dead_pnc_resident_count',
                '--',
                'dead_pnc_migrant_count',
                '--',
            ),
        )



class MPRBirthsAndDeathsBeta(MPRBirthsAndDeaths):

    def custom_data(self, selected_location, domain):
        filters = get_location_filter(selected_location.location_id, domain)
        if filters['aggregation_level']>1:
            filters['aggregation_level'] -= 1

        filters['month'] = date(self.config['year'], self.config['month'], 1)
        data = dict()
        mother_death_data = AggMPRAwc.objects.filter(**filters).values('month').annotate(
            dead_F_resident_adult_count=F('mother_death_permanent_resident'),
            dead_F_migrant_adult_count=F('mother_death_temp_resident'),
            dead_preg_resident_count=F('pregnancy_death_permanent_resident'),
            dead_preg_migrant_count=F('pregnancy_death_temp_resident'),
            dead_del_resident_count=F('delivery_death_permanent_resident'),
            dead_del_migrant_count=F('delivery_death_temp_resident'),
            dead_pnc_resident_count=F('pnc_death_permanent_resident'),
            dead_pnc_migrant_count=F('pnc_death_temp_resident'),
        ).order_by('month').first()

        child_data = AggChildHealth.objects.filter(**filters).values('month').annotate(
            live_F_resident_birth_count=Sum(
                Case(
                    When(
                        gender='F', then='live_birth_permanent_resident'
                    ),
                    default=Value(0)
                )
            ),
            live_M_resident_birth_count=Sum(
                Case(
                    When(
                        gender='M', then='live_birth_permanent_resident'
                    ),
                    default=Value(0)
                )
            ),
            live_F_migrant_birth_count=Sum(
                Case(
                    When(
                        gender='F', then='live_birth_temp_resident'
                    ),
                    default=Value(0)
                )
            ),
            live_M_migrant_birth_count=Sum(
                Case(
                    When(
                        gender='M', then='live_birth_temp_resident'
                    ),
                    default=Value(0)
                )
            ),
            still_F_resident_birth_count=Sum(
                Case(
                    When(
                        gender='F', then='still_birth_permanent_resident'
                    ),
                    default=Value(0)
                )
            ),
            still_M_resident_birth_count=Sum(
                Case(
                    When(
                        gender='M', then='still_birth_permanent_resident'
                    ),
                    default=Value(0)
                )
            ),
            still_F_migrant_birth_count=Sum(
                Case(
                    When(
                        gender='F', then='still_birth_temp_resident'
                    ),
                    default=Value(0)
                )
            ),
            still_M_migrant_birth_count=Sum(
                Case(
                    When(
                        gender='M', then='still_birth_temp_resident'
                    ),
                    default=Value(0)
                )
            ),
            weighed_F_resident_birth_count=Sum(
                Case(
                    When(
                        gender='F', then='weighed_in_3_days_permanent_resident'
                    ),
                    default=Value(0)
                )
            ),
            weighed_M_resident_birth_count=Sum(
                Case(
                    When(
                        gender='M', then='weighed_in_3_days_permanent_resident'
                    ),
                    default=Value(0)
                )
            ),
            weighed_F_migrant_birth_count=Sum(
                Case(
                    When(
                        gender='F', then='weighed_in_3_days_temp_resident'
                    ),
                    default=Value(0)
                )
            ),
            weighed_M_migrant_birth_count=Sum(
                Case(
                    When(
                        gender='M', then='weighed_in_3_days_temp_resident'
                    ),
                    default=Value(0)
                )
            ),
            dead_F_resident_neo_count=Sum(
                Case(
                    When(
                        gender='F', then='neonatal_deaths_permanent_resident'
                    ),
                    default=Value(0)
                )
            ),
            dead_M_resident_neo_count=Sum(
                Case(
                    When(
                        gender='M', then='neonatal_deaths_permanent_resident'
                    ),
                    default=Value(0)
                )
            ),
            dead_F_migrant_neo_count=Sum(
                Case(
                    When(
                        gender='F', then='neonatal_deaths_temp_resident'
                    ),
                    default=Value(0)
                )
            ),
            dead_M_migrant_neo_count=Sum(
                Case(
                    When(
                        gender='M', then='neonatal_deaths_temp_resident'
                    ),
                    default=Value(0)
                )
            ),
            dead_F_resident_postneo_count=Sum(
                Case(
                    When(
                        gender='F', then='post_neonatal_deaths_permanent_resident'
                    ),
                    default=Value(0)
                )
            ),
            dead_M_resident_postneo_count=Sum(
                Case(
                    When(
                        gender='M', then='post_neonatal_deaths_permanent_resident'
                    ),
                    default=Value(0)
                )
            ),
            dead_F_migrant_postneo_count=Sum(
                Case(
                    When(
                        gender='F', then='post_neonatal_deaths_temp_resident'
                    ),
                    default=Value(0)
                )
            ),
            dead_M_migrant_postneo_count=Sum(
                Case(
                    When(
                        gender='M', then='post_neonatal_deaths_temp_resident'
                    ),
                    default=Value(0)
                )
            ),
            dead_F_resident_child_count=Sum(
                Case(
                    When(
                        gender='F', then='total_deaths_permanent_resident'
                    ),
                    default=Value(0)
                )
            ),
            dead_M_resident_child_count=Sum(
                Case(
                    When(
                        gender='M', then='total_deaths_permanent_resident'
                    ),
                    default=Value(0)
                )
            ),
            dead_F_migrant_child_count=Sum(
                Case(
                    When(
                        gender='F', then='total_deaths_temp_resident'
                    ),
                    default=Value(0)
                )
            ),
            dead_M_migrant_child_count=Sum(
                Case(
                    When(
                        gender='M', then='total_deaths_temp_resident'
                    ),
                    default=Value(0)
                )
            )
        ).order_by('month').first()

        if child_data:
            data.update(child_data)
        if mother_death_data:
            data.update(mother_death_data)

        data = {key: value if value else 0 for key, value in data.items()}
        return data


class MPRAWCDetails(ICDSMixin, MPRData):

    title = '3. Details of new registrations at AWC during the month'
    slug = 'awc_details'

    @property
    def headers(self):
        return DataTablesHeader(
            DataTablesColumn(_('Category')),
            DataTablesColumnGroup(
                _('Among permanent residents of AWC area'),
                ICDSDataTableColumn(_('Girls/Women'), sortable=False, span=1),
                ICDSDataTableColumn(_('Boys'), sortable=False, span=1),
            ),
            DataTablesColumnGroup(
                _('Among temporary residents of AWC area'),
                ICDSDataTableColumn(_('Girls/Women'), sortable=False, span=1),
                ICDSDataTableColumn(_('Boys'), sortable=False, span=1),
            )
        )

    @property
    def rows(self):
        if self.config['location_id']:
            data = self.custom_data(selected_location=self.selected_location, domain=self.config['domain'])
            rows = []
            for row in self.row_config:
                row_data = []
                for idx, cell in enumerate(row):
                    row_data.append(data.get(cell, cell if cell == '--' or idx == 0 else 0))
                rows.append(row_data)
            return rows

    @property
    def row_config(self):
        return (
            (
                _('a. Pregnant Women'),
                'pregnant_resident_count',
                '--',
                'pregnant_migrant_count',
                '--'
            ),
            (
                _('b. Live Births'),
                'live_F_resident_birth_count',
                'live_M_resident_birth_count',
                'live_F_migrant_birth_count',
                'live_M_migrant_birth_count'
            ),
            (
                _('c. 0-3 years children (excluding live births)'),
                'F_resident_count',
                'M_resident_count',
                'F_migrant_count',
                'M_migrant_count'
            ),
            (
                _('d. 3-6 years children'),
                'F_resident_count_1',
                'M_resident_count_1',
                'F_migrant_count_1',
                'M_migrant_count_1'
            ),
        )


class MPRAWCDetailsBeta(ICDSMixin, MPRData):

    title = '3. Details of new registrations at AWC during the month'
    slug = 'awc_details'

    @property
    def headers(self):
        return DataTablesHeader(
            DataTablesColumn(_('Category')),
            DataTablesColumnGroup(
                _('Among permanent residents of AWC area'),
                ICDSDataTableColumn(_('Girls/Women'), sortable=False, span=1),
                ICDSDataTableColumn(_('Boys'), sortable=False, span=1),
            ),
            DataTablesColumnGroup(
                _('Among temporary residents of AWC area'),
                ICDSDataTableColumn(_('Girls/Women'), sortable=False, span=1),
                ICDSDataTableColumn(_('Boys'), sortable=False, span=1),
            )
        )

    @property
    def rows(self):
        if self.config['location_id']:
            filters = get_location_filter(self.config['location_id'], self.config['domain'])
            filters['month'] = date(self.config['year'], self.config['month'], 1)
            if filters['aggregation_level'] > 1:
                filters['aggregation_level'] -= 1


            data = dict()

            ccs_data = AggCcsRecord.objects.filter(**filters).values('month').annotate(
                pregnant_resident=Sum('pregnant_permanent_resident'),
                pregnant_migrant=Sum('pregnant_temp_resident')
            ).order_by('month').first()

            child_data = AggChildHealth.objects.filter(**filters).values('month').annotate(
                live_birth_F_permanent_resident=Sum(
                    Case(
                        When(gender='F', then='live_birth_permanent_resident'), default=Value(0)
                    )
                ),
                live_birth_M_permanent_resident=Sum(
                    Case(
                        When(gender='M', then='live_birth_permanent_resident'), default=Value(0)
                    )
                ),
                live_birth_F_temp_resident=Sum(
                    Case(
                        When(gender='F', then='live_birth_temp_resident'), default=Value(0)
                    )
                ),
                live_birth_M_temp_resident=Sum(
                    Case(
                        When(gender='M', then='live_birth_temp_resident'), default=Value(0)
                    )
                ),
                child_0_3_F_permanent_resident=Sum(
                    Case(
                        When(gender='F', age_tranche__lte=36, then='permanent_resident'), default=Value(0)
                    )
                ),
                child_0_3_M_permanent_resident=Sum(
                    Case(
                        When(gender='F', then='permanent_resident'), default=Value(0)
                    )
                ),
                child_0_3_F_temp_resident=Sum(
                    Case(
                        When(gender='F', age_tranche__lte=36, then='temp_resident'), default=Value(0)
                    )
                ),
                child_0_3_M_temp_resident=Sum(
                    Case(
                        When(gender='M', age_tranche__lte=36, then='temp_resident'), default=Value(0)
                    )
                ),
                child_3_6_F_permanent_resident=Sum(
                    Case(
                        When(gender='F', age_tranche__gt=36,  then='permanent_resident'), default=Value(0)
                    )
                ),
                child_3_6_M_permanent_resident=Sum(
                    Case(
                        When(gender='M', age_tranche__gt=36, then='permanent_resident'), default=Value(0)
                    )
                ),
                child_3_6_F_temp_resident=Sum(
                    Case(
                        When(gender='F', age_tranche__gt=36,  then='temp_resident'), default=Value(0)
                    )
                ),
                child_3_6_M_temp_resident=Sum(
                    Case(
                        When(gender='M', age_tranche__gt=36, then='temp_resident'), default=Value(0)
                    )
                ),
            ).order_by('month').first()

            if child_data:
                data.update(child_data)

            if ccs_data:
                data.update(ccs_data)

            data = {key: value if value else 0 for key, value in data.items()}
            rows = []
            for row in self.row_config:
                row_data = []
                for idx, cell in enumerate(row):
                    indicator_value = data.get(cell, cell if cell == '--' or idx == 0 else 0)
                    row_data.append(indicator_value if indicator_value else 0)
                rows.append(row_data)


            return rows

    @property
    def row_config(self):
        return (
            (
                _('a. Pregnant Women'),
                'pregnant_resident',
                '--',
                'pregnant_migrant',
                '--'
            ),
            (
                _('b. Live Births'),
                'live_birth_F_permanent_resident',
                'live_birth_M_permanent_resident',
                'live_birth_F_temp_resident',
                'live_birth_M_temp_resident'
            ),
            (
                _('c. 0-3 years children (excluding live births)'),
                'child_0_3_F_permanent_resident',
                'child_0_3_M_permanent_resident',
                'child_0_3_F_temp_resident',
                'child_0_3_M_temp_resident'
            ),
            (
                _('d. 3-6 years children'),
                'child_3_6_F_permanent_resident',
                'child_3_6_M_permanent_resident',
                'child_3_6_F_temp_resident',
                'child_3_6_M_temp_resident'
            ),
        )


class MPRSupplementaryNutrition(ICDSMixin, MPRData):

    title = '4. Delivery of Supplementary Nutrition and Pre-School Education (PSE)'
    slug = 'supplementary_nutrition'

    def __init__(self, config, allow_conditional_agg=False):
        super(MPRSupplementaryNutrition, self).__init__(config, allow_conditional_agg)
        self.awc_open_count = 0

    @property
    def subtitle(self):
        return 'Average no. days AWCs were open during the month? %.1f' % (
            self.awc_open_count / (self.awc_number or 1)
        ),

    @property
    def headers(self):
        return DataTablesHeader(
            DataTablesColumn(''),
            DataTablesColumn(_('Morning snacks/ breakfast')),
            DataTablesColumn(_('Hot cooked meals/RTE')),
            DataTablesColumn(_('Take home ration (THR')),
            DataTablesColumn(_('PSE'))
        )

    @property
    def rows(self):
        if self.config['location_id']:
            data = self.custom_data(selected_location=self.selected_location, domain=self.config['domain'])
            self.awc_open_count = data.get('awc_open_count', 0)
            rows = []
            for row in self.row_config:
                row_data = []
                for idx, cell in enumerate(row):
                    if isinstance(cell, dict):
                        num = data.get(cell['column'], 0)
                        if cell['second_value'] == 'location_number':
                            denom = self.awc_number
                        else:
                            denom = data.get(cell['second_value'], 1)
                        if 'format' in cell:
                            cell_data = '%.1f%%' % (cell['func'](num, float(denom or 1)) * 100)
                        else:
                            cell_data = '%.1f' % cell['func'](num, float(denom or 1))
                        row_data.append(cell_data)
                    else:
                        row_data.append(data.get(cell, cell if cell == '--' or idx == 0 else 0))
                rows.append(row_data)
            return rows

    @property
    def row_config(self):
        return (
            (
                _('a. Average number of days services were provided'),
                {
                    'column': 'open_bfast_count',
                    'func': truediv,
                    'second_value': "location_number",
                },
                {
                    'column': 'open_hotcooked_count',
                    'func': truediv,
                    'second_value': "location_number",
                },
                {
                    'column': 'days_thr_provided_count',
                    'func': truediv,
                    'second_value': "location_number",
                },
                {
                    'column': 'open_pse_count',
                    'func': truediv,
                    'second_value': "location_number",
                }
            ),
            (
                _('b. % of AWCs provided supplementary food for 21 or more days'),
                {
                    'column': 'open_bfast_count_21',
                    'func': truediv,
                    'second_value': "location_number",
                    'format': 'percent'
                },
                {
                    'column': 'open_hotcooked_count_21',
                    'func': truediv,
                    'second_value': "location_number",
                    'format': 'percent'
                },
                '--',
                '--'
            ),
            (
                _('c. % of AWCs providing PSE for 16 or more days'),
                '--',
                '--',
                '--',
                {
                    'column': 'open_hotcooked_count_16',
                    'func': truediv,
                    'second_value': "location_number",
                    'format': 'percent'
                }
            ),
            (
                _('d. % of AWCs providing services for 9 days or less'),
                {
                    'column': 'open_bfast_count_9',
                    'func': truediv,
                    'second_value': "location_number",
                    'format': 'percent'
                },
                {
                    'column': 'open_hotcooked_count_9',
                    'func': truediv,
                    'second_value': "location_number",
                    'format': 'percent'
                },
                '',
                {
                    'column': 'open_hotcooked_count_9',
                    'func': truediv,
                    'second_value': "location_number",
                    'format': 'percent'
                }
            ),
        )


class MPRSupplementaryNutritionBeta(ICDSMixin, MPRData):

    title = '4. Delivery of Supplementary Nutrition and Pre-School Education (PSE)'
    slug = 'supplementary_nutrition'

    def __init__(self, config, allow_conditional_agg=False):
        super(MPRSupplementaryNutritionBeta, self).__init__(config, allow_conditional_agg)
        self.awc_open_count = 0

    @property
    def subtitle(self):
        return 'Average no. days AWCs were open during the month? %.1f' % (
            self.awc_open_count / (self.awc_number or 1)
        ),

    @property
    def headers(self):
        return DataTablesHeader(
            DataTablesColumn(''),
            DataTablesColumn(_('Morning snacks/ breakfast')),
            DataTablesColumn(_('Hot cooked meals/RTE')),
            DataTablesColumn(_('Take home ration (THR')),
            DataTablesColumn(_('PSE'))
        )

    @property
    def rows(self):
        if self.config['location_id']:
            filters = get_location_filter(self.config['location_id'], self.config['domain'])
            if filters.get('aggregation_level') > 1:
                filters['aggregation_level'] -= 1

            filters['month'] = date(self.config['year'], self.config['month'], 1)

            data = ServiceDeliveryReportView.objects.filter(**filters).values(
                'num_launched_awcs',
                'breakfast_served',
                'hcm_served',
                'thr_served',
                'pse_provided',
                'breakfast_21_days',
                'hcm_21_days',
                'pse_16_days',
                'breakfast_9_days',
                'hcm_9_days',
                'pse_9_days',
                'num_launched_awcs').order_by('month').first()

            data = data if data else {};
            self.awc_open_count = data.get('awc_days_open', 0)
            rows = []
            for row in self.row_config:
                row_data = []
                for idx, cell in enumerate(row):
                    if isinstance(cell, dict):
                        num = data.get(cell['column'], 0)
                        if cell['second_value'] == 'location_number':
                            denom = self.awc_number
                        else:
                            denom = data.get(cell['second_value'], 1)
                        if 'format' in cell:
                            cell_data = '%.1f%%' % (cell['func'](num, float(denom or 1)) * 100)
                        else:
                            cell_data = '%.1f' % cell['func'](num, float(denom or 1))
                        row_data.append(cell_data)
                    else:
                        row_data.append(data.get(cell, cell if cell == '--' or idx == 0 else 0))
                rows.append(row_data)
            return rows

    @property
    def row_config(self):
        return (
            (
                _('a. Average number of days services were provided'),
                {
                    'column': 'breakfast_served',
                    'func': truediv,
                    'second_value': "num_launched_awcs",
                },
                {
                    'column': 'hcm_served',
                    'func': truediv,
                    'second_value': "num_launched_awcs",
                },
                {
                    'column': 'thr_served',
                    'func': truediv,
                    'second_value': "num_launched_awcs",
                },
                {
                    'column': 'pse_provided',
                    'func': truediv,
                    'second_value': "num_launched_awcs",
                }
            ),
            (
                _('b. % of AWCs provided supplementary food for 21 or more days'),
                {
                    'column': 'breakfast_21_days',
                    'func': truediv,
                    'second_value': "num_launched_awcs",
                    'format': 'percent'
                },
                {
                    'column': 'hcm_21_days',
                    'func': truediv,
                    'second_value': "num_launched_awcs",
                    'format': 'percent'
                },
                '--',
                '--'
            ),
            (
                _('c. % of AWCs providing PSE for 16 or more days'),
                '--',
                '--',
                '--',
                {
                    'column': 'pse_16_days',
                    'func': truediv,
                    'second_value': "num_launched_awcs",
                    'format': 'percent'
                }
            ),
            (
                _('d. % of AWCs providing services for 9 days or less'),
                {
                    'column': 'breakfast_9_days',
                    'func': truediv,
                    'second_value': "num_launched_awcs",
                    'format': 'percent'
                },
                {
                    'column': 'hcm_9_days',
                    'func': truediv,
                    'second_value': "num_launched_awcs",
                    'format': 'percent'
                },
                '',
                {
                    'column': 'pse_9_days',
                    'func': truediv,
                    'second_value': "num_launched_awcs",
                    'format': 'percent'
                }
            ),
        )



class MPRUsingSalt(ICDSMixin, MPRData):

    slug = 'using_salt'
    title = "5. Number of AWCs using Iodized Salt"

    @property
    def rows(self):
        if self.config['location_id']:
            data = self.custom_data(selected_location=self.selected_location, domain=self.config['domain'])
            use_salt = data.get('use_salt', 0)
            percent = "%.2f" % ((use_salt or 0) * 100 / float(self.awc_number or 1))
            return [
                ["Number of AWCs using Iodized Salt:", use_salt],
                ["% of AWCs:", percent + " %"]
            ]

        return []

class MPRUsingSaltBeta(ICDSMixin, MPRData):

    slug = 'using_salt'
    title = "5. Number of AWCs using Iodized Salt"

    @property
    def rows(self):
        if self.config['location_id']:
            filters = get_location_filter(self.config['location_id'], self.config['domain'])
            if filters.get('aggregation_level') > 1:
                filters['aggregation_level'] -= 1

            filters['month'] = date(self.config['year'], self.config['month'], 1)

            awc_data = AggAwc.objects.filter(**filters).values(
                'use_salt',
                'num_launched_awcs').order_by('month').first()

            
            if not awc_data:
                awc_data = dict()
            use_salt = awc_data.get('use_salt', 0)
            percent = "%.2f" % ((use_salt or 0) * 100 / float(awc_data.get('num_launched_awcs', 0) or 1))
            return [
                ["Number of AWCs using Iodized Salt:", use_salt],
                ["% of AWCs:", percent + " %"]
            ]

        return []

class MPRProgrammeCoverage(ICDSMixin, MPRData):

    title = '6. Programme Coverage'
    slug = 'programme_coverage'
    has_sections = True

    @property
    def headers(self):
        return DataTablesHeader(
            DataTablesColumn('Category'),
            DataTablesColumnGroup(
                _('6-35 months'),
                DataTablesColumn(_('Girls')),
                DataTablesColumn(_('Boys'))
            ),
            DataTablesColumnGroup(
                _('3-71 months'),
                DataTablesColumn(_('Girls')),
                DataTablesColumn(_('Boys'))
            ),
            DataTablesColumnGroup(
                _('All Children (6-71 months)'),
                DataTablesColumn(_('Girls')),
                DataTablesColumn(_('Boys')),
                DataTablesColumn(_('Total'))
            ),
            DataTablesColumn(_('Pregnant Women')),
            DataTablesColumn(_('Lactating mothers'))
        )

    @property
    def rows(self):
        if self.config['location_id']:
            data = self.custom_data(selected_location=self.selected_location, domain=self.config['domain'])
            sections = []
            for section in self.row_config:
                rows = []
                for row in section['rows_config']:
                    row_data = []
                    for idx, cell in enumerate(row):
                        if isinstance(cell, dict):
                            num = 0
                            for c in cell['columns']:
                                num += data.get(c, 0)

                            if 'second_value' in cell:
                                denom = data.get(cell['second_value'], 1)
                                alias_data = cell['func'](num, float(denom or 1))
                                if "format" in cell:
                                    cell_data = "%.1f%%" % (cell['func'](num, float(denom or 1)) * 100)
                                else:
                                    cell_data = "%.1f" % cell['func'](num, float(denom or 1))
                            else:
                                cell_data = num
                                alias_data = num

                            if 'alias' in cell:
                                data[cell['alias']] = alias_data
                            row_data.append(cell_data)
                        elif isinstance(cell, tuple):
                            cell_data = 0
                            for c in cell:
                                cell_data += data.get(c, 0)
                            row_data.append(cell_data)
                        else:
                            row_data.append(data.get(cell, cell if cell == '--' or idx == 0 else 0))
                    rows.append(row_data)
                sections.append(dict(
                    title=section['title'],
                    headers=self.headers,
                    slug=section['slug'],
                    rows=rows
                ))
            print(data)
            return sections

    @property
    def row_config(self):
        return [
            {
                'title': "a. Supplementary Nutrition beneficiaries (number of those among "
                         "residents who were given supplementary nutrition for 21+ days "
                         "during the reporting month)",
                'slug': 'programme_coverage_1',
                'rows_config': (
                    (
                        _('ST'),
                        'thr_rations_female_st',
                        'thr_rations_male_st',
                        'thr_rations_female_st_1',
                        'thr_rations_male_st_1',
                        {
                            'columns': ('thr_rations_female_st', 'thr_rations_female_st_1'),
                            'alias': 'rations_female_st'
                        },
                        {
                            'columns': ('thr_rations_male_st', 'thr_rations_male_st_1'),
                            'alias': 'rations_male_st'
                        },
                        {
                            'columns': ('rations_female_st', 'rations_male_st'),
                            'alias': 'all_rations_st'
                        },
                        'thr_rations_pregnant_st',
                        'thr_rations_lactating_st'
                    ),
                    (
                        _('SC'),
                        'thr_rations_female_sc',
                        'thr_rations_male_sc',
                        'thr_rations_female_sc_1',
                        'thr_rations_male_sc_1',
                        {
                            'columns': ('thr_rations_female_sc', 'thr_rations_female_sc_1'),
                            'alias': 'rations_female_sc'
                        },
                        {
                            'columns': ('thr_rations_male_sc', 'thr_rations_male_sc_1'),
                            'alias': 'rations_male_sc'
                        },
                        {
                            'columns': ('rations_female_sc', 'rations_male_sc'),
                            'alias': 'all_rations_sc'
                        },
                        'thr_rations_pregnant_sc',
                        'thr_rations_lactating_sc'
                    ),
                    (
                        _('Other'),
                        'thr_rations_female_others',
                        'thr_rations_male_others',
                        'thr_rations_female_others_1',
                        'thr_rations_male_others_1',
                        {
                            'columns': ('thr_rations_female_others', 'thr_rations_female_others_1'),
                            'alias': 'rations_female_others'
                        },
                        {
                            'columns': ('thr_rations_male_others', 'thr_rations_male_others_1'),
                            'alias': 'rations_male_others'
                        },
                        {
                            'columns': ('rations_female_others', 'rations_male_others'),
                            'alias': 'all_rations_others'
                        },
                        'thr_rations_pregnant_others',
                        'thr_rations_lactating_others'
                    ),
                    (
                        _('All Categories (Total)'),
                        ('thr_rations_female_st', 'thr_rations_female_sc', 'thr_rations_female_others'),
                        ('thr_rations_male_st', 'thr_rations_male_sc', 'thr_rations_male_others'),
                        ('thr_rations_female_st_1', 'thr_rations_female_sc_1', 'thr_rations_female_others_1'),
                        ('thr_rations_male_st_1', 'thr_rations_male_sc_1', 'thr_rations_male_others_1'),
                        ('rations_female_st', 'rations_female_sc', 'rations_female_others'),
                        ('rations_male_st', 'rations_male_sc', 'rations_male_others'),
                        ('all_rations_st', 'all_rations_sc', 'all_rations_others'),
                        ('thr_rations_pregnant_st', 'thr_rations_pregnant_sc', 'thr_rations_pregnant_others'),
                        ('thr_rations_lactating_st', 'thr_rations_lactating_sc', 'thr_rations_lactating_others'),
                    ),
                    (
                        _('Disabled'),
                        'thr_rations_female_disabled',
                        'thr_rations_male_disabled',
                        'thr_rations_female_disabled_1',
                        'thr_rations_male_disabled_1',
                        ('thr_rations_female_disabled', 'thr_rations_female_disabled_1'),
                        ('thr_rations_male_disabled', 'thr_rations_male_disabled_1'),
                        ('thr_rations_female_disabled', 'thr_rations_female_disabled_1',
                         'thr_rations_male_disabled', 'thr_rations_male_disabled_1'),
                        'thr_rations_pregnant_disabled',
                        'thr_rations_lactating_disabled'
                    ),
                    (
                        _('Minority'),
                        'thr_rations_female_minority',
                        'thr_rations_male_minority',
                        'thr_rations_female_minority_1',
                        'thr_rations_male_minority_1',
                        ('thr_rations_female_minority', 'thr_rations_female_minority_1'),
                        ('thr_rations_male_minority', 'thr_rations_male_minority_1'),
                        ('thr_rations_female_minority', 'thr_rations_female_minority_1',
                         'thr_rations_male_minority', 'thr_rations_male_minority_1'),
                        'thr_rations_pregnant_minority',
                        'thr_rations_lactating_minority'
                    ),
                )
            },
            {
                'title': 'b. Feeding Efficiency',
                'slug': 'programme_coverage_2',
                'rows_config': (
                    (
                        _('I. Population Total'),
                        'child_count_female',
                        'child_count_male',
                        'child_count_female_1',
                        'child_count_male_1',
                        ('child_count_female', 'child_count_female_1'),
                        ('child_count_male', 'child_count_male_1'),
                        ('child_count_female', 'child_count_female_1',
                         'child_count_male', 'child_count_male_1'),
                        'pregnant',
                        'lactating'
                    ),
                    (
                        _('II. Usual absentees during the month'),
                        'thr_rations_absent_female',
                        'thr_rations_absent_male',
                        'thr_rations_absent_female_1',
                        'thr_rations_absent_male_1',
                        ('thr_rations_absent_female', 'thr_rations_absent_female_1'),
                        ('thr_rations_absent_male', 'thr_rations_absent_male_1'),
                        ('thr_rations_absent_female', 'thr_rations_absent_female_1',
                         'thr_rations_absent_male', 'thr_rations_absent_male_1'),
                        'thr_rations_absent_pregnant',
                        'thr_rations_absent_lactating'
                    ),
                    (
                        _('III. Total present for at least one day during the month'),
                        {
                            'columns': (
                                'thr_rations_partial_female',
                                'thr_rations_female_sc',
                                'thr_rations_female_st',
                                'thr_rations_female_others'
                            ),
                            'alias': 'sum_thr_rations_female'
                        },
                        {
                            'columns': (
                                'thr_rations_partial_male',
                                'thr_rations_male_sc',
                                'thr_rations_male_st',
                                'thr_rations_male_others'
                            ),
                            'alias': 'sum_thr_rations_male'
                        },
                        {
                            'columns': (
                                'thr_rations_partial_female_1',
                                'thr_rations_female_sc_1',
                                'thr_rations_female_st_1',
                                'thr_rations_female_others_1'
                            ),
                            'alias': 'sum_thr_rations_female_1'
                        },
                        {
                            'columns': (
                                'thr_rations_partial_male_1',
                                'thr_rations_male_sc_1',
                                'thr_rations_male_st_1',
                                'thr_rations_male_others_1'
                            ),
                            'alias': 'sum_thr_rations_male_1'
                        },
                        {
                            'columns': ('sum_thr_rations_female', 'sum_thr_rations_female_1'),
                            'alias': 'total_rations_partial_female',
                        },
                        {
                            'columns': ('sum_thr_rations_male', 'sum_thr_rations_male_1'),
                            'alias': 'total_rations_partial_male'
                        },
                        {
                            'columns': ('total_rations_partial_female', 'total_rations_partial_male'),
                            'alias': 'all_rations_partial'
                        },
                        {
                            'columns': (
                                'thr_rations_partial_pregnant',
                                'thr_rations_pregnant_sc',
                                'thr_rations_pregnant_st',
                                'thr_rations_pregnant_others',
                            )
                        },
                        {
                            'columns': (
                                'thr_rations_partial_lactating',
                                'thr_rations_lactating_sc',
                                'thr_rations_lactating_st',
                                'thr_rations_lactating_others',
                            )
                        }
                    ),
                    (
                        _('IV. Expected Total Person Feeding Days (TPFD)'),
                        {
                            'columns': ('thr_rations_partial_female',),
                            'func': mul,
                            'second_value': 'days_thr_provided_count',
                            'alias': 'rations_partial_female'
                        },
                        {
                            'columns': ('thr_rations_partial_male',),
                            'func': mul,
                            'second_value': 'days_thr_provided_count',
                            'alias': 'rations_partial_male'
                        },
                        {
                            'columns': ('thr_rations_partial_female_1',),
                            'func': mul,
                            'second_value': 'days_thr_provided_count',
                            'alias': 'rations_partial_female_1'
                        },
                        {
                            'columns': ('thr_rations_partial_male_1',),
                            'func': mul,
                            'second_value': 'days_thr_provided_count',
                            'alias': 'rations_partial_female_1'
                        },
                        {
                            'columns': ('total_rations_partial_female',),
                            'func': mul,
                            'second_value': 'days_thr_provided_count',
                            'alias': 'all_rations_partial_female'
                        },
                        {
                            'columns': ('total_rations_partial_male',),
                            'func': mul,
                            'second_value': 'days_thr_provided_count',
                            'alias': 'all_rations_partial_male'
                        },
                        {
                            'columns': ('all_rations_partial',),
                            'func': mul,
                            'second_value': 'days_thr_provided_count',
                            'alias': 'rations_partial'
                        },
                        {
                            'columns': ('thr_rations_partial_pregnant',),
                            'func': mul,
                            'second_value': 'days_thr_provided_count',
                            'alias': 'rations_partial_pregnant'
                        },
                        {
                            'columns': ('thr_rations_partial_lactating',),
                            'func': mul,
                            'second_value': 'days_thr_provided_count',
                            'alias': 'rations_partial_lactating'
                        }
                    ),
                    (
                        _('V. Actual TPFD'),
                        'thr_total_rations_female',
                        'thr_total_rations_male',
                        'thr_total_rations_female_1',
                        'thr_total_rations_male_1',
                        {
                            'columns': ('thr_total_rations_female', 'thr_total_rations_female_1'),
                            'alias': 'total_thr_total_rations_female'
                        },
                        {
                            'columns': ('thr_total_rations_male', 'thr_total_rations_male_1'),
                            'alias': 'total_thr_total_rations_male'
                        },
                        {
                            'columns': ('total_thr_total_rations_female', 'total_thr_total_rations_male'),
                            'alias': 'total_thr_total_rations'
                        },
                        'thr_total_rations_pregnant',
                        'thr_total_rations_lactating'
                    ),
                    (
                        _('VI. Feeding Efficiency'),
                        {
                            'columns': ('thr_total_rations_female',),
                            'func': truediv,
                            'second_value': 'rations_partial_female',
                        },
                        {
                            'columns': ('thr_total_rations_male',),
                            'func': truediv,
                            'second_value': 'rations_partial_male',
                        },
                        {
                            'columns': ('thr_total_rations_female_1',),
                            'func': truediv,
                            'second_value': 'rations_partial_female_1',
                        },
                        {
                            'columns': ('thr_total_rations_male_1',),
                            'func': truediv,
                            'second_value': 'rations_partial_male_1',
                        },
                        {
                            'columns': ('total_thr_total_rations_female',),
                            'func': truediv,
                            'second_value': 'all_rations_partial_female',
                        },
                        {
                            'columns': ('total_thr_total_rations_male',),
                            'func': truediv,
                            'second_value': 'all_rations_partial_male',
                        },
                        {
                            'columns': ('total_thr_total_rations',),
                            'func': truediv,
                            'second_value': 'rations_partial',
                        },
                        {
                            'columns': ('thr_rations_pregnant_minority',),
                            'func': truediv,
                            'second_value': 'rations_partial_pregnant',
                        },
                        {
                            'columns': ('thr_rations_lactating_minority',),
                            'func': truediv,
                            'second_value': 'rations_partial_lactating',
                        },
                    ),
                )
            },
            {
                'title': 'c. Temporary Residents who received supplementary food during the month',
                'slug': 'programme_coverage_3',
                'rows_config': (
                    (
                        _('Number of temporary residents who received supplementary food'),
                        'thr_rations_migrant_female',
                        'thr_rations_migrant_male',
                        'thr_rations_migrant_female_1',
                        'thr_rations_migrant_male_1',
                        {
                            'columns': ('thr_rations_migrant_female', 'thr_rations_migrant_female_1'),
                            'alias': 'rations_migrant_female'
                        },
                        {
                            'columns': ('thr_rations_migrant_male', 'thr_rations_migrant_male_1'),
                            'alias': 'rations_migrant_male'
                        },
                        {
                            'columns': ('rations_migrant_female', 'rations_migrant_male'),
                            'alias': 'rations_migrant'
                        },
                        'thr_rations_migrant_pregnant',
                        'thr_rations_migrant_lactating'
                    ),
                )
            }
        ]


class MPRProgrammeCoverageBeta(MPRProgrammeCoverage):

    @property
    def headers(self):
        return DataTablesHeader(
            DataTablesColumn('Category'),
            DataTablesColumnGroup(
                _('6-35 months'),
                DataTablesColumn(_('Girls')),
                DataTablesColumn(_('Boys'))
            ),
            DataTablesColumnGroup(
                _('36-71 months'),
                DataTablesColumn(_('Girls')),
                DataTablesColumn(_('Boys'))
            ),
            DataTablesColumnGroup(
                _('All Children (6-71 months)'),
                DataTablesColumn(_('Girls')),
                DataTablesColumn(_('Boys')),
                DataTablesColumn(_('Total'))
            ),
            DataTablesColumn(_('Pregnant Women')),
            DataTablesColumn(_('Lactating mothers'))
        )

    def custom_data(self, selected_location, domain):
        filters = get_location_filter(selected_location.location_id, domain)
        if filters.get('aggregation_level') > 1:
            filters['aggregation_level'] -= 1

        filters['month'] = date(self.config['year'], self.config['month'], 1)
        data = dict()
        child_data = AggChildHealth.objects.filter(**filters).values('month').annotate(
            thr_rations_female_st=Sum(Case(When(gender='F', then='thr_25_days_st_resident'))),
            thr_rations_male_st=Sum(Case(When(gender='M', then='thr_25_days_st_resident'))),
            thr_rations_female_st_1=Sum(Case(When(gender='F', then='lunch_25_days_st_resident'))),
            thr_rations_male_st_1=Sum(Case(When(gender='M', then='lunch_25_days_st_resident'))),

            thr_rations_female_sc=Sum(Case(When(gender='F', then='thr_25_days_sc_resident'))),
            thr_rations_male_sc=Sum(Case(When(gender='M', then='thr_25_days_sc_resident'))),
            thr_rations_female_sc_1=Sum(Case(When(gender='F', then='lunch_25_days_sc_resident'))),
            thr_rations_male_sc_1=Sum(Case(When(gender='M', then='lunch_25_days_sc_resident'))),

            thr_rations_female_others=Sum(Case(When(gender='F', then='thr_25_days_other_resident'))),
            thr_rations_male_others=Sum(Case(When(gender='M', then='thr_25_days_other_resident'))),
            thr_rations_female_others_1=Sum(Case(When(gender='F', then='lunch_25_days_other_resident'))),
            thr_rations_male_others_1=Sum(Case(When(gender='M', then='lunch_25_days_other_resident'))),

            thr_rations_female_disabled=Sum(Case(When(gender='F', then='thr_25_days_disabled_resident'))),
            thr_rations_male_disabled=Sum(Case(When(gender='M', then='thr_25_days_disabled_resident'))),
            thr_rations_female_disabled_1=Sum(Case(When(gender='F', then='lunch_25_days_disabled_resident'))),
            thr_rations_male_disabled_1=Sum(Case(When(gender='M', then='lunch_25_days_disabled_resident'))),

            thr_rations_female_minority=Sum(Case(When(gender='F', then='thr_25_days_minority_resident'))),
            thr_rations_male_minority=Sum(Case(When(gender='M', then='thr_25_days_minority_resident'))),
            thr_rations_female_minority_1=Sum(Case(When(gender='F', then='lunch_25_days_minority_resident'))),
            thr_rations_male_minority_1=Sum(Case(When(gender='M', then='lunch_25_days_minority_resident'))),
            child_count_female=Sum(Case(When(gender='F', then='thr_eligible'))),
            child_count_male=Sum(Case(When(gender='M', then='thr_eligible'))),
            child_count_female_1=Sum(Case(When(gender='F', then='pse_eligible'))),
            child_count_male_1=Sum(Case(When(gender='M', then='pse_eligible'))),
            thr_rations_absent_female=Sum(Case(When(gender='F', then='thr_0_days_resident'))),
            thr_rations_absent_male=Sum(Case(When(gender='M', then='thr_0_days_resident'))),
            thr_rations_absent_female_1=Sum(Case(When(gender='F', then='lunch_0_days_resident'))),
            thr_rations_absent_male_1=Sum(Case(When(gender='M', then='lunch_0_days_resident'))),
            sum_thr_rations_female=Sum(Case(When(gender='F', then='thr_1_days_resident'))),
            sum_thr_rations_male=Sum(Case(When(gender='M', then='thr_1_days_resident'))),
            sum_thr_rations_female_1=Sum(Case(When(gender='F', then='lunch_1_days_resident'))),
            sum_thr_rations_male_1=Sum(Case(When(gender='M', then='lunch_1_days_resident'))),
            thr_total_rations_female=Sum(Case(When(gender='F', then='total_thr_resident'))),
            thr_total_rations_male=Sum(Case(When(gender='M', then='total_thr_resident'))),
            thr_total_rations_female_1=Sum(Case(When(gender='F', then='total_lunch_resident'))),
            thr_total_rations_male_1=Sum(Case(When(gender='M', then='total_lunch_resident'))),
            thr_rations_migrant_female=Sum(Case(When(gender='F', then='thr_1_days_migrant'))),
            thr_rations_migrant_male=Sum(Case(When(gender='M', then='thr_1_days_migrant'))),
            thr_rations_migrant_female_1=Sum(Case(When(gender='F', then='lunch_1_days_migrant'))),
            thr_rations_migrant_male_1=Sum(Case(When(gender='M', then='lunch_1_days_migrant'))),
        ).order_by('month').first()

        mother_data = AggCcsRecord.objects.filter(**filters).values('month').annotate(
            thr_rations_pregnant_st=Sum(Case(When(ccs_status='pregnant', then='thr_25_days_st_resident'))),
            thr_rations_lactating_st=Sum(Case(When(ccs_status='lactating', then='thr_25_days_st_resident'))),
            thr_rations_pregnant_sc=Sum(Case(When(ccs_status='pregnant', then='thr_25_days_sc_resident'))),
            thr_rations_lactating_sc=Sum(Case(When(ccs_status='lactating', then='thr_25_days_sc_resident'))),
            thr_rations_pregnant_others=Sum(Case(When(ccs_status='pregnant', then='thr_25_days_other_resident'))),
            thr_rations_lactating_others=Sum(Case(When(ccs_status='lactating', then='thr_25_days_other_resident'))),
            thr_rations_pregnant_disabled=Sum(Case(When(ccs_status='pregnant', then='thr_25_days_disabled_resident'))),
            thr_rations_lactating_disabled=Sum(Case(When(ccs_status='lactating', then='thr_25_days_disabled_resident'))),
            thr_rations_pregnant_minority=Sum(Case(When(ccs_status='pregnant', then='thr_25_days_minority_resident'))),
            thr_rations_lactating_minority=Sum(Case(When(ccs_status='lactating', then='thr_25_days_minority_resident'))),
            thr_rations_absent_pregnant=Sum(Case(When(ccs_status='pregnant', then='thr_0_days_resident'))),
            thr_rations_absent_lactating=Sum(Case(When(ccs_status='lactating', then='thr_0_days_resident'))),
            thr_rations_partial_pregnant=Sum(Case(When(ccs_status='pregnant', then='thr_1_days_resident'))),
            thr_rations_partial_lactating=Sum(Case(When(ccs_status='lactating', then='thr_1_days_resident'))),
            thr_total_rations_pregnant=Sum(Case(When(ccs_status='pregnant', then='total_thr_resident'))),
            thr_total_rations_lactating=Sum(Case(When(ccs_status='lactating', then='total_thr_resident'))),
            thr_rations_migrant_pregnant=Sum(Case(When(ccs_status='pregnant', then='thr_1_days_migrant'))),
            thr_rations_migrant_lactating=Sum(Case(When(ccs_status='lactating', then='thr_1_days_migrant'))),
            pregnant=Sum('pregnant'),
            lactating=Sum('lactating'),
        ).order_by('month').first()

        if child_data:
            data.update(child_data)
        if mother_data:
            data.update(mother_data)
        data = {key: value if value else 0 for key, value in data.items()}
        return data

    @property
    def row_config(self):
        parent_row_config = super(MPRProgrammeCoverageBeta, self).row_config

        feeding_efficiency = list(parent_row_config[1]['rows_config'])

        del feeding_efficiency[3]
        del feeding_efficiency[4]

        feeding_efficiency[2] = (
            _('III. Total present for at least one day during the month'),
            'sum_thr_rations_female',
            'sum_thr_rations_male',
            'sum_thr_rations_female_1',
            'sum_thr_rations_male_1',
            {
                'columns': ('sum_thr_rations_female', 'sum_thr_rations_female_1'),
                'alias': 'total_rations_partial_female',
            },
            {
                'columns': ('sum_thr_rations_male', 'sum_thr_rations_male_1'),
                'alias': 'total_rations_partial_male'
            },
            {
                'columns': ('total_rations_partial_female', 'total_rations_partial_male'),
                'alias': 'all_rations_partial'
            },
            'thr_rations_partial_pregnant',
            'thr_rations_partial_lactating'
        )

        parent_row_config[1]['rows_config'] = tuple(feeding_efficiency)
        return parent_row_config



class MPRPreschoolEducation(ICDSMixin, MPRData):

    title = '7. Pre-school Education conducted for children 3-6 years'
    slug = 'preschool'
    has_sections = True

    @property
    def rows(self):
        if self.config['location_id']:
            data = self.custom_data(selected_location=self.selected_location, domain=self.config['domain'])
            sections = []
            for section in self.row_config:
                rows = []
                for row in section['rows_config']:
                    row_data = []
                    for idx, cell in enumerate(row):
                        if isinstance(cell, dict):
                            num = 0
                            for c in cell['columns']:
                                num += data.get(c, 0)

                            if 'second_value' in cell:
                                denom = data.get(cell['second_value'], 1)
                                alias_data = cell['func'](num, float(denom or 1))
                                cell_data = "%.1f" % cell['func'](num, float(denom or 1))
                            else:
                                cell_data = num
                                alias_data = num

                            if 'alias' in cell:
                                data[cell['alias']] = alias_data
                            row_data.append(cell_data)
                        elif isinstance(cell, tuple):
                            cell_data = 0
                            for c in cell:
                                cell_data += data.get(c, 0)
                            row_data.append(cell_data)
                        else:
                            row_data.append(data.get(cell, cell if cell == '--' or idx == 0 else 0))
                    rows.append(row_data)
                sections.append(dict(
                    title=section['title'],
                    slug=section['slug'],
                    headers=section['headers'],
                    rows=rows
                ))
            return sections

    @property
    def row_config(self):
        return [
            {
                'title': 'a. Average attendance of children for 16 or more days '
                         'in the reporting month by different categories',
                'slug': 'preschool_1',
                'headers': DataTablesHeader(
                    DataTablesColumn('Category'),
                    DataTablesColumn('Girls'),
                    DataTablesColumn('Boys'),
                    DataTablesColumn('Total')
                ),
                'rows_config': (
                    (
                        _('ST'),
                        'pse_21_days_female_st',
                        'pse_21_days_male_st',
                        {
                            'columns': ('pse_21_days_female_st', 'pse_21_days_male_st'),
                            'alias': '21_days_st'
                        }
                    ),
                    (
                        _('SC'),
                        'pse_21_days_female_sc',
                        'pse_21_days_male_sc',
                        {
                            'columns': ('pse_21_days_female_sc', 'pse_21_days_male_sc'),
                            'alias': '21_days_sc'
                        }
                    ),
                    (
                        _('Other'),
                        'pse_21_days_female_others',
                        'pse_21_days_male_others',
                        {
                            'columns': ('pse_21_days_female_others', 'pse_21_days_male_others'),
                            'alias': '21_days_others'
                        }
                    ),
                    (
                        _('All Categories (Total)'),
                        ('pse_21_days_female_st', 'pse_21_days_female_sc', 'pse_21_days_female_others'),
                        ('pse_21_days_male_st', 'pse_21_days_male_sc', 'pse_21_days_male_others'),
                        ('21_days_st', '21_days_sc', '21_days_others')
                    ),
                    (
                        _('Disabled'),
                        'pse_21_days_female_disabled',
                        'pse_21_days_male_disabled',
                        ('pse_21_days_female_disabled', 'pse_21_days_male_disabled')
                    ),
                    (
                        _('Minority'),
                        'pse_21_days_female_minority',
                        'pse_21_days_male_minority',
                        ('pse_21_days_female_minority', 'pse_21_days_male_minority')
                    )
                )

            },
            {
                'title': 'b. Total Daily Attendance of Children by age category',
                'slug': 'preschool_2',
                'headers': DataTablesHeader(
                    DataTablesColumn('Age Category'),
                    DataTablesColumn('Girls'),
                    DataTablesColumn('Boys'),
                    DataTablesColumn('Total')
                ),
                'rows_config': (
                    (
                        _('3-4 years'),
                        'pse_daily_attendance_female',
                        'pse_daily_attendance_male',
                        {
                            'columns': ('pse_daily_attendance_female', 'pse_daily_attendance_male'),
                            'alias': 'attendance_1'
                        }
                    ),
                    (
                        _('4-5 years'),
                        'pse_daily_attendance_female_1',
                        'pse_daily_attendance_male_1',
                        {
                            'columns': ('pse_daily_attendance_female_1', 'pse_daily_attendance_male_1'),
                            'alias': 'attendance_2'
                        }
                    ),
                    (
                        _('5-6 years'),
                        'pse_daily_attendance_female_2',
                        'pse_daily_attendance_male_2',
                        {
                            'columns': ('pse_daily_attendance_female_2', 'pse_daily_attendance_male_2'),
                            'alias': 'attendance_3'
                        }
                    ),
                    (
                        _('All Children'),
                        (
                            'pse_daily_attendance_female',
                            'pse_daily_attendance_female_1',
                            'pse_daily_attendance_female_2'
                        ),
                        (
                            'pse_daily_attendance_male',
                            'pse_daily_attendance_male_1',
                            'pse_daily_attendance_male_2'
                        ),
                        (
                            'attendance_1',
                            'attendance_2',
                            'attendance_3'
                        )
                    )
                )
            },
            {
                'title': 'c. PSE Attendance Efficiency',
                'slug': 'preschool_3',
                'headers': DataTablesHeader(
                    DataTablesColumn(''),
                    DataTablesColumn('Girls'),
                    DataTablesColumn('Boys'),
                    DataTablesColumn('Total')
                ),
                'rows_config': (
                    (
                        _('I. Annual Population Totals (3-6 years)'),
                        'child_count_female',
                        'child_count_male',
                        {
                            'columns': ('child_count_female', 'child_count_male'),
                            'alias': 'child_count'
                        }
                    ),
                    (
                        _('II. Usual Absentees during the month'),
                        'pse_absent_female',
                        'pse_absent_male',
                        {
                            'columns': ('pse_absent_female', 'pse_absent_male'),
                            'alias': 'absent'
                        }
                    ),
                    (
                        _('III. Total present for at least one day in month'),
                        'pse_partial_female',
                        'pse_partial_male',
                        {
                            'columns': ('pse_partial_female', 'pse_partial_male'),
                            'alias': 'partial'
                        }
                    ),
                    (
                        _('IV. Expected Total Daily Attendance'),
                        {
                            'columns': ('pse_partial_female',),
                            'func': mul,
                            'second_value': 'open_pse_count',
                            'alias': 'expected_attendance_female'
                        },
                        {
                            'columns': ('pse_partial_male',),
                            'func': mul,
                            'second_value': 'open_pse_count',
                            'alias': 'expected_attendance_male'
                        },
                        {
                            'columns': ('partial',),
                            'func': mul,
                            'second_value': 'open_pse_count',
                            'alias': 'expected_attendance'
                        }
                    ),
                    (
                        _('V. Actual Total Daily Attendance'),
                        {
                            'columns': (
                                'pse_daily_attendance_female',
                                'pse_daily_attendance_female_1',
                                'pse_daily_attendance_female_2'
                            ),
                            'alias': 'attendance_female'
                        },
                        {
                            'columns': (
                                'pse_daily_attendance_male',
                                'pse_daily_attendance_male_1',
                                'pse_daily_attendance_male_2'
                            ),
                            'alias': 'attendance_male'
                        },
                        {
                            'columns': (
                                'attendance_female',
                                'attendance_male',
                            ),
                            'alias': 'attendance'
                        }
                    ),
                    (
                        _('VI. PSE Attendance Efficiency'),
                        {
                            'columns': ('attendance_female',),
                            'func': truediv,
                            'second_value': 'expected_attendance_female',
                            'format': 'percent',
                        },
                        {
                            'columns': 'attendance_male',
                            'func': truediv,
                            'second_value': 'expected_attendance_male',
                            'format': 'percent',
                        },
                        {
                            'columns': 'attendance',
                            'func': truediv,
                            'second_value': 'expected_attendance',
                            'format': 'percent',
                        }
                    )
                )
            },
            {
                'title': 'd. PSE Activities',
                'slug': 'preschool_4',
                'headers': [],
                'rows_config': (
                    (
                        _('I. Average no. of days on which at least four PSE activities were conducted at AWCs'),
                        'open_four_acts_count'
                    ),
                    (
                        _('II. Average no. of days on which any PSE activity was conducted at AWCs'),
                        'open_one_acts_count'
                    )
                )
            }
        ]


class MPRPreschoolEducationBeta(ICDSMixin, MPRData):

    title = '7. Pre-school Education conducted for children 3-6 years'
    slug = 'preschool'
    has_sections = True

    @property
    def rows(self):
        if self.config['location_id']:

            filters = get_location_filter(self.config['location_id'], self.config['domain'])
            if filters.get('aggregation_level') > 1:
                filters['aggregation_level'] -= 1

            filters['month'] = date(self.config['year'], self.config['month'], 1)

            data = self.get_child_data(filters)
            data.update(self.get_activity_data(filters))

            sections = []
            for section in self.row_config:

                rows = []
                for row in section['rows_config']:
                    row_data = []
                    for idx, cell in enumerate(row):
                        if isinstance(cell, dict):
                            num = 0
                            for c in cell['columns']:
                                num += data.get(c, 0)

                            if 'second_value' in cell:
                                denom = data.get(cell['second_value'], 1)
                                alias_data = cell['func'](num, float(denom or 1))
                                cell_data = "%.2f" % cell['func'](num, float(denom or 1))
                                if cell.get('format') == 'percent':
                                    cell_data = f"{cell_data}%"
                            else:
                                cell_data = num
                                alias_data = num

                            if 'alias' in cell:
                                data[cell['alias']] = alias_data
                            row_data.append(cell_data)
                        elif isinstance(cell, tuple):
                            cell_data = 0
                            for c in cell:
                                cell_data += data.get(c, 0)
                            row_data.append(cell_data)
                        else:
                            row_data.append(data.get(cell, cell if cell == '--' or idx == 0 else 0))
                    rows.append(row_data)
                sections.append(dict(
                    title=section['title'],
                    slug=section['slug'],
                    headers=section['headers'],
                    rows=rows
                ))
            return sections

    def get_child_data(self, filters):
        child_data = AggChildHealth.objects.filter(**filters).values('month').annotate(
            pse_attendance_f_3_4=Sum(Case(When(gender='F',
                                               age_tranche__in=['36', '48'],
                                               then='total_pse_days_attended',
                                               ), default=Value(0))),
            pse_attendance_m_3_4=Sum(Case(When(gender='M',
                                               age_tranche__in=['36', '48'],
                                               then='total_pse_days_attended',
                                               ), default=Value(0))),
            pse_attendance_f_4_5=Sum(Case(When(gender='F',
                                               age_tranche__in=['60'],
                                               then='total_pse_days_attended',
                                               ), default=Value(0))),
            pse_attendance_m_4_5=Sum(Case(When(gender='M',
                                               age_tranche__in=['60'],
                                               then='total_pse_days_attended',
                                               ), default=Value(0))),
            pse_attendance_f_5_6=Sum(Case(When(gender='F',
                                               age_tranche__in=['72'],
                                               then='total_pse_days_attended',
                                               ), default=Value(0))),
            pse_attendance_m_5_6=Sum(Case(When(gender='M',
                                               age_tranche__in=['72'],
                                               then='total_pse_days_attended',
                                               ), default=Value(0))),
            pse_eligible_f=Sum(Case(When(gender='F',
                                         then='pse_eligible',
                                         ), default=Value(0))),
            pse_eligible_m=Sum(Case(When(gender='M',
                                         then='pse_eligible',
                                         ), default=Value(0))),
            pse_attended_0_days_f=Sum(Case(When(gender='F',
                                                then='pse_attended_0_days',
                                                ), default=Value(0))),
            pse_attended_0_days_m=Sum(Case(When(gender='M',
                                                then='pse_attended_0_days',
                                                ), default=Value(0))),
            pse_attended_1_days_f=Sum(Case(When(gender='F',
                                                then='pse_attended_1_days',
                                                ), default=Value(0))),
            pse_attended_1_days_m=Sum(Case(When(gender='M',
                                                then='pse_attended_1_days',
                                                ), default=Value(0))),
            total_pse_days_attended_f=Sum(Case(When(gender='F',
                                                    then='total_pse_days_attended',
                                                    ), default=Value(0))),
            total_pse_days_attended_m=Sum(Case(When(gender='M',
                                                    then='total_pse_days_attended',
                                                    ), default=Value(0))),
            pse_attended_16_days_st_f=Sum(Case(When(gender='F',
                                                    then='pse_attended_16_days_st',
                                                    ), default=Value(0))),
            pse_attended_16_days_st_M=Sum(Case(When(gender='M',
                                                    then='pse_attended_16_days_st',
                                                    ), default=Value(0))),
            pse_attended_16_days_sc_f=Sum(Case(When(gender='F',
                                                    then='pse_attended_16_days_sc',
                                                    ), default=Value(0))),
            pse_attended_16_days_sc_m=Sum(Case(When(gender='M',
                                                    then='pse_attended_16_days_sc',
                                                    ), default=Value(0))),
            pse_attended_16_days_other_f=Sum(Case(When(gender='F',
                                                       then='pse_attended_16_days_other',
                                                       ), default=Value(0))),
            pse_attended_16_days_other_m=Sum(Case(When(gender='M',
                                                       then='pse_attended_16_days_other',
                                                       ), default=Value(0))),
            pse_attended_16_days_disabled_f=Sum(Case(When(gender='F',
                                                          then='pse_attended_16_days_disabled',
                                                          ), default=Value(0))),
            pse_attended_16_days_disabled_m=Sum(Case(When(gender='M',
                                                          then='pse_attended_16_days_disabled',
                                                          ), default=Value(0))),
            pse_attended_16_days_minority_f=Sum(Case(When(gender='F',
                                                          then='pse_attended_16_days_minority',
                                                          ), default=Value(0))),
            pse_attended_16_days_minority_m=Sum(Case(When(gender='M',
                                                          then='pse_attended_16_days_minority',
                                                          ), default=Value(0))),
        ).order_by('month').first()

        if not child_data:
            return {}
        attendance_total_data_map = {
            'total_pse_attendance_3_4': ['pse_attendance_f_3_4', 'pse_attendance_m_3_4'],
            'total_pse_attendance_4_5': ['pse_attendance_f_4_5', 'pse_attendance_m_4_5'],
            'total_pse_attendance_5_6': ['pse_attendance_f_5_6', 'pse_attendance_m_5_6'],
            'total_pse_f': ['pse_attendance_f_3_4', 'pse_attendance_f_4_5', 'pse_attendance_f_5_6'],
            'total_pse_m': ['pse_attendance_m_3_4', 'pse_attendance_m_4_5', 'pse_attendance_m_5_6'],
            'total_pse_attendance': ['pse_attendance_f_3_4', 'pse_attendance_m_3_4',
                                     'pse_attendance_f_4_5', 'pse_attendance_m_4_5',
                                     'pse_attendance_f_5_6', 'pse_attendance_m_5_6'],
        }

        for total_attendance_key, total_attendance_summation_cols in attendance_total_data_map.items():
            child_data[total_attendance_key] = sum([child_data[key] for key in total_attendance_summation_cols])
        return child_data

    def get_activity_data(self, filters):
        data = AggAwc.objects.filter(**filters).values(
            'num_launched_awcs',
            'num_days_4_pse_activities',
            'num_days_1_pse_activities',
            'awc_days_open'
        ).order_by('month').first()

        if not data:
            return {}

        pse_4 = data['num_days_4_pse_activities'] if data['num_days_4_pse_activities']  else 0
        pse_1 = data['num_days_1_pse_activities'] if data['num_days_1_pse_activities'] else 0
        awc_open = data['awc_days_open'] if data['awc_days_open'] else 0
        if data['num_launched_awcs']:
            avg_pse_4 = pse_4 / data['num_launched_awcs']
            avg_pse_1 = pse_1 / data['num_launched_awcs']
            avg_awc_open = awc_open / data['num_launched_awcs']
        return {
            'open_four_acts_count': avg_pse_4,
            'open_one_acts_count': avg_pse_1,
            'open_pse_count': avg_awc_open
        }

    @property
    def row_config(self):
        return [
            {
                'title': 'a. Average attendance of children for 25 or more days '
                         'in the reporting month by different categories',
                'slug': 'preschool_1',
                'headers': DataTablesHeader(
                    DataTablesColumn('Category'),
                    DataTablesColumn('Girls'),
                    DataTablesColumn('Boys'),
                    DataTablesColumn('Total')
                ),
                'rows_config': (
                    (
                        _('ST'),
                        'pse_attended_16_days_st_f',
                        'pse_attended_16_days_st_m',
                        {
                            'columns': ('pse_attended_16_days_st_f', 'pse_attended_16_days_st_m'),
                            'alias': '21_days_st'
                        }
                    ),
                    (
                        _('SC'),
                        'pse_attended_16_days_sc_f',
                        'pse_attended_16_days_sc_m',
                        {
                            'columns': ('pse_attended_16_days_sc_f', 'pse_attended_16_days_sc_m'),
                            'alias': '21_days_sc'
                        }
                    ),
                    (
                        _('Other'),
                        'pse_attended_16_days_other_f',
                        'pse_attended_16_days_other_m',
                        {
                            'columns': ('pse_attended_16_days_other_f', 'pse_attended_16_days_other_m'),
                            'alias': '21_days_others'
                        }
                    ),
                    (
                        _('All Categories (Total)'),
                        ('pse_attended_16_days_st_f', 'pse_attended_16_days_sc_f', 'pse_attended_16_days_other_f'),
                        ('pse_attended_16_days_st_m', 'pse_attended_16_days_sc_m', 'pse_attended_16_days_other_m'),
                        ('21_days_st', '21_days_sc', '21_days_others')
                    ),
                    (
                        _('Disabled'),
                        'pse_attended_16_days_disabled_f',
                        'pse_attended_16_days_disabled_m',
                        ('pse_attended_16_days_disabled_f', 'pse_attended_16_days_disabled_m')
                    ),
                    (
                        _('Minority'),
                        'pse_attended_16_days_minority_f',
                        'pse_attended_16_days_minority_m',
                        ('pse_attended_16_days_minority_f', 'pse_attended_16_days_minority_m')
                    )
                )

            },
            {
                'title': 'b. Total Daily Attendance of Children by age category',
                'slug': 'preschool_2',
                'headers': DataTablesHeader(
                    DataTablesColumn('Age Category'),
                    DataTablesColumn('Girls'),
                    DataTablesColumn('Boys'),
                    DataTablesColumn('Total')
                ),
                'rows_config': (
                    (
                        _('3-4 years'),
                        'pse_attendance_f_3_4',
                        'pse_attendance_m_3_4',
                        'total_pse_attendance_3_4'
                    ),
                    (
                        _('4-5 years'),
                        'pse_attendance_f_4_5',
                        'pse_attendance_f_4_5',
                        'total_pse_attendance_4_5'
                    ),
                    (
                        _('5-6 years'),
                        'pse_attendance_f_5_6',
                        'pse_attendance_f_5_6',
                        'total_pse_attendance_5_6'
                    ),
                    (
                        _('All Children'),
                        'total_pse_f',
                        'total_pse_m',
                        'total_pse_attendance'
                    )
                )
            },
            {
                'title': 'c. PSE Attendance Efficiency',
                'slug': 'preschool_3',
                'headers': DataTablesHeader(
                    DataTablesColumn(''),
                    DataTablesColumn('Girls'),
                    DataTablesColumn('Boys'),
                    DataTablesColumn('Total')
                ),
                'rows_config': (
                    (
                        _('I. Annual Population Totals (3-6 years)'),
                        'pse_eligible_f',
                        'pse_eligible_m',
                        {
                            'columns': ('pse_eligible_f', 'pse_eligible_m'),
                            'alias': 'child_count'
                        }
                    ),
                    (
                        _('II. Usual Absentees during the month'),
                        'pse_attended_0_days_f',
                        'pse_attended_0_days_m',
                        {
                            'columns': ('pse_attended_0_days_f', 'pse_attended_0_days_m'),
                            'alias': 'absent'
                        }
                    ),
                    (
                        _('III. Total present for at least one day in month'),
                        'pse_attended_1_days_f',
                        'pse_attended_1_days_m',
                        {
                            'columns': ('pse_attended_1_days_f', 'pse_attended_1_days_m'),
                            'alias': 'partial'
                        }
                    ),
                    (
                        _('IV. Expected Total Daily Attendance'),
                        {
                            'columns': ('pse_attended_1_days_f',),
                            'func': mul,
                            'second_value': 'open_pse_count',
                            'alias': 'expected_attendance_female'
                        },
                        {
                            'columns': ('pse_attended_1_days_m',),
                            'func': mul,
                            'second_value': 'open_pse_count',
                            'alias': 'expected_attendance_male'
                        },
                        {
                            'columns': ('partial',),
                            'func': mul,
                            'second_value': 'open_pse_count',
                            'alias': 'expected_attendance'
                        }
                    ),
                    (
                        _('V. Actual Total Daily Attendance'),
                            'total_pse_days_attended_f',
                            'total_pse_days_attended_m',
                        {
                            'columns': (
                                'total_pse_days_attended_f',
                                'total_pse_days_attended_m',
                            ),
                            'alias': 'attendance'
                        }
                    ),
                    (
                        _('VI. PSE Attendance Efficiency'),
                        {
                            'columns': ('total_pse_days_attended_f',),
                            'func': _get_percent,
                            'second_value': 'expected_attendance_female',
                            'format': 'percent',
                        },
                        {
                            'columns': ('total_pse_days_attended_m',),
                            'func': _get_percent,
                            'second_value': 'expected_attendance_male',
                            'format': 'percent',
                        },
                        {
                            'columns': ('attendance',),
                            'func': _get_percent,
                            'second_value': 'expected_attendance',
                            'format': 'percent',
                        }
                    )
                )
            },
            {
                'title': 'd. PSE Activities',
                'slug': 'preschool_4',
                'headers': [],
                'rows_config': (
                    (
                        _('I. Average no. of days on which at least four PSE activities were conducted at AWCs'),
                        'open_four_acts_count'
                    ),
                    (
                        _('II. Average no. of days on which any PSE activity was conducted at AWCs'),
                        'open_one_acts_count'
                    )
                )
            }
        ]



class MPRGrowthMonitoring(ICDSMixin, MPRData):

    title = '8. Growth Monitoring and Classification of Nutritional Status of Children ' \
            '(as per growth chart based on the WHO Child Growth Standards)'
    slug = 'growth_monitoring'

    @property
    def rows(self):
        if self.config['location_id']:
            data = self.custom_data(selected_location=self.selected_location, domain=self.config['domain'])
            rows = []
            for row in self.row_config:
                row_data = []
                for idx, cell in enumerate(row):
                    if isinstance(cell, dict):
                        num = 0
                        if 'second_value' in cell:
                            for c in cell['columns']:
                                num += data.get(c, 0)
                            denom = data.get(cell['second_value'], 1)
                            alias_data = cell['func'](num, float(denom or 1))
                            cell_data = "%.1f" % cell['func'](num, float(denom or 1))
                        else:
                            for c in cell['columns']:
                                if 'func' in cell:
                                    if num == 0:
                                        num = data.get(c)
                                    else:
                                        num = cell['func'](num, data.get(c, 0))
                                else:
                                    num += data.get(c, 0)
                            cell_data = num
                            alias_data = num

                        if 'alias' in cell:
                            data[cell['alias']] = alias_data
                        row_data.append(cell_data)
                    elif isinstance(cell, tuple):
                        cell_data = 0
                        for c in cell:
                            cell_data += data.get(c, 0)
                        row_data.append(cell_data)
                    else:
                        row_data.append(data.get(cell, cell if cell == '--' or idx in [0, 1] else 0))
                rows.append(row_data)

            return rows

    @property
    def headers(self):
        return DataTablesHeader(
            DataTablesColumn(''),
            DataTablesColumn(''),
            DataTablesColumnGroup(
                '0 m - 1 yr',
                DataTablesColumn('Girls'),
                DataTablesColumn('Boys')
            ),
            DataTablesColumnGroup(
                '1 yr - 3 yrs',
                DataTablesColumn('Girls'),
                DataTablesColumn('Boys')
            ),
            DataTablesColumnGroup(
                '3 yrs - 5 yrs',
                DataTablesColumn('Girls'),
                DataTablesColumn('Boys')
            ),
            DataTablesColumnGroup(
                'All Children',
                DataTablesColumn('Girls'),
                DataTablesColumn('Boys'),
                DataTablesColumn('Total')
            )
        )

    @property
    def row_config(self):
        return (
            (
                'I. Total number of children weighed',
                '',
                'F_resident_weighed_count',
                'M_resident_weighed_count',
                'F_resident_weighed_count_1',
                'M_resident_weighed_count_1',
                'F_resident_weighed_count_2',
                'M_resident_weighed_count_2',
                {
                    'columns': (
                        'F_resident_weighed_count',
                        'F_resident_weighed_count_1',
                        'F_resident_weighed_count_2'
                    ),
                    'alias': 'resident_female_weight'
                },
                {
                    'columns': (
                        'M_resident_weighed_count',
                        'M_resident_weighed_count_1',
                        'M_resident_weighed_count_2'
                    ),
                    'alias': 'resident_male_weight'
                },
                {
                    'columns': ('resident_female_weight', 'resident_male_weight'),
                    'alias': 'all_resident_weight'
                },
            ),
            (
                'II. Annual Population Totals',
                '',
                'child_count_female',
                'child_count_male',
                'child_count_female_1',
                'child_count_male_1',
                'child_count_female_2',
                'child_count_male_2',
                {
                    'columns': ('child_count_female', 'child_count_female_1', 'child_count_female_2'),
                    'alias': 'child_female'
                },
                {
                    'columns': ('child_count_male', 'child_count_male_1', 'child_count_male_2'),
                    'alias': 'child_male'
                },
                {
                    'columns': ('child_female', 'child_male'),
                    'alias': 'all_child'
                },
            ),
            (
                'III. Weighing Efficiency (%)',
                '',
                {
                    'columns': ('F_resident_weighed_count',),
                    'func': truediv,
                    'second_value': 'child_count_female'
                },
                {
                    'columns': ('M_resident_weighed_count',),
                    'func': truediv,
                    'second_value': 'child_count_male'
                },
                {
                    'columns': ('F_resident_weighed_count_1',),
                    'func': truediv,
                    'second_value': 'child_count_female_1'
                },
                {
                    'columns': ('M_resident_weighed_count_1',),
                    'func': truediv,
                    'second_value': 'child_count_male_1'
                },
                {
                    'columns': ('F_resident_weighed_count_2',),
                    'func': truediv,
                    'second_value': 'child_count_female_2'
                },
                {
                    'columns': ('M_resident_weighed_count_2',),
                    'func': truediv,
                    'second_value': 'child_count_male_2'
                },
                {
                    'columns': ('resident_female_weight',),
                    'func': truediv,
                    'second_value': 'child_female'
                },
                {
                    'columns': ('resident_male_weight',),
                    'func': truediv,
                    'second_value': 'child_male'
                },
                {
                    'columns': ('all_resident_weight',),
                    'func': truediv,
                    'second_value': 'all_child'
                }
            ),
            (
                'IV. Out of the children weighed, no of children found:',
            ),
            (
                'a. Normal (Green)',
                'Num',
                {
                    'columns': (
                        'F_resident_weighed_count',
                        'F_mod_resident_weighed_count',
                        'F_sev_resident_weighed_count'
                    ),
                    'func': sub,
                    'alias': 'F_sub_weight'
                },
                {
                    'columns': (
                        'M_resident_weighed_count',
                        'M_mod_resident_weighed_count',
                        'M_sev_resident_weighed_count'
                    ),
                    'func': sub,
                    'alias': 'M_sub_weight'
                },
                {
                    'columns': (
                        'F_resident_weighed_count_1',
                        'F_mod_resident_weighed_count_1',
                        'F_sev_resident_weighed_count_1'
                    ),
                    'func': sub,
                    'alias': 'F_sub_weight_1'
                },
                {
                    'columns': (
                        'M_resident_weighed_count_1',
                        'M_mod_resident_weighed_count_1',
                        'M_sev_resident_weighed_count_1'
                    ),
                    'func': sub,
                    'alias': 'M_sub_weight_1'
                },
                {
                    'columns': (
                        'F_resident_weighed_count_2',
                        'F_mod_resident_weighed_count_2',
                        'F_sev_resident_weighed_count_2'
                    ),
                    'func': sub,
                    'alias': 'F_sub_weight_2'
                },
                {
                    'columns': (
                        'M_resident_weighed_count_2',
                        'M_mod_resident_weighed_count_2',
                        'M_sev_resident_weighed_count_2'
                    ),
                    'func': sub,
                    'alias': 'M_sub_weight_2'
                },
                {
                    'columns': ('F_sub_weight', 'F_sub_weight_1', 'F_sub_weight_2'),
                    'alias': 'all_F_sub_weight'
                },
                {
                    'columns': ('M_sub_weight', 'M_sub_weight_1', 'M_sub_weight_2'),
                    'alias': 'all_M_sub_weight'
                },
                {
                    'columns': ('all_F_sub_weight', 'all_M_sub_weight'),
                    'alias': 'all_sub_weight'
                }
            ),
            (
                '',
                '%',
                {
                    'columns': ('F_sub_weight',),
                    'func': truediv,
                    'second_value': 'F_resident_weighed_count',
                },
                {
                    'columns': ('M_sub_weight',),
                    'func': truediv,
                    'second_value': 'M_resident_weighed_count',
                },
                {
                    'columns': ('F_sub_weight_1',),
                    'func': truediv,
                    'second_value': 'F_resident_weighed_count_1',
                },
                {
                    'columns': ('M_sub_weight_1',),
                    'func': truediv,
                    'second_value': 'M_resident_weighed_count_1',
                },
                {
                    'columns': ('F_sub_weight_2',),
                    'func': truediv,
                    'second_value': 'F_resident_weighed_count_2',
                },
                {
                    'columns': ('M_sub_weight_2',),
                    'func': truediv,
                    'second_value': 'M_resident_weighed_count_2',
                },
                {
                    'columns': ('all_F_sub_weight',),
                    'func': truediv,
                    'second_value': 'resident_female_weight'
                },
                {
                    'columns': ('all_M_sub_weight',),
                    'func': truediv,
                    'second_value': 'resident_male_weight'
                },
                {
                    'columns': ('all_sub_weight',),
                    'func': truediv,
                    'second_value': 'all_resident_weight'
                },
            ),
            (
                'b. Moderately underweight (Yellow)',
                'Num',
                'F_mod_resident_weighed_count',
                'M_mod_resident_weighed_count',
                'F_mod_resident_weighed_count_1',
                'M_mod_resident_weighed_count_1',
                'F_mod_resident_weighed_count_2',
                'M_mod_resident_weighed_count_2',
                {
                    'columns': (
                        'F_mod_resident_weighed_count',
                        'F_mod_resident_weighed_count_1',
                        'F_mod_resident_weighed_count_2'),
                    'alias': 'F_sum_mod_resident_weighted'
                },
                {
                    'columns': (
                        'M_mod_resident_weighed_count',
                        'M_mod_resident_weighed_count_1',
                        'M_mod_resident_weighed_count_2'),
                    'alias': 'M_sum_mod_resident_weighted'
                },
                {
                    'columns': ('F_sum_mod_resident_weighted', 'M_sum_mod_resident_weighted'),
                    'alias': 'all_mod_resident_weighted'
                }
            ),
            (
                '',
                '%',
                {
                    'columns': ('F_mod_resident_weighed_count',),
                    'func': truediv,
                    'second_value': 'F_resident_weighed_count',
                },
                {
                    'columns': ('M_mod_resident_weighed_count',),
                    'func': truediv,
                    'second_value': 'M_resident_weighed_count',
                },
                {
                    'columns': ('F_mod_resident_weighed_count_1',),
                    'func': truediv,
                    'second_value': 'F_resident_weighed_count_1',
                },
                {
                    'columns': ('M_mod_resident_weighed_count_1',),
                    'func': truediv,
                    'second_value': 'M_resident_weighed_count_1',
                },
                {
                    'columns': ('F_mod_resident_weighed_count_2',),
                    'func': truediv,
                    'second_value': 'F_resident_weighed_count_2',
                },
                {
                    'columns': ('M_mod_resident_weighed_count_2',),
                    'func': truediv,
                    'second_value': 'M_resident_weighed_count_2',
                },
                {
                    'columns': ('F_sum_mod_resident_weighted',),
                    'func': truediv,
                    'second_value': 'resident_female_weight'
                },
                {
                    'columns': ('M_sum_mod_resident_weighted',),
                    'func': truediv,
                    'second_value': 'resident_male_weight'
                },
                {
                    'columns': ('all_mod_resident_weighted',),
                    'func': truediv,
                    'second_value': 'all_resident_weight'
                },
            ),
            (
                'Severely Underweight (Orange)',
                'Num',
                'F_sev_resident_weighed_count',
                'M_sev_resident_weighed_count',
                'F_sev_resident_weighed_count_1',
                'M_sev_resident_weighed_count_1',
                'F_sev_resident_weighed_count_2',
                'M_sev_resident_weighed_count_2',
                {
                    'columns': (
                        'F_sev_resident_weighed_count',
                        'F_sev_resident_weighed_count_1',
                        'F_sev_resident_weighed_count_2'),
                    'alias': 'F_sum_sev_resident_weighted'
                },
                {
                    'columns': (
                        'M_sev_resident_weighed_count',
                        'M_sev_resident_weighed_count_1',
                        'M_sev_resident_weighed_count_2'),
                    'alias': 'M_sev_mod_resident_weighted'
                },
                {
                    'columns': ('F_sum_sev_resident_weighted', 'M_sev_mod_resident_weighted'),
                    'alias': 'all_sev_resident_weighted'
                }
            ),
            (
                '',
                '%',
                {
                    'columns': ('F_sev_resident_weighed_count',),
                    'func': truediv,
                    'second_value': 'F_resident_weighed_count',
                },
                {
                    'columns': ('M_sev_resident_weighed_count',),
                    'func': truediv,
                    'second_value': 'M_resident_weighed_count',
                },
                {
                    'columns': ('F_sev_resident_weighed_count_1',),
                    'func': truediv,
                    'second_value': 'F_resident_weighed_count_1',
                },
                {
                    'columns': ('M_sev_resident_weighed_count_1',),
                    'func': truediv,
                    'second_value': 'M_resident_weighed_count_1',
                },
                {
                    'columns': ('F_sev_resident_weighed_count_2',),
                    'func': truediv,
                    'second_value': 'F_resident_weighed_count_2',
                },
                {
                    'columns': ('M_sev_resident_weighed_count_2',),
                    'func': truediv,
                    'second_value': 'M_resident_weighed_count_2',
                },
                {
                    'columns': ('F_sum_sev_resident_weighted',),
                    'func': truediv,
                    'second_value': 'resident_female_weight'
                },
                {
                    'columns': ('M_sum_sev_resident_weighted',),
                    'func': truediv,
                    'second_value': 'resident_male_weight'
                },
                {
                    'columns': ('all_sev_resident_weighted',),
                    'func': truediv,
                    'second_value': 'all_resident_weight'
                },
            )
        )


class MPRGrowthMonitoringBeta(ICDSMixin, MPRData):
    title = '8. Growth Monitoring and Classification of Nutritional Status of Children ' \
            '(as per growth chart based on the WHO Child Growth Standards)'
    slug = 'growth_monitoring'

    @property
    def headers(self):
        return DataTablesHeader(
            DataTablesColumn(''),
            DataTablesColumn(''),
            DataTablesColumnGroup(
                '0 m - 1 yr',
                DataTablesColumn('Girls'),
                DataTablesColumn('Boys')
            ),
            DataTablesColumnGroup(
                '1 yr - 3 yrs',
                DataTablesColumn('Girls'),
                DataTablesColumn('Boys')
            ),
            DataTablesColumnGroup(
                '3 yrs - 5 yrs',
                DataTablesColumn('Girls'),
                DataTablesColumn('Boys')
            ),
            DataTablesColumnGroup(
                'All Children',
                DataTablesColumn('Girls'),
                DataTablesColumn('Boys'),
                DataTablesColumn('Total')
            )
        )

    @property
    def rows(self):
        if self.config['location_id']:
            filters = get_location_filter(self.config['location_id'], self.config['domain'])
            if filters.get('aggregation_level') > 1:
                filters['aggregation_level'] -= 1

            filters['month'] = date(self.config['year'], self.config['month'], 1)
            data = self.get_child_gm_data(filters)
            rows = []
            for row in self.row_config:
                row_data = []
                for idx, cell in enumerate(row):
                    if isinstance(cell, dict):
                        num = 0
                        if 'second_value' in cell:
                            for c in cell['columns']:
                                num += data.get(c, 0)
                            denom = data.get(cell['second_value'], 1)
                            alias_data = cell['func'](num, float(denom or 1))
                            cell_data = "%.1f" % cell['func'](num, float(denom or 1))
                        else:
                            for c in cell['columns']:
                                if 'func' in cell:
                                    if num == 0:
                                        num = data.get(c)
                                    else:
                                        num = cell['func'](num, data.get(c, 0))
                                else:
                                    num += data.get(c, 0)
                            cell_data = num
                            alias_data = num

                        if 'alias' in cell:
                            data[cell['alias']] = alias_data
                        row_data.append(cell_data)
                    elif isinstance(cell, tuple):
                        cell_data = 0
                        for c in cell:
                            cell_data += data.get(c, 0)
                        row_data.append(cell_data)
                    else:
                        row_data.append(data.get(cell, cell if cell == '--' or idx in [0, 1] else 0))
                rows.append(row_data)

            return rows

    def get_child_gm_data(self, filters):
        child_data = AggChildHealth.objects.filter(**filters).values('gender').annotate(
            weighed_count=Sum(Case(When(age_tranche__in=['0', '6', '12'],
                                  then='nutrition_status_weighed',
                                  ), default=Value(0))),
            weighed_count_1=Sum(Case(When(age_tranche__in=['24', '36'],
                                    then='nutrition_status_weighed',
                                    ), default=Value(0))),
            weighed_count_2=Sum(Case(When(age_tranche__in=['48', '60'],
                                    then='nutrition_status_weighed',
                                    ), default=Value(0))),
            child_count=Sum(Case(When(age_tranche__in=['0', '6', '12'],
                                      then='valid_in_month',
                                      ), default=Value(0))),
            child_count_1=Sum(Case(When(age_tranche__in=['24', '36'],
                                        then='valid_in_month',
                                        ), default=Value(0))),
            child_count_2=Sum(Case(When(age_tranche__in=['48', '60'],
                                        then='valid_in_month',
                                        ), default=Value(0))),
            sub_weighed_count=Sum(Case(When(age_tranche__in=['0', '6', '12'],
                                     then='nutrition_status_normal',
                                     ), default=Value(0))),
            sub_weighed_count_1=Sum(Case(When(age_tranche__in=['24', '36'],
                                       then='nutrition_status_normal',
                                       ), default=Value(0))),
            sub_weighed_count_2=Sum(Case(When(age_tranche__in=['48', '60'],
                                       then='nutrition_status_normal',
                                       ), default=Value(0))),
            mod_weighed_count=Sum(Case(When(age_tranche__in=['0', '6', '12'],
                                            then='nutrition_status_moderately_underweight',
                                            ), default=Value(0))),
            mod_weighed_count_1=Sum(Case(When(age_tranche__in=['24', '36'],
                                              then='nutrition_status_moderately_underweight',
                                              ), default=Value(0))),
            mod_weighed_count_2=Sum(Case(When(age_tranche__in=['48', '60'],
                                              then='nutrition_status_moderately_underweight',
                                              ), default=Value(0))),
            sev_weighed_count=Sum(Case(When(age_tranche__in=['0', '6', '12'],
                                            then='nutrition_status_severely_underweight',
                                            ), default=Value(0))),
            sev_weighed_count_1=Sum(Case(When(age_tranche__in=['24', '36'],
                                              then='nutrition_status_severely_underweight',
                                              ), default=Value(0))),
            sev_weighed_count_2=Sum(Case(When(age_tranche__in=['48', '60'],
                                              then='nutrition_status_severely_underweight',
                                              ), default=Value(0))),
        )
        data = dict()

        for row in child_data:
            data.update({
                f"{row['gender']}_{key}": value
                for key, value in row.items()
            })
        return data

    @property
    def row_config(self):
        return (
            (
                'I. Total number of children weighed',
                '',
                'F_weighed_count',
                'M_weighed_count',
                'F_weighed_count_1',
                'M_weighed_count_1',
                'F_weighed_count_2',
                'M_weighed_count_2',
                {
                    'columns': (
                        'F_weighed_count',
                        'F_weighed_count_1',
                        'F_weighed_count_2'
                    ),
                    'alias': 'resident_female_weight'
                },
                {
                    'columns': (
                        'M_weighed_count',
                        'M_weighed_count_1',
                        'M_weighed_count_2'
                    ),
                    'alias': 'resident_male_weight'
                },
                {
                    'columns': ('resident_female_weight', 'resident_male_weight'),
                    'alias': 'all_resident_weight'
                },
            ),
            (
                'II. Annual Population Totals',
                '',
                'F_child_count',
                'M_child_count',
                'F_child_count_1',
                'M_child_count_1',
                'F_child_count_2',
                'M_child_count_2',
                {
                    'columns': ('F_child_count', 'F_child_count_1', 'F_child_count_2'),
                    'alias': 'child_female'
                },
                {
                    'columns': ('M_child_count', 'M_child_count_1', 'M_child_count_2'),
                    'alias': 'child_male'
                },
                {
                    'columns': ('child_female', 'child_male'),
                    'alias': 'all_child'
                },
            ),
            (
                'III. Weighing Efficiency (%)',
                '',
                {
                    'columns': ('F_weighed_count',),
                    'func': truediv,
                    'second_value': 'F_child_count'
                },
                {
                    'columns': ('M_weighed_count',),
                    'func': truediv,
                    'second_value': 'M_child_count'
                },
                {
                    'columns': ('F_weighed_count_1',),
                    'func': truediv,
                    'second_value': 'F_child_count_1'
                },
                {
                    'columns': ('M_weighed_count_1',),
                    'func': truediv,
                    'second_value': 'M_child_count_1'
                },
                {
                    'columns': ('F_weighed_count_2',),
                    'func': truediv,
                    'second_value': 'F_child_count_2'
                },
                {
                    'columns': ('M_weighed_count_2',),
                    'func': truediv,
                    'second_value': 'M_child_count_2'
                },
                {
                    'columns': ('resident_female_weight',),
                    'func': truediv,
                    'second_value': 'child_female'
                },
                {
                    'columns': ('resident_male_weight',),
                    'func': truediv,
                    'second_value': 'child_male'
                },
                {
                    'columns': ('all_resident_weight',),
                    'func': truediv,
                    'second_value': 'all_child'
                }
            ),
            (
                'IV. Out of the children weighed, no of children found:',
            ),
            (
                'a. Normal (Green)',
                'Num',
                'F_sub_weighed_count',
                'M_sub_weighed_count',
                'F_sub_weighed_count_1',
                'M_sub_weighed_count_1',
                'F_sub_weighed_count_2',
                'M_sub_weighed_count_2',
                {
                    'columns': ('F_sub_weighed_count', 'F_sub_weighed_count_1', 'F_sub_weighed_count_2'),
                    'alias': 'all_F_sub_weight'
                },
                {
                    'columns': ('M_sub_weighed_count', 'M_sub_weighed_count_1', 'M_sub_weighed_count_2'),
                    'alias': 'all_M_sub_weight'
                },
                {
                    'columns': ('all_F_sub_weight', 'all_M_sub_weight'),
                    'alias': 'all_sub_weight'
                }
            ),
            (
                '',
                '%',
                {
                    'columns': ('F_sub_weighed_count',),
                    'func': truediv,
                    'second_value': 'F_weighed_count',
                },
                {
                    'columns': ('M_sub_weighed_count',),
                    'func': truediv,
                    'second_value': 'M_weighed_count',
                },
                {
                    'columns': ('F_sub_weighed_count_1',),
                    'func': truediv,
                    'second_value': 'F_weighed_count_1',
                },
                {
                    'columns': ('M_sub_weighed_count_1',),
                    'func': truediv,
                    'second_value': 'M_weighed_count_1',
                },
                {
                    'columns': ('F_sub_weighed_count_2',),
                    'func': truediv,
                    'second_value': 'F_weighed_count_2',
                },
                {
                    'columns': ('M_sub_weighed_count_2',),
                    'func': truediv,
                    'second_value': 'M_weighed_count_2',
                },
                {
                    'columns': ('all_F_sub_weight',),
                    'func': truediv,
                    'second_value': 'resident_female_weight'
                },
                {
                    'columns': ('all_M_sub_weight',),
                    'func': truediv,
                    'second_value': 'resident_male_weight'
                },
                {
                    'columns': ('all_sub_weight',),
                    'func': truediv,
                    'second_value': 'all_resident_weight'
                },
            ),
            (
                'b. Moderately underweight (Yellow)',
                'Num',
                'F_mod_weighed_count',
                'M_mod_weighed_count',
                'F_mod_weighed_count_1',
                'M_mod_weighed_count_1',
                'F_mod_weighed_count_2',
                'M_mod_weighed_count_2',
                {
                    'columns': (
                        'F_mod_weighed_count',
                        'F_mod_weighed_count_1',
                        'F_mod_weighed_count_2'),
                    'alias': 'F_sum_mod_resident_weighted'
                },
                {
                    'columns': (
                        'M_mod_weighed_count',
                        'M_mod_weighed_count_1',
                        'M_mod_weighed_count_2'),
                    'alias': 'M_sum_mod_resident_weighted'
                },
                {
                    'columns': ('F_sum_mod_resident_weighted', 'M_sum_mod_resident_weighted'),
                    'alias': 'all_mod_resident_weighted'
                }
            ),
            (
                '',
                '%',
                {
                    'columns': ('F_mod_weighed_count',),
                    'func': truediv,
                    'second_value': 'F_weighed_count',
                },
                {
                    'columns': ('M_mod_weighed_count',),
                    'func': truediv,
                    'second_value': 'M_weighed_count',
                },
                {
                    'columns': ('F_mod_weighed_count_1',),
                    'func': truediv,
                    'second_value': 'F_weighed_count_1',
                },
                {
                    'columns': ('M_mod_weighed_count_1',),
                    'func': truediv,
                    'second_value': 'M_weighed_count_1',
                },
                {
                    'columns': ('F_mod_weighed_count_2',),
                    'func': truediv,
                    'second_value': 'F_weighed_count_2',
                },
                {
                    'columns': ('M_mod_weighed_count_2',),
                    'func': truediv,
                    'second_value': 'M_weighed_count_2',
                },
                {
                    'columns': ('F_sum_mod_resident_weighted',),
                    'func': truediv,
                    'second_value': 'resident_female_weight'
                },
                {
                    'columns': ('M_sum_mod_resident_weighted',),
                    'func': truediv,
                    'second_value': 'resident_male_weight'
                },
                {
                    'columns': ('all_mod_resident_weighted',),
                    'func': truediv,
                    'second_value': 'all_resident_weight'
                },
            ),
            (
                'Severely Underweight (Orange)',
                'Num',
                'F_sev_weighed_count',
                'M_sev_weighed_count',
                'F_sev_weighed_count_1',
                'M_sev_weighed_count_1',
                'F_sev_weighed_count_2',
                'M_sev_weighed_count_2',
                {
                    'columns': (
                        'F_sev_weighed_count',
                        'F_sev_weighed_count_1',
                        'F_sev_weighed_count_2'),
                    'alias': 'F_sum_sev_resident_weighted'
                },
                {
                    'columns': (
                        'M_sev_weighed_count',
                        'M_sev_weighed_count_1',
                        'M_sev_weighed_count_2'),
                    'alias': 'M_sev_mod_resident_weighted'
                },
                {
                    'columns': ('F_sum_sev_resident_weighted', 'M_sev_mod_resident_weighted'),
                    'alias': 'all_sev_resident_weighted'
                }
            ),
            (
                '',
                '%',
                {
                    'columns': ('F_sev_weighed_count',),
                    'func': truediv,
                    'second_value': 'F_weighed_count',
                },
                {
                    'columns': ('M_sev_weighed_count',),
                    'func': truediv,
                    'second_value': 'M_weighed_count',
                },
                {
                    'columns': ('F_sev_weighed_count_1',),
                    'func': truediv,
                    'second_value': 'F_weighed_count_1',
                },
                {
                    'columns': ('M_sev_weighed_count_1',),
                    'func': truediv,
                    'second_value': 'M_weighed_count_1',
                },
                {
                    'columns': ('F_sev_weighed_count_2',),
                    'func': truediv,
                    'second_value': 'F_weighed_count_2',
                },
                {
                    'columns': ('M_sev_weighed_count_2',),
                    'func': truediv,
                    'second_value': 'M_weighed_count_2',
                },
                {
                    'columns': ('F_sum_sev_resident_weighted',),
                    'func': truediv,
                    'second_value': 'resident_female_weight'
                },
                {
                    'columns': ('M_sum_sev_resident_weighted',),
                    'func': truediv,
                    'second_value': 'resident_male_weight'
                },
                {
                    'columns': ('all_sev_resident_weighted',),
                    'func': truediv,
                    'second_value': 'all_resident_weight'
                },
            )
        )


class MPRImmunizationCoverage(ICDSMixin, MPRData):

    title = '9. Immunization Coverage'
    slug = 'immunization_coverage'

    @property
    def rows(self):
        if self.config['location_id']:
            data = self.custom_data(selected_location=self.selected_location, domain=self.config['domain'])
            children_completing = data.get('open_child_count', 0)
            vaccination = data.get('open_child_1yr_immun_complete', 0)
            immunization = "%.1f%%" % ((vaccination / float(children_completing or 1)) * 100)
            return [
                ['(I)', 'Children Completing 12 months during the month:', children_completing],
                ['(II)', 'Of this, number of children who have received all vaccinations:', vaccination],
                ['(III)', 'Completed timely immunization coverage (%):', immunization]
            ]

class MPRImmunizationCoverageBeta(MPRImmunizationCoverage):

    title = '9. Immunization Coverage'
    slug = 'immunization_coverage'

    def custom_data(self, selected_location, domain):
        filters = get_location_filter(selected_location.location_id, domain)
        if filters.get('aggregation_level') > 1:
            filters['aggregation_level'] -= 1

        filters['month'] = date(self.config['year'], self.config['month'], 1)
        immunization_data = AggChildHealth.objects.filter(**filters).values('month').annotate(
            open_child_count=F('fully_immunized_eligible_in_month'),
            open_child_1yr_immun_complete=F('fully_immun_before_month_end')
        ).order_by('month').first()

        return immunization_data if immunization_data else {}


class MPRVhnd(ICDSMixin, MPRData):

    title = '10. Village Health Sanitation and Nutrition Day (VHSND) activity summary'
    slug = 'vhnd'

    @property
    def headers(self):
        return DataTablesHeader(
            DataTablesColumn('Activity'),
            DataTablesColumn('No. of AWCs reported'),
            DataTablesColumn('% of AWCs reported')
        )

    @property
    def rows(self):
        if self.config['location_id']:
            data = self.custom_data(selected_location=self.selected_location, domain=self.config['domain'])
            rows = []
            for row in self.row_config:
                row_data = []
                for idx, cell in enumerate(row):
                    if isinstance(cell, dict):
                        num = data.get(cell['column'], 0)
                        row_data.append("%.1f%%" % cell['func'](num, self.awc_number))
                    else:
                        row_data.append(data.get(cell, cell if not cell or idx == 0 else 0))
                rows.append(row_data)
            return rows

    @property
    def row_config(self):
        return (
            (
                'a) Was VHSND conducted on planned date?',
                'done_when_planned',
                {
                    'column': 'done_when_planned',
                    'func': _get_percent,
                    'second_value': 'location_number'
                }
            ),
            (
                'b) AWW present during VHSND?',
                'aww_present',
                {
                    'column': 'aww_present',
                    'func': _get_percent,
                    'second_value': 'location_number'
                }
            ),
            (
                'c) ICDS Supervisor present during VHSND?',
                'icds_sup',
                {
                    'column': 'icds_sup',
                    'func': _get_percent,
                    'second_value': 'location_number'
                }
            ),
            (
                'd) ASHA present during VHSND?',
                'asha_present',
                {
                    'column': 'asha_present',
                    'func': _get_percent,
                    'second_value': 'location_number'
                }
            ),
            (
                'e) ANM / MPW present during VHSND?',
                'anm_mpw',
                {
                    'column': 'anm_mpw',
                    'func': _get_percent,
                    'second_value': 'location_number'
                }
            ),
            (
                'f) Group health education conducted?',
                'health_edu_org',
                {
                    'column': 'health_edu_org',
                    'func': _get_percent,
                    'second_value': 'location_number'
                }
            ),
            (
                'g) Demonstration conducted?',
                'display_tools',
                {
                    'column': 'display_tools',
                    'func': _get_percent,
                    'second_value': 'location_number'
                }
            ),
            (
                'h) Take home rations distributed?',
                'thr_distr',
                {
                    'column': 'thr_distr',
                    'func': _get_percent,
                    'second_value': 'location_number'
                }
            ),
            (
                'i) Any children immunized?',
                'child_immu',
                {
                    'column': 'child_immu',
                    'func': _get_percent,
                    'second_value': 'location_number'
                }
            ),
            (
                'j) Vitamin A supplements administered?',
                'vit_a_given',
                {
                    'column': 'vit_a_given',
                    'func': _get_percent,
                    'second_value': 'location_number'
                }
            ),
            (
                'k) Any antenatal check-ups conducted?',
                'anc_today',
                {
                    'column': 'anc_today',
                    'func': _get_percent,
                    'second_value': 'location_number'
                }
            ),
            (
                'l) Did village leaders/VHSNC members participate?',
                'local_leader',
                {
                    'column': 'local_leader',
                    'func': _get_percent,
                    'second_value': 'location_number'
                }
            ),
            (
                'm) Was a due list prepared before the VHSND for:',
                '',
                ''
            ),
            (
                'Immunization',
                'due_list_prep_immunization',
                {
                    'column': 'due_list_prep_immunization',
                    'func': _get_percent,
                    'second_value': 'location_number'
                }
            ),
            (
                'Vitamin A',
                'due_list_prep_vita_a',
                {
                    'column': 'due_list_prep_vita_a',
                    'func': _get_percent,
                    'second_value': 'location_number'
                }
            ),
            (
                'Antenatal check-ups',
                'due_list_prep_antenatal_checkup',
                {
                    'column': 'due_list_prep_antenatal_checkup',
                    'func': _get_percent,
                    'second_value': 'location_number'
                }
            )
        )


class MPRVhndBeta(MPRVhnd):
    def custom_data(self, selected_location, domain):
        filters = get_location_filter(selected_location.location_id, domain)
        if filters.get('aggregation_level') > 1:
            filters['aggregation_level'] -= 1

        filters['month'] = date(self.config['year'], self.config['month'], 1)
        data = dict()
        agg_mpr_data = AggMPRAwc.objects.filter(**filters).values('month').annotate(
            done_when_planned=F('vhnd_done_when_planned'),
            aww_present=F('vhnd_with_aww_present'),
            icds_sup=F('vhnd_with_icds_sup'),
            asha_present=F('vhnd_with_asha_present'),
            anm_mpw=F('vhnd_with_anm_mpw'),
            health_edu_org=F('vhnd_with_health_edu_org'),
            display_tools=F('vhnd_with_display_tools'),
            thr_distr=F('vhnd_with_thr_distr'),
            child_immu=F('vhnd_with_child_immu'),
            vit_a_given=F('vhnd_with_vit_a_given'),
            anc_today=F('vhnd_with_anc_today'),
            local_leader=F('vhnd_with_local_leader'),
            due_list_prep_immunization=F('vhnd_with_due_list_prep_immunization'),
            due_list_prep_vita_a=F('vhnd_with_due_list_prep_vita_a'),
            due_list_prep_antenatal_checkup=F('vhnd_with_due_list_prep_antenatal_checkup'),
        ).order_by('month').first()

        agg_awc_data = AggAwc.objects.filter(**filters).values('month').annotate(
            location_number=F('num_launched_awcs')
        ).order_by('month').first()
        if agg_mpr_data:
            data.update(agg_mpr_data)
        if agg_awc_data:
            data.update(agg_awc_data)
        return {key: value if value else 0 for key,value in data.items() }


class MPRReferralServices(ICDSMixin, MPRData):

    title = '11. Referral Services'
    slug = 'referral_services'

    @property
    def headers(self):
        return DataTablesHeader(
            DataTablesColumn('Type of Problems'),
            DataTablesColumnGroup(
                'AWC referred',
                DataTablesColumn('Number'),
                DataTablesColumn('%')
            ),
            DataTablesColumn('Average no. referred to health facility'),
            DataTablesColumn('Average no. reached health facility')
        )

    @property
    def rows(self):
        if self.config['location_id']:
            data = self.custom_data(selected_location=self.selected_location, domain=self.config['domain'])
            rows = []
            for row in self.row_config:
                row_data = []
                for idx, cell in enumerate(row):
                    if isinstance(cell, dict):
                        num = 0
                        if 'second_value' in cell:
                            for c in cell['columns']:
                                num += data.get(c, 0)
                            if cell['second_value'] == 'location_number':
                                denom = self.awc_number
                            else:
                                denom = data.get(cell['second_value'], 1)
                            alias_data = cell['func'](num, float(denom or 1))
                            if 'format' in cell:
                                cell_data = "%.1f%%" % (alias_data * 100)
                            else:
                                cell_data = "%.1f" % cell['func'](num, float(denom or 1))
                        else:
                            values = []
                            for c in cell['columns']:
                                values.append(data.get(c, 0))
                            if 'func' in cell:
                                num = cell['func'](values)
                            else:
                                num = sum(values)
                            cell_data = num
                            alias_data = num

                        if 'alias' in cell:
                            data[cell['alias']] = alias_data
                        row_data.append(cell_data)
                    elif isinstance(cell, tuple):
                        cell_data = 0
                        for c in cell:
                            cell_data += data.get(c, 0)
                        row_data.append(cell_data)
                    else:
                        row_data.append(data.get(cell, cell if cell == '--' or idx == 0 else 0))
                rows.append(row_data)

            return rows

    @property
    def row_config(self):
        return (
            (
                'I. Children',
                {
                    'columns': (
                        'referred_premature',
                        'referred_sepsis',
                        'referred_diarrhoea',
                        'referred_pneumonia',
                        'referred_fever_child',
                        'referred_severely_underweight',
                        'referred_other_child'
                    ),
                    'func': max,
                    'alias': 'max_children'
                },
                {
                    'columns': ('max_children',),
                    'func': truediv,
                    'second_value': 'location_number',
                    'format': 'percent'
                },
                {
                    'columns': (
                        'referred_premature_all',
                        'referred_sepsis_all',
                        'referred_diarrhoea_all',
                        'referred_pneumonia_all',
                        'referred_fever_child_all',
                        'referred_severely_underweight_all',
                        'referred_other_child_all'
                    ),
                    'func': truediv,
                    'second_value': 'number_location'
                },
                {
                    'columns': (
                        'premature_reached_count',
                        'sepsis_reached_count',
                        'diarrhoea_reached_count',
                        'pneumonia_reached_count',
                        'fever_child_reached_count',
                        'sev_underweight_reached_count',
                        'other_child_reached_count'
                    ),
                    'func': truediv,
                    'second_value': 'number_location'
                },
            ),
            (
                'a. Premature',
                'referred_premature',
                {
                    'columns': ('referred_premature',),
                    'func': truediv,
                    'second_value': 'location_number',
                    'format': 'percent'
                },
                {
                    'columns': ('referred_premature_all',),
                    'func': truediv,
                    'second_value': 'location_number',
                },
                {
                    'columns': ('premature_reached_count',),
                    'func': truediv,
                    'second_value': 'location_number',
                }
            ),
            (
                'b. Sepsis',
                'referred_sepsis',
                {
                    'columns': ('referred_sepsis',),
                    'func': truediv,
                    'second_value': 'location_number',
                    'format': 'percent'
                },
                {
                    'columns': ('referred_sepsis_all',),
                    'func': truediv,
                    'second_value': 'location_number',
                },
                {
                    'columns': ('sepsis_reached_count',),
                    'func': truediv,
                    'second_value': 'location_number',
                }
            ),
            (
                'c. Diarrhea',
                'referred_diarrhoea',
                {
                    'columns': ('referred_diarrhoea',),
                    'func': truediv,
                    'second_value': 'location_number',
                    'format': 'percent'
                },
                {
                    'columns': ('referred_diarrhoea_all',),
                    'func': truediv,
                    'second_value': 'location_number',
                },
                {
                    'columns': ('diarrhoea_reached_count',),
                    'func': truediv,
                    'second_value': 'location_number',
                }
            ),
            (
                'd. Pneumonia',
                'referred_pneumonia',
                {
                    'columns': ('referred_pneumonia',),
                    'func': truediv,
                    'second_value': 'location_number',
                    'format': 'percent'
                },
                {
                    'columns': ('referred_pneumonia_all',),
                    'func': truediv,
                    'second_value': 'location_number',
                },
                {
                    'columns': ('pneumonia_reached_count',),
                    'func': truediv,
                    'second_value': 'location_number',
                }
            ),
            (
                'e. Fever',
                'referred_fever_child',
                {
                    'columns': ('referred_fever_child',),
                    'func': truediv,
                    'second_value': 'location_number',
                    'format': 'percent'
                },
                {
                    'columns': ('referred_fever_child_all',),
                    'func': truediv,
                    'second_value': 'location_number',
                },
                {
                    'columns': ('fever_child_reached_count',),
                    'func': truediv,
                    'second_value': 'location_number',
                }
            ),
            (
                'f. Severely underweight',
                'referred_severely_underweight',
                {
                    'columns': ('referred_severely_underweight',),
                    'func': truediv,
                    'second_value': 'location_number',
                    'format': 'percent'
                },
                {
                    'columns': ('referred_severely_underweight_all',),
                    'func': truediv,
                    'second_value': 'location_number',
                },
                {
                    'columns': ('sev_underweight_reached_count',),
                    'func': truediv,
                    'second_value': 'location_number',
                }
            ),
            (
                'g. Other',
                'referred_other_child',
                {
                    'columns': ('referred_other_child',),
                    'func': truediv,
                    'second_value': 'location_number',
                    'format': 'percent'
                },
                {
                    'columns': ('referred_other_child_all',),
                    'func': truediv,
                    'second_value': 'location_number',
                },
                {
                    'columns': ('other_child_reached_count',),
                    'func': truediv,
                    'second_value': 'location_number',
                }
            ),
            (
                'II. Pregnant and Lactating Women',
                {
                    'columns': (
                        'referred_bleeding',
                        'referred_convulsions',
                        'referred_prolonged_labor',
                        'referred_abortion_complications',
                        'referred_fever_discharge',
                        'referred_other',
                    ),
                    'func': max,
                    'alias': 'max_pregnant'
                },
                {
                    'columns': ('max_pregnant',),
                    'func': truediv,
                    'second_value': 'location_number',
                    'format': 'percent'
                },
                {
                    'columns': (
                        'referred_bleeding_all',
                        'referred_convulsions_all',
                        'referred_prolonged_labor_all',
                        'referred_abortion_complications_all',
                        'referred_fever_discharge_all',
                        'referred_other_all'
                    ),
                    'func': truediv,
                    'second_value': 'number_location'
                },
                {
                    'columns': (
                        'bleeding_reached_count',
                        'convulsions_reached_count',
                        'prolonged_labor_reached_count',
                        'abort_comp_reached_count',
                        'fever_discharge_reached_count',
                        'other_reached_count'
                    ),
                    'func': truediv,
                    'second_value': 'number_location'
                },
            ),
            (
                'a. Bleeding',
                'referred_bleeding',
                {
                    'columns': ('referred_bleeding',),
                    'func': truediv,
                    'second_value': 'location_number',
                    'format': 'percent'
                },
                {
                    'columns': ('referred_bleeding_all',),
                    'func': truediv,
                    'second_value': 'location_number',
                },
                {
                    'columns': ('bleeding_reached_count',),
                    'func': truediv,
                    'second_value': 'location_number',
                }
            ),
            (
                'b. Convulsion',
                'referred_convulsions',
                {
                    'columns': ('referred_convulsions',),
                    'func': truediv,
                    'second_value': 'location_number',
                    'format': 'percent'
                },
                {
                    'columns': ('referred_convulsions_all',),
                    'func': truediv,
                    'second_value': 'location_number',
                },
                {
                    'columns': ('convulsions_reached_count',),
                    'func': truediv,
                    'second_value': 'location_number',
                }
            ),
            (
                'c. Prolonged labour',
                'referred_prolonged_labor',
                {
                    'columns': ('referred_prolonged_labor',),
                    'func': truediv,
                    'second_value': 'location_number',
                    'format': 'percent'
                },
                {
                    'columns': ('referred_prolonged_labor_all',),
                    'func': truediv,
                    'second_value': 'location_number',
                },
                {
                    'columns': ('prolonged_labor_reached_count',),
                    'func': truediv,
                    'second_value': 'location_number',
                }
            ),
            (
                'd. Abortion complications',
                'referred_abortion_complications',
                {
                    'columns': ('referred_abortion_complications',),
                    'func': truediv,
                    'second_value': 'location_number',
                    'format': 'percent'
                },
                {
                    'columns': ('referred_abortion_complications_all',),
                    'func': truediv,
                    'second_value': 'location_number',
                },
                {
                    'columns': ('abort_comp_reached_count',),
                    'func': truediv,
                    'second_value': 'location_number',
                }
            ),
            (
                'e. Fever/offensive discharge after delivery',
                'referred_fever_discharge',
                {
                    'columns': ('referred_fever_discharge',),
                    'func': truediv,
                    'second_value': 'location_number',
                    'format': 'percent'
                },
                {
                    'columns': ('referred_fever_discharge_all',),
                    'func': truediv,
                    'second_value': 'location_number',
                },
                {
                    'columns': ('fever_discharge_reached_count',),
                    'func': truediv,
                    'second_value': 'location_number',
                }
            ),
            (
                'f. Other',
                'referred_other',
                {
                    'columns': ('referred_other',),
                    'func': truediv,
                    'second_value': 'location_number',
                    'format': 'percent'
                },
                {
                    'columns': ('referred_other_all',),
                    'func': truediv,
                    'second_value': 'location_number',
                },
                {
                    'columns': ('other_reached_count',),
                    'func': truediv,
                    'second_value': 'location_number',
                }
            )
        )

class MPRReferralServicesBeta(MPRReferralServices):

    def custom_data(self, selected_location, domain):
        filters = get_location_filter(selected_location.location_id, domain)
        if filters.get('aggregation_level') > 1:
            filters['aggregation_level'] -= 1

        filters['month'] = date(self.config['year'], self.config['month'], 1)
        data = dict()
        agg_mpr_data = AggMPRAwc.objects.filter(**filters).values('month').annotate(
            referred_premature=F('num_premature_referral_awcs'),
            referred_premature_all=F('total_premature_referrals'),
            premature_reached_count=F('total_premature_reached_facility'),

            referred_sepsis=F('num_sepsis_referral_awcs'),
            referred_sepsis_all=F('total_sepsis_referrals'),
            sepsis_reached_count=F('total_sepsis_reached_facility'),

            referred_diarrhoea=F('num_diarrhoea_referral_awcs'),
            referred_diarrhoea_all=F('total_diarrhoea_referrals'),
            diarrhoea_reached_count=F('total_diarrhoea_reached_facility'),

            referred_pneumonia=F('num_pneumonia_referral_awcs'),
            referred_pneumonia_all=F('total_pneumonia_referrals'),
            pneumonia_reached_count=F('total_pneumonia_reached_facility'),

            referred_fever_child=F('num_fever_referral_awcs'),
            referred_fever_child_all=F('total_fever_referrals'),
            fever_child_reached_count=F('total_fever_reached_facility'),

            referred_severely_underweight=F('num_severely_underweight_referral_awcs'),
            referred_severely_underweight_all=F('total_severely_underweight_referrals'),
            sev_underweight_reached_count=F('total_severely_underweight_reached_facility'),

            referred_other_child=F('num_other_child_referral_awcs'),
            referred_other_child_all=F('total_other_child_referrals'),
            other_child_reached_count=F('total_other_child_reached_facility'),

            referred_bleeding=F('num_bleeding_referral_awcs'),
            referred_bleeding_all=F('total_bleeding_referrals'),
            bleeding_reached_count=F('total_bleeding_reached_facility'),

            referred_convulsions=F('num_convulsions_referral_awcs'),
            referred_convulsions_all=F('total_convulsions_referrals'),
            convulsions_reached_count=F('total_convulsions_reached_facility'),

            referred_prolonged_labor=F('num_prolonged_labor_referral_awcs'),
            referred_prolonged_labor_all=F('total_prolonged_labor_referrals'),
            prolonged_labor_reached_count=F('total_prolonged_labor_reached_facility'),

            referred_abortion_complications=F('num_abortion_complications_referral_awcs'),
            referred_abortion_complications_all=F('total_abortion_complications_referrals'),
            abort_comp_reached_count=F('total_abortion_complications_reached_facility'),

            referred_fever_discharge=F('num_fever_discharge_referral_awcs'),
            referred_fever_discharge_all=F('total_fever_discharge_referrals'),
            fever_discharge_reached_count=F('total_fever_discharge_reached_facility'),

            referred_other=F('num_other_referral_awcs'),
            referred_other_all=F('total_other_referrals'),
            other_reached_count=F('total_other_reached_facility'),

        ).first()

        agg_awc_data = AggAwc.objects.filter(**filters).values('month').annotate(
            location_number=F('num_launched_awcs')
        ).order_by('month').first()
        if agg_mpr_data:
            data.update(agg_mpr_data)
        if agg_awc_data:
            data.update(agg_awc_data)

        print(data)
        return {key: value if value else 0 for key, value in data.items()}



class MPRMonitoring(ICDSMixin, MPRData):

    title = '12. Monitoring and Supervision During the Month'
    subtitle = '(I) Visits to AWCs',
    slug = 'monitoring'

    @property
    def headers(self):
        return DataTablesHeader(
            DataTablesColumn('Visited By'),
            DataTablesColumn('No. of AWCs Visited'),
            DataTablesColumn('% of AWCs Visited')
        )

    @property
    def rows(self):
        if self.config['location_id']:
            data = self.custom_data(selected_location=self.selected_location, domain=self.config['domain'])
            rows = []
            for row in self.row_config:
                row_data = []
                for idx, cell in enumerate(row):
                    if isinstance(cell, dict):
                        num = 0
                        for c in cell['columns']:
                            num += data.get(c, 0)
                        row_data.append("%.1f%%" % cell['func'](num, self.awc_number))
                    else:
                        row_data.append(data.get(cell, cell if not cell or idx == 0 else 0))
                rows.append(row_data)
            return rows

    @property
    def row_config(self):
        return (
            (
                '(From AWW MPR)',
                '',
                ''
            ),
            (
                'a. ICDS Supervisor',
                'visitor_icds_sup',
                {
                    'columns': ('visitor_icds_sup',),
                    'func': _get_percent,
                    'second_value': 'location_number',
                }
            ),
            (
                'b. ANM',
                'visitor_anm',
                {
                    'columns': ('visitor_anm',),
                    'func': _get_percent,
                    'second_value': 'location_number',
                }
            ),
            (
                'c. Health Supervisor',
                'visitor_health_sup',
                {
                    'columns': ('visitor_health_sup',),
                    'func': _get_percent,
                    'second_value': 'location_number',
                }
            ),
            (
                'd. CDPO/ACDPO',
                'visitor_cdpo',
                {
                    'columns': ('visitor_cdpo',),
                    'func': _get_percent,
                    'second_value': 'location_number',
                }
            ),
            (
                'e. Medical Officer',
                'visitor_med_officer',
                {
                    'columns': ('visitor_med_officer',),
                    'func': _get_percent,
                    'second_value': 'location_number',
                }
            ),
            (
                'f. ICDS Dist Programme Officer (DPO)',
                'visitor_dpo',
                {
                    'columns': ('visitor_dpo',),
                    'func': _get_percent,
                    'second_value': 'location_number',
                }
            ),
            (
                'g. State level officials',
                'visitor_officer_state',
                {
                    'columns': ('visitor_officer_state',),
                    'func': _get_percent,
                    'second_value': 'location_number',
                }
            ),
            (
                'h. Officials from Central Government',
                'visitor_officer_central',
                {
                    'columns': ('visitor_officer_central',),
                    'func': _get_percent,
                    'second_value': 'location_number',
                }
            )
        )

class MPRMonitoringBeta(MPRMonitoring):

    def custom_data(self, selected_location, domain):
        filters = get_location_filter(selected_location.location_id, domain)
        if filters.get('aggregation_level') > 1:
            filters['aggregation_level'] -= 1

        filters['month'] = date(self.config['year'], self.config['month'], 1)
        data = dict()
        agg_mpr_data = AggMPRAwc.objects.filter(**filters).values(
            'visitor_icds_sup',
            'visitor_anm',
            'visitor_health_sup',
            'visitor_cdpo',
            'visitor_med_officer',
            'visitor_dpo',
            'visitor_officer_state',
            'visitor_officer_central'
        ).order_by('month').first()
        agg_awc_data = AggAwc.objects.filter(**filters).values('month').annotate(
            location_number=F('num_launched_awcs')
        ).order_by('month').first()
        if agg_mpr_data:
            data.update(agg_mpr_data)
        if agg_awc_data:
            data.update(agg_awc_data)
        return {key: value if value else 0 for key,value in data.items() }



def _get_percent(a, b):
    return (a / (b or 1)) * 100
