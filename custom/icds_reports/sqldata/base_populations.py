from datetime import date
from custom.icds_reports.utils import ICDSMixin
from custom.icds_reports.models.aggregate import AggAwc
from custom.icds_reports.utils import get_location_filter


class BasePopulation(ICDSMixin):

    slug = 'population'

    @property
    def headers(self):
        return []

    @property
    def rows(self):
        if self.config['location_id']:
            filters = get_location_filter(self.config['location_id'], self.config['domain'])

            if filters.get('aggregation_level')>1:
                filters['aggregation_level'] -= 1

            filters['month'] = date(self.config['year'], self.config['month'], 1)
            awc_data = AggAwc.objects.filter(**filters).values('cases_person').order_by('month').first()
            return [
                [
                    "Total Population of the project:",
                    awc_data.get('cases_person') if awc_data else 0
                ]
            ]
