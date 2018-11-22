from __future__ import absolute_import
from __future__ import unicode_literals

from dateutil.relativedelta import relativedelta

from corehq.apps.userreports.models import StaticDataSourceConfiguration, get_datasource_config
from corehq.apps.userreports.util import get_table_name

from custom.icds_reports.utils.aggregation_helpers import BaseICDSAggregationHelper, transform_day_to_month


class AggLsHelper(BaseICDSAggregationHelper):

    base_tablename = 'agg_ls'
    awc_location_ucr = 'static-awc_location'
    ls_vhnd_ucr = 'static-ls_vhnd_form'
    ls_home_visit_ucr = 'static-ls_home_visit_forms_filled'
    ls_awc_mgt_ucr = 'static-awc_mgt_forms'

    def __init__(self, month):
        self.month_start = transform_day_to_month(month)
        self.next_month_start = self.month_start + relativedelta(months=1)

    def _tablename_func(self, agg_level):
        return "{}_{}_{}".format(self.base_tablename, self.month_start.strftime("%Y-%m-%d"), agg_level)

    def drop_table_if_exists(self, agg_level):
        return """
        DROP TABLE IF EXISTS "{table_name}"
        """.format(table_name=self._tablename_func(agg_level))

    def create_child_table(self, agg_level):
        return """
        CREATE TABLE "{table_name}" (
        CHECK (month=DATE %(start_date)s AND aggregation_level={agg_level})
        ) INHERITS ({base_tablename})
        """.format(
            table_name=self._tablename_func(agg_level),
            base_tablename=self.base_tablename,
            agg_level=agg_level
        ), {
            "start_date": self.month_start
        }

    @property
    def tablename(self):
        return self._tablename_func(4)

    def _ucr_tablename(self, ucr_id):
        doc_id = StaticDataSourceConfiguration.get_doc_id(self.domain, ucr_id)
        config, _ = get_datasource_config(doc_id, self.domain)
        return get_table_name(self.domain, config.table_id)

    def aggregate_query(self):
        """
        Returns the base aggregate query which is used to insert all the locations
        into the LS data table.
        """
        return """
        INSERT INTO "{tablename}"
        (state_id, district_id, block_id, supervisor_id, month,
         unique_awc_vists, vhnd_observed, beneficiary_vists, aggregation_level
        )
        (
        SELECT DISTINCT
        state_id,
        district_id,
        block_id,
        supervisor_id,
        %(start_date)s,
        0,
        0,
        0,
        4
        FROM "{awc_location_ucr}"
        )
        """.format(
            tablename=self.tablename,
            awc_location_ucr=self._ucr_tablename(ucr_id=self.awc_location_ucr)
        ), {
            'start_date': self.month_start
        }

    def updates(self):
        """
        Returns the update query.
        This query updated the ls databased on the form submissions.
        Following data is updated with the returned queries:
            1) vhnd forms submitted
            2) unique awcs visits made by LS
            3) number of beneficiary form visited
        """
        yield """
            UPDATE "{tablename}" agg_ls
            SET vhnd_observed = ut.vhnd_observed
            FROM (
                SELECT count(*) as vhnd_observed,
                location_id as supervisor_id
                FROM "{ls_vhnd_ucr}"
                WHERE vhnd_date > %(start_date)s AND vhnd_date < %(end_date)s
                GROUP BY location_id
            ) ut
            WHERE agg_ls.supervisor_id = ut.supervisor_id
        """.format(
            tablename=self.tablename,
            ls_vhnd_ucr=self._ucr_tablename(ucr_id=self.ls_vhnd_ucr)
        ), {
            "start_date": self.month_start,
            "end_date": self.next_month_start
        }

        yield """
            UPDATE "{tablename}" agg_ls
            SET unique_awc_vists = ut.unique_awc_vists
            FROM (
                SELECT count(distinct awc_id) as unique_awc_vists,
                location_id as supervisor_id
                FROM "{ls_awc_mgt_ucr}"
                WHERE submitted_on > %(start_date)s AND  submitted_on< %(end_date)s
                AND location_entered is not null and location_entered <> ''
                GROUP BY location_id
            ) ut
            WHERE agg_ls.supervisor_id = ut.supervisor_id
        """.format(
            tablename=self.tablename,
            ls_awc_mgt_ucr=self._ucr_tablename(ucr_id=self.ls_awc_mgt_ucr)
        ), {
            "start_date": self.month_start,
            "end_date": self.next_month_start
        }

        yield """
            UPDATE "{tablename}" agg_ls
            SET beneficiary_vists = ut.beneficiary_vists
            FROM (
                SELECT
                count(*) as beneficiary_vists,
                location_id as supervisor_id
                FROM "{ls_home_visit_ucr}"
                WHERE submitted_on > %(start_date)s AND  submitted_on< %(end_date)s
                AND visit_type_entered is not null AND visit_type_entered <> ''
                GROUP BY location_id
            ) ut
            WHERE agg_ls.supervisor_id = ut.supervisor_id
        """.format(
            tablename=self.tablename,
            ls_home_visit_ucr=self._ucr_tablename(ucr_id=self.ls_home_visit_ucr)
        ), {
            "start_date": self.month_start,
            "end_date": self.next_month_start
        }

    def indexes(self, aggregation_level):
        """
        Returns queries to create indices  for columns
        district_id, block_id, supervisor_id and state_id based on
        aggregation level
        """
        indexes = []
        agg_locations = ['state_id']
        if aggregation_level > 1:
            indexes.append('CREATE INDEX ON "{}" (district_id)'.format(self._tablename_func(aggregation_level)))
            agg_locations.append('district_id')
        if aggregation_level > 2:
            indexes.append('CREATE INDEX ON "{}" (block_id)'.format(self._tablename_func(aggregation_level)))
            agg_locations.append('block_id')
        if aggregation_level > 3:
            indexes.append('CREATE INDEX ON "{}" (supervisor_id)'.format(self._tablename_func(aggregation_level)))
            agg_locations.append('supervisor_id')

        indexes.append('CREATE INDEX ON "{}" ({})'.format(self._tablename_func(aggregation_level),
                                                          ', '.join(agg_locations)))
        return indexes

    def rollup_query(self, agg_level):
        """
        Returns the roll up query to the agg_level passed as argument.
        Roll up query is used to roll up the data from supervisor level
        to block level to district level to state level
        """
        locations = ['state_id', 'district_id', 'block_id', 'supervisor_id']

        for i in range(3, agg_level - 1, -1):
            locations[i] = "'All'"

        return """
            INSERT INTO "{to_table}" (
            vhnd_observed,
            beneficiary_vists,
            unique_awc_vists,
            aggregation_level,
            state_id,
            district_id,
            block_id,
            supervisor_id,
            month)
            (
                SELECT
                sum(vhnd_observed) as vhnd_observed,
                sum(beneficiary_vists) as beneficiary_vists,
                sum(unique_awc_vists) as unique_awc_vists,
                {agg_level},
                {locations},
                month
                FROM "{from_table}"
                GROUP BY {group_by}, month
            )
        """.format(
            agg_level=agg_level,
            to_table=self._tablename_func(agg_level),
            locations=','.join(locations),
            from_table=self._tablename_func(agg_level + 1),
            group_by=','.join(locations[:agg_level])
        )
