from datetime import date
import argparse

from django.core.management.base import BaseCommand

from custom.icds_reports.reports.disha import build_dumps_for_month


class Command(BaseCommand):
    help = "Build DISHA data dumps"

    def add_arguments(self, parser):
        def valid_date(s):
            try:
                return date.strptime(s, "%Y-%m-%d").date().replace(day=1)
            except ValueError:
                msg = "Not a valid date: '{0}'.".format(s)
                raise argparse.ArgumentTypeError(msg)

        parser.add_argument(
            'month',
            type=valid_date,
            help="Month of DISHA dump - format = YYYY-MM-DD",
        )

    def handle(self, month):
        build_dumps_for_month(month)
