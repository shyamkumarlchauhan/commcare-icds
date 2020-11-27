import gzip
import json
from datetime import datetime

from django.core.management.base import BaseCommand

from corehq.apps.dump_reload.const import DATETIME_FORMAT
from custom.icds.data_management.state_dump.couch import dump_couch_data, AVAILABLE_COUCH_TYPES
from custom.icds.data_management.state_dump.sql import AVAILABLE_SQL_TYPES
from custom.icds.management.commands.prepare_filter_values_for_state_dump import FilterContext


class Command(BaseCommand):
    help = """Dump a ICDS data for a single state.

    Use in conjunction with `prepare_filter_values_for_state_dump`.
    """

    def add_arguments(self, parser):
        parser.add_argument('domain_name')
        parser.add_argument('state', help="The name of the state")
        parser.add_argument(
            '-b', '--backend', choices=('couch', 'sql'),
            help="Limit the data output to this backend"
        )
        parser.add_argument(
            '-t', '--type', dest='doc_types', action='append', default=[],
            help='An app_label, app_label.ModelName or CouchDB doc_type to limit the '
                 'dump output to (use multiple --type to add multiple apps/models).'
                 f'\bAvailable couch types are: {", ".join(AVAILABLE_COUCH_TYPES)}'
                 f'\nAvailable SQL types are: {AVAILABLE_SQL_TYPES}'
        )

    def handle(self, domain_name, state, **options):
        backend = options.get("backend", None)

        self.utcnow = datetime.utcnow().strftime(DATETIME_FORMAT)
        context = FilterContext(domain_name, state, options.get("type", []))
        if not context.validate():
            print("Some state ID files are missing. Have you run 'prepare_filter_values_for_state_dump'?")

        self.stdout.ending = None
        meta = {}  # {dumper_slug: {model_name: count}}
        filename = _get_filename("dump", "couch", domain_name, self.utcnow)
        blob_meta_filename = _get_filename("blob_meta", "couch", domain_name, self.utcnow)

        with gzip.open(filename, 'wt') as data_stream, gzip.open(blob_meta_filename, 'wt') as blob_stream:
            meta["couch"] = dump_couch_data(domain_name, context, data_stream, blob_stream)

        counts_filename = _get_filename("counts", "couch", domain_name, self.utcnow, "json")
        with open(counts_filename, 'wt') as z:
            json.dump(meta, z)

        self._print_stats(meta)
        self.stdout.write('\nData dumped to file: {}'.format(filename))
        self.stdout.write('\nData blob meta to file: {}'.format(blob_meta_filename))
        self.stdout.write('\nData doc counts to file: {}'.format(counts_filename))

    def _print_stats(self, meta):
        self.stdout.ending = '\n'
        self.stdout.write('{0} Dump Stats {0}'.format('-' * 32))
        for dumper, models in sorted(meta.items()):
            self.stdout.write(dumper)
            for model, count in sorted(models.items()):
                self.stdout.write("  {:<50}: {}".format(model, count))
        self.stdout.write('{0}{0}'.format('-' * 38))
        self.stdout.write('Dumped {} objects'.format(sum(
            count for model in meta.values() for count in model.values()
        )))
        self.stdout.write('{0}{0}'.format('-' * 38))



def _get_filename(name, slug, domain, utcnow, ext="gz"):
    return '{}-{}-{}-{}.{}'.format(name, slug, domain, utcnow, ext)
