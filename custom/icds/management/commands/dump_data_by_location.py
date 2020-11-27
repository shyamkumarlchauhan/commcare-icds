import gzip
import json
import os
import zipfile
from datetime import datetime

from django.core.management import CommandError
from django.core.management.base import BaseCommand

from corehq.apps.dump_reload.const import DATETIME_FORMAT
from corehq.apps.dump_reload.util import get_model_label
from corehq.blobs.models import BlobMeta
from custom.icds.data_management.state_dump.couch import dump_couch_data, AVAILABLE_COUCH_TYPES, dump_toggle_data, \
    dump_domain_object, BLOB_META_STATS_KEY
from custom.icds.data_management.state_dump.sql import AVAILABLE_SQL_TYPES, dump_simple_sql_data, dump_form_case_data
from custom.icds.management.commands.prepare_filter_values_for_state_dump import FilterContext


class Command(BaseCommand):
    help = """Dump a ICDS data for a single state.

    Use in conjunction with `prepare_filter_values_for_state_dump`.
    """

    def add_arguments(self, parser):
        parser.add_argument('domain_name')
        parser.add_argument('state', help="The name of the state")
        parser.add_argument(
            '-d', '--dumper', choices=('domain', 'toggles', 'couch', 'sql', 'sql-sharded'),
            help="Limit the data output to this dumper"
        )
        parser.add_argument(
            '-t', '--type', dest='doc_types', action='append', default=[],
            help='An app_label, app_label.ModelName or CouchDB doc_type to limit the '
                 'dump output to (use multiple --type to add multiple apps/models).'
                 f'\bAvailable couch types are: {", ".join(AVAILABLE_COUCH_TYPES)}'
                 f'\nAvailable SQL types are: {AVAILABLE_SQL_TYPES}'
        )
        parser.add_argument(
            '--output-path', help="Write output data to this path"
        )

    def handle(self, domain_name, state, **options):
        output = options.get("output_path", None)
        if output and os.path.exists(output):
            raise CommandError(f"Path exists: {output}")

        backends = {
            'domain': dump_domain_object,
            'toggles': dump_toggle_data,
            'couch': dump_couch_data,
            'sql': dump_simple_sql_data,
            'sql-sharded': dump_form_case_data
        }
        backend = options.get("dumper", None)
        selected_backends = [backend] if backend else list(backends)

        self.utcnow = datetime.utcnow().strftime(DATETIME_FORMAT)
        context = FilterContext(domain_name, state, options.get("type", []))
        if not context.validate():
            print("Some state ID files are missing. Have you run 'prepare_filter_values_for_state_dump'?")

        zipname = output or 'data-dump-{}-{}-{}.zip'.format(domain_name, "_".join(selected_backends), self.utcnow)
        meta = {}  # {dumper_slug: {model_name: count}}
        for slug, backend_fn in backends.items():
            if slug in selected_backends:
                meta.update(
                    self.dump_data_for_backend(domain_name, slug, backend_fn, context, zipname)
                )

        with zipfile.ZipFile(zipname, mode='a', allowZip64=True) as z:
            z.writestr('meta.json', json.dumps(meta))

        self._print_stats(meta)
        self.stdout.write('\nData dumped to file: {}'.format(zipname))

    def dump_data_for_backend(self, domain_name, slug, dumper, context, zipname):
        self.stdout.ending = None
        filename = _get_filename("dump", slug, domain_name, self.utcnow)
        blob_meta_filename = _get_filename("blob_meta", slug, domain_name, self.utcnow)

        with gzip.open(filename, 'wt') as data_stream, gzip.open(blob_meta_filename, 'wt') as blob_stream:
            stats = dumper(domain_name, context, data_stream, blob_stream)

        meta = {
            slug: stats
        }
        # filename must match up with stats key
        blob_name = f"sql-{slug}-blob_meta"
        blob_stats = stats.pop(BLOB_META_STATS_KEY, None)
        if blob_stats:
            meta[blob_name] = {get_model_label(BlobMeta): blob_stats}

        with zipfile.ZipFile(zipname, mode='a', allowZip64=True) as z:
            z.write(filename, '{}.gz'.format(slug))
            if blob_stats:
                z.write(blob_meta_filename, f'{blob_name}.gz')

        os.remove(filename)
        os.remove(blob_meta_filename)

        return meta

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
