import gzip
import json
import multiprocessing
import os
import zipfile
from collections import namedtuple, Counter
from concurrent import futures
from datetime import datetime
from threading import Lock

from django.core.management import CommandError
from django.core.management.base import BaseCommand

from corehq.apps.dump_reload.const import DATETIME_FORMAT
from corehq.apps.dump_reload.util import get_model_label
from corehq.blobs.models import BlobMeta
from corehq.sql_db.util import get_db_aliases_for_partitioned_query
from custom.icds.data_management.state_dump.couch import dump_couch_data, AVAILABLE_COUCH_TYPES, dump_toggle_data, \
    dump_domain_object, BLOB_META_STATS_KEY
from custom.icds.data_management.state_dump.sql import AVAILABLE_SQL_TYPES, dump_simple_sql_data, dump_form_case_data
from custom.icds.management.commands.prepare_filter_values_for_location_dump import FilterContext

Dumper = namedtuple("Dumper", "function, partition_dbs")

DUMPERS = {
    'domain': Dumper(dump_domain_object, False),
    'toggles': Dumper(dump_toggle_data, False),
    'couch': Dumper(dump_couch_data, False),
    'sql': Dumper(dump_simple_sql_data, False),
    'sql-sharded': Dumper(dump_form_case_data, True)
}


class Command(BaseCommand):
    help = """Dump a ICDS data for a single location.

    Use in conjunction with `prepare_filter_values_for_location_dump`.
    """

    def add_arguments(self, parser):
        parser.add_argument('domain_name')
        parser.add_argument('location', help="ID or name of the location.")
        parser.add_argument(
            '-d', '--dumper', action='append', choices=('domain', 'toggles', 'couch', 'sql', 'sql-sharded'),
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
        parser.add_argument('--thread-count', type=int,
                            help="Max number of threads to use. If not set each dumper will use it's"
                                 " own thread and the sharded dumper will use one thread for each"
                                 " sharded database. Set to 0 to disable multi-threading.")

    def handle(self, domain_name, location, **options):
        output = options.get("output_path", None)
        if output and os.path.exists(output):
            raise CommandError(f"Path exists: {output}")

        selected_backends = options.get("dumper", None) or list(DUMPERS)

        context = FilterContext(domain_name, location, options.get("type", []))
        if not context.validate():
            raise CommandError(
                "Some location ID files are missing. Have you run 'prepare_filter_values_for_location_dump'?"
            )

        zipname = output or 'data-dump-{}-{}-{}.zip'.format(
            domain_name, "_".join(selected_backends), datetime.utcnow().strftime(DATETIME_FORMAT)
        )

        args_list = []
        for slug in selected_backends:
            dumper = DUMPERS[slug]
            args = {"args": (domain_name, slug, dumper, context, zipname), "kwargs": {}}
            if dumper.partition_dbs:
                args_list.extend(
                    {**args, **{"kwargs": {"limit_to_db": db}}}
                    for db in get_db_aliases_for_partitioned_query()
                )
            else:
                args_list.append(args)

        self.pool_size = options.get("thread_count", len(args_list))
        lock = Lock()

        if self.pool_size == 0:
            results = self._synchronous_dump(args_list, lock)
        else:
            results = self._threaded_dump(args_list, lock)

        meta = {}  # {dumper_slug: {model_name: count}}
        for result in results:
            meta.update(result)

        print(f"All dumps complete. Writing metadata to ZIP.")
        with zipfile.ZipFile(zipname, mode='a', allowZip64=True) as z:
            z.writestr('meta.json', json.dumps(meta))

        self._print_stats(meta)
        self.stdout.write('\nData dumped to file: {}'.format(zipname))

    def _synchronous_dump(self, args_list, lock):
        for args in args_list:
            yield dump_data_for_backend(ziplock=lock, *args["args"], **args["kwargs"])

    def _threaded_dump(self, args_list, lock):
        results = []
        with futures.ThreadPoolExecutor(max_workers=self.pool_size) as executor:
            for args in args_list:
                results.append(
                    executor.submit(dump_data_for_backend, ziplock=lock, *args["args"], **args["kwargs"])
                )

            for result in futures.as_completed(results):
                yield result.result()

            executor.shutdown()

    def _print_stats(self, meta):
        self.stdout.ending = '\n'
        self.stdout.write('{0} Dump Stats {0}'.format('-' * 32))
        totals = Counter()
        for dumper, models in sorted(meta.items()):
            for model, count in sorted(models.items()):
                totals[model] += count

        for model, count in sorted(totals.items()):
            self.stdout.write("  {:<50}: {}".format(model, count))
        self.stdout.write('{0}{0}'.format('-' * 38))
        self.stdout.write('Dumped {} objects'.format(sum(totals.values())))
        self.stdout.write('{0}{0}'.format('-' * 38))


def dump_data_for_backend(domain_name, slug, dumper, context, zipname, ziplock=None, limit_to_db=None):
    filename = _get_filename("dump", slug, domain_name, limit_to_db)
    blob_meta_filename = _get_filename("blob_meta", slug, domain_name, limit_to_db)

    kwargs = {"limit_to_db": limit_to_db} if limit_to_db else {}
    with gzip.open(filename, 'wt') as data_stream, gzip.open(blob_meta_filename, 'wt') as blob_stream:
        stats = dumper.function(domain_name, context, data_stream, blob_stream, **kwargs)

    db = f"-{limit_to_db}" if limit_to_db else ""
    # filename must match up with meta key
    key = f"{slug}{db}"
    blob_key = f"sql-{slug}-blob_meta{db}"
    meta = {
        key: stats
    }

    blob_stats = stats.pop(BLOB_META_STATS_KEY, None)
    if blob_stats:
        meta[blob_key] = {get_model_label(BlobMeta): blob_stats}

    print(f"Dump {filename} complete. Copying data to ZIP.")
    with ziplock:
        with zipfile.ZipFile(zipname, mode='a', allowZip64=True) as z:
            z.write(filename, f'{key}.gz')
            if blob_stats:
                z.write(blob_meta_filename, f'{blob_key}.gz')

    os.remove(filename)
    os.remove(blob_meta_filename)
    return meta


def _get_filename(name, slug, domain, limit_to_db, ext="gz"):
    db = f"-{limit_to_db}" if limit_to_db else ""
    return '{}-{}-{}{}-{}.{}'.format(name, slug, domain, db, datetime.utcnow().strftime(DATETIME_FORMAT), ext)
