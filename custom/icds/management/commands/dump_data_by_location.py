import gzip
import json
import os
import zipfile
from datetime import datetime

from django.core.management.base import BaseCommand

from corehq.apps.dump_reload.const import DATETIME_FORMAT
from corehq.apps.locations.dbaccessors import get_user_ids_by_location
from corehq.apps.locations.models import SQLLocation, LocationType
from custom.icds.data_management.state_dump.couch import dump_couch_data


class Command(BaseCommand):
    help = "Dump a ICDS data for a single sate"

    def add_arguments(self, parser):
        parser.add_argument('domain_name')
        parser.add_argument('state')
        parser.add_argument('--clear-cache', action="store_true")

    def handle(self, domain_name, state, **options):
        clear_cache = options["clear-cache"]

        self.utcnow = datetime.utcnow().strftime(DATETIME_FORMAT)
        user_ids, location_data = _get_user_location_data(domain_name, state, clear_cache)

        self.stdout.ending = None
        meta = {}  # {dumper_slug: {model_name: count}}
        filename = _get_filename("dump", "couch", domain_name, self.utcnow)
        blob_meta_filename = _get_filename("blob_meta", "couch", domain_name, self.utcnow)

        with gzip.open(filename, 'wt') as data_stream, gzip.open(blob_meta_filename, 'wt') as blob_stream:
            meta["couch"] = dump_couch_data(domain_name, user_ids, data_stream, blob_stream)

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


def _get_user_location_data(domain, state, clear_cache):
    """
    :return: tuple[list[user_ids]], list[list[location_type_name, location_id]]
    """
    location_filename = _get_filename("locations", "", domain, "")
    user_filename = _get_filename("users", "", domain, "")
    if clear_cache or not os.path.exists(user_filename) or not os.path.exists(location_filename):
        location = SQLLocation.active_objects.get(domain=domain, location_type__name="state", name=state)
        all_locations = location.get_descendants(include_self=True).values_list("location_type__name", "location_id")
        user_ids = []
        for type_name, location_id in all_locations:
            user_ids.extend(get_user_ids_by_location(domain, location_id))
        with open(location_filename, 'w') as f:
            f.writelines("{}\n".format(",".join(loc_data)) for loc_data in all_locations)
        with open(user_filename, 'w') as f:
            f.writelines(f"{user_id}\n" for user_id in user_ids)
    else:
        with open(location_filename, 'r') as f:
            location_data = [line.strip().split(",") for line in f.readlines()]
        with open(user_filename, 'r') as f:
            user_ids = {line.strip() for line in f.readlines()}
    return user_ids, location_data


def _get_filename(name, slug, domain, utcnow, ext="gz"):
    return '{}-{}-{}-{}.{}'.format(name, slug, domain, utcnow, ext)


def _get_dump_stream_filename(slug, domain, utcnow):
    return 'dump-{}-{}-{}.gz'.format(slug, domain, utcnow)
