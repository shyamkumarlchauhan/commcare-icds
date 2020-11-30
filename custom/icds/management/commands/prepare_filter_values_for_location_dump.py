import json
import os

from django.core.management.base import BaseCommand
from django.utils.functional import cached_property
from django.utils.text import slugify

from corehq.apps.cleanup.utils import confirm
from corehq.apps.locations.dbaccessors import get_users_by_location_id
from corehq.apps.locations.models import SQLLocation

OWNER = "owner_ids"

USER = "user_data"

LOCATION = "location_ids"


class Command(BaseCommand):
    help = """Prepare ID files for dumping state data

    Use in conjunction with `dump_data_by_location`
    """

    def add_arguments(self, parser):
        parser.add_argument('domain_name')
        parser.add_argument('location_id_or_name', help="The name or ID of the location")
        parser.add_argument('--json-output', action="store_true", help="Produce JSON output for use in tests")

    def handle(self, domain_name, location_id_or_name, **options):
        context = FilterContext(domain_name, location_id_or_name)
        existing = context.files_exist()
        if existing:
            print("This will overwrite the following files:\n\t{}".format("\n\t".join(existing)))
            if not confirm("Do you want to continue?"):
                return

        try:
            location = SQLLocation.active_objects.get(location_id=location_id_or_name)
        except SQLLocation.DoesNotExist:
            locations = list(SQLLocation.active_objects.filter(domain=domain_name, name=location_id_or_name))
            if len(locations) > 1:
                print(f"Multiple locations found with name = '{location_id_or_name}'. Select one and use the ID "
                      f"instead of the name:")
                for location in locations:
                    print(f"[{location.location_type.name}] {location.name}: {location.location_id}")
                return

            location = locations[0]

        location_ids = list(location.get_descendants(include_self=True).values_list("location_id", flat=True))
        user_data = []
        owner_ids = []
        for location_id in location_ids:
            for user in get_users_by_location_id(domain_name, location_id):
                user_data.append(",".join([user._id, user.username]))
                owner_ids.extend(user.get_owner_ids())

        context.write_data(location_ids, user_data, owner_ids)
        if options.get("json_output"):
            # this is used in tests to allow cleanup of the files
            return json.dumps({
                "paths": {
                    "location": context.location_id_file,
                    "user": context.user_data_file,
                    "owner": context.owner_id_file,
                }
            })


class FilterContext:
    def __init__(self, domain, location, types=None):
        self.location_id_file = get_location_id_filename(domain, location, LOCATION)
        self.user_data_file = get_location_id_filename(domain, location, USER)
        self.owner_id_file = get_location_id_filename(domain, location, OWNER)
        self.types = types

    @cached_property
    def user_ids(self):
        return {
            row[0] for row in self.user_data
        }

    @cached_property
    def usernames(self):
        return {
            row[1] for row in self.user_data
        }

    @cached_property
    def user_data(self):
        return [line.split(",") for line in self.load_file(self.user_data_file)]

    @cached_property
    def owner_ids(self):
        return self.load_file(self.owner_id_file)

    @cached_property
    def location_ids(self):
        return self.load_file(self.location_id_file)

    def load_file(self, filename):
        with open(filename, 'r') as f:
            return {line.strip() for line in f.readlines()}

    def files_exist(self):
        return [
            path
            for path in (self.location_id_file, self.user_data_file, self.owner_id_file)
            if os.path.exists(path)
        ]

    def validate(self):
        return len(self.files_exist()) == 3

    def write_data(self, location_ids, user_data, owner_ids):
        _write_data(self.location_id_file, location_ids)
        _write_data(self.user_data_file, user_data)
        _write_data(self.owner_id_file, owner_ids)


def _write_data(filename, data):
    print(f"Writing {len(data)} records to {filename}")
    with open(filename, 'w') as f:
        f.writelines(f"{item}\n" for item in data)


def get_location_id_filename(domain, state, name):
    state = slugify(state)
    return f'{domain}-{state}-{name}.csv'
