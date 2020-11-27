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
        parser.add_argument('state', help="The name of the state")

    def handle(self, domain_name, state, **options):
        context = FilterContext(domain_name, state)
        existing = context.files_exist()
        if existing:
            print("This will overwrite the following files:\n\t{}".format("\n\t".join(existing)))
            if not confirm("Do you want to continue?"):
                return

        location = SQLLocation.active_objects.get(domain=domain_name, location_type__name="state", name=state)
        location_ids = list(location.get_descendants(include_self=True).values_list("location_id", flat=True))
        user_data = []
        owner_ids = []
        for location_id in location_ids:
            for user in get_users_by_location_id(domain_name, location_id):
                user_data.append(",".join([user._id, user.username]))
                owner_ids.extend(user.get_owner_ids())

        context.write_data(location_ids, user_data, owner_ids)


class FilterContext:
    def __init__(self, domain, state, types=None):
        self.location_id_file = get_state_id_filename(domain, state, LOCATION)
        self.user_data_file = get_state_id_filename(domain, state, USER)
        self.owner_id_file = get_state_id_filename(domain, state, OWNER)
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


def get_state_id_filename(domain, state, name):
    state = slugify(state)
    return f'{domain}-{state}-{name}.csv'
