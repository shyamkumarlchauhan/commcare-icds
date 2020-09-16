from corehq.apps.es import UserES
from corehq.apps.locations.models import SQLLocation
from corehq.form_processor.interfaces.dbaccessors import FormAccessors


location_ids = SQLLocation.objects.get_locations_and_children_ids(['3c9e7afe194647fc818543bfddf01729'])
domain='icds-cas'
query = UserES().domain(domain).mobile_users()
query = query.location(location_ids)


user_ids = [user.get('_id') for user in query.run().hits]
fa = FormAccessors('icds-cas')


count = 0

print("total_users : {}".format(len(user_ids)))
for index, user_id in enumerate(user_ids):
	count += len(fa.get_form_ids_for_user(user.get('_id')))
	print("Counted for {}/{}".format(index, len(user_ids)))

