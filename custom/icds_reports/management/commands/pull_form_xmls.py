import csv
import re
import os

from dateutil import parser

from django.core.management.base import BaseCommand
from corehq.apps.es import UserES
from corehq.apps.locations.models import SQLLocation
from corehq.form_processor.interfaces.dbaccessors import FormAccessors
from corehq.apps.es.forms import FormES
from xml.dom.minidom import parseString


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument("domain", help="Name of domain")
        parser.add_argument("location_id", help="Name of domain")
        parser.add_argument(
            'start_date',
            type=date_type,
            help='The start date (inclusive). format YYYY-MM-DD'
        )
        parser.add_argument(
            'end_date',
            type=date_type,
            help='The end date (exclusive). format YYYY-MM-DD'
        )

    def get_user_ids_under_location(self, location_id, domain ):
        location_ids = SQLLocation.objects.get_locations_and_children_ids([location_id])
        user_query = UserES().domain(domain).mobile_users()
        user_query = user_query.location(location_ids)
        user_ids = [user.get('_id') for user in user_query.run().hits]
        return user_ids

    def get_xmls_by_forms(self, domain, forms):
        fa = FormAccessors(domain)
        form_xmls = []

        counter = 0
        for form in forms:
            form_xml = str(fa.get_form(form.get('_id')).get_xml())
            form_date = parser.parse(form.get('received_on')).date()
            form_xmls.append((form_date, form_xml))
            if count % 100 == 0:
                print("DONE {}/{}".format(count, len(forms)))
            count += 1
        return form_xmls

    def get_cleaned_xmls(self, form_xmls):
        regex = re.compile(r'<[A-Za-z0-9]*[:]*aadhar_number>') # regex to find all aadhar_number tags
        cleaned_form_xmls = list()
        counter = 0
        for form_date, xml in form_xmls:
            aadhar_tags = regex.findall(xml)
            xform = parseString(xml)

            for aadhar_tag in aadhar_tags:
                tag_name = aadhar_tag[1:-1]
                for node in xform.getElementsByTagName(tag_name):
                    node.childNodes = []  # clear the Aadhar number tag

            form_name = xform.getElementsByTagName('data')[0].attributes.get('name').value
            file_name = f"{form_name}_{form_date}.xml"
            form = (file_name, str(xform.toprettyxml()))
            cleaned_form_xmls.append(form)
            if count % 100 == 0:
                print("DONE {}/{}".format(count, len(forms)))
            count += 1

        return cleaned_form_xmls

    def handle(self, domain, location_id, start_date, end_date, **options):
        user_ids = self.get_user_ids_under_location(location_id, domain)
        print("Pulled user ids")
        form_query = FormES().domain(domain).submitted(gte=start_date, lt=end_date)
        form_query = form_query.user_id(user_ids)
        forms = form_query.run().hits
        print("Pulled forms")
        form_xmls = self.get_xmls_by_forms(domain, forms)
        print("pulled form xmls")
        cleaned_form_xmls = get_cleaned_xmls(form_xmls)
        print("pulled cleaned xmls, now writing to disk")

        directory_name = "all_xml_forms_{start_date}_to_{end_date}"

        if not os.path.isdir(directory_name):
            os.mkdir(directory_name)

        for file_name, form_xml in cleaned_form_xmls:
            file_path = f'{directory_name}/{file_name}'
            with(file_path, 'w') as fout:
                fout.write(form_xml)


