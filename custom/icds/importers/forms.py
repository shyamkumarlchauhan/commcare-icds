from django import forms
from django.core.exceptions import ValidationError
from django.utils.translation import ugettext_lazy

from crispy_forms import layout as crispy
from crispy_forms.bootstrap import StrictButton
from crispy_forms.helper import FormHelper

from corehq.util.workbook_json.excel import get_workbook

from extensions.icds.custom.icds.importers import (
    validate_dashboard_users_upload,
    validate_inventory_upload,
    validate_mobile_users_upload,
)


class ValidateUserInventoryImportsForm(forms.Form):
    dashboard_users_file = forms.FileField(label="Dashboard Users")
    mobile_users_file = forms.FileField(label="Mobile Users")
    inventory_file = forms.FileField(label="Inventory")
    india_district_file = forms.FileField(label="India District")

    def __init__(self, request, *args, **kwargs):
        self.request = request
        super(ValidateUserInventoryImportsForm, self).__init__(*args, **kwargs)
        self.helper = FormHelper()
        self.helper.form_method = 'post'
        self.helper.layout = crispy.Layout(
            crispy.Fieldset(
                "",
                crispy.Div(
                    crispy.Field(
                        'dashboard_users_file',
                        data_bind="value: file",
                    ),
                ),
                crispy.Div(
                    crispy.Field(
                        'mobile_users_file',
                        data_bind="value: file",
                    ),
                ),
                crispy.Div(
                    crispy.Field(
                        'inventory_file',
                        data_bind="value: file",
                    ),
                ),
                crispy.Div(
                    crispy.Field(
                        'india_district_file',
                        data_bind="value: file",
                    ),
                ),
            ),
            StrictButton(
                ugettext_lazy('Validate'),
                css_class='btn-primary',
                type='submit',
            ),
        )

    def clean(self):
        error_list = {}
        dashboard_users_worksheet = get_workbook(self.cleaned_data['dashboard_users_file']).worksheets[0]
        error_list['dashboard_users_file'] = validate_dashboard_users_upload(dashboard_users_worksheet)

        mobile_users_worksheet = get_workbook(self.cleaned_data['mobile_users_file']).worksheets[0]
        error_list['mobile_users_file'] = validate_mobile_users_upload(mobile_users_worksheet)

        # read it again so that rows are available
        mobile_users_worksheet = get_workbook(self.cleaned_data['mobile_users_file']).worksheets[0]
        inventory_worksheet = get_workbook(self.cleaned_data['inventory_file']).worksheets[0]
        india_district_worksheet = get_workbook(self.cleaned_data['india_district_file']).worksheets[0]
        error_list['inventory_file'] = validate_inventory_upload(mobile_users_worksheet, inventory_worksheet,
                                                                 india_district_worksheet, self._get_awc_count())

        if error_list:
            raise ValidationError(error_list)

    def _get_awc_count(self):
        inventory_filename = self.cleaned_data['inventory_file'].name
        try:
            # filename is like MN_07Dist_4087AWC_Inventory
            return int(''.join([i for i in list(inventory_filename.split("_")[2]) if i.isdigit()]))
        except:
            raise ValidationError("Could not find number of AWCs from inventory filename")
