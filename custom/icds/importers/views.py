from django.contrib import messages
from django.shortcuts import redirect
from django.utils.functional import cached_property
from django.utils.translation import ugettext, ugettext_noop

from corehq.apps.users.views.mobile.users import BaseManageCommCareUserView
from extensions.icds.custom.icds.importers.forms import ValidateUserInventoryImportsForm


class ValidateUserInventoryImportsView(BaseManageCommCareUserView):
    template_name = 'icds/importers/validate_users_and_inventory_imports.html'
    urlname = 'validate_users_and_inventory_imports'
    page_title = ugettext_noop("Validate users and inventory uploads")

    @property
    def page_context(self):
        return {
            'form': self._form
        }

    @cached_property
    def _form(self):
        if self.request.method == "POST":
            return ValidateUserInventoryImportsForm(
                self.request,
                self.request.POST,
                self.request.FILES,
            )
        else:
            return ValidateUserInventoryImportsForm(self.request)

    def post(self, request, *args, **kwargs):
        if self._form.is_valid():
            messages.success(request, ugettext("No errors found"))
            return redirect(self.urlname, self.domain)
        return self.get(request, *args, **kwargs)
