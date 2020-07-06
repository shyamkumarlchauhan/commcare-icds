from functools import wraps

from django.contrib import messages
from django.http import JsonResponse
from django.utils.safestring import mark_safe
from django.utils.translation import ugettext_lazy
from django.utils.translation import ugettext as _

from corehq.apps.hqwebapp.views import no_permissions
from custom.icds.const import ICDS_DOMAIN, IS_ICDS_ENVIRONMENT

DATA_INTERFACE_ACCESS_DENIED = mark_safe(ugettext_lazy(
    "This project has blocked access to interfaces that edit data for forms and cases"
))


def check_data_interfaces_blocked_for_domain(view_func):
    @wraps(view_func)
    def _inner(request, domain, *args, **kwargs):
        if is_icds_cas_project(domain):
            return no_permissions(request, message=DATA_INTERFACE_ACCESS_DENIED)
        else:
            return view_func(request, domain, *args, **kwargs)
    return _inner


def check_edit_access_for_domain(fn):
    @wraps(fn)
    def _check_edit_access_for_domain(request, domain, *args, **kwargs):
        if is_icds_cas_project(domain):
            messages.error(request, _("You don't have permission to edit these fields"))
            return JsonResponse({'success': 0})
        else:
            return fn(request, domain, *args, **kwargs)
    return _check_edit_access_for_domain


def is_icds_cas_project(domain):
    return IS_ICDS_ENVIRONMENT and domain == ICDS_DOMAIN
