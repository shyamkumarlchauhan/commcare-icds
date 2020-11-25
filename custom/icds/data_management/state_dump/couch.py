import json
from collections import Counter

from corehq.apps.domain.models import Domain
from corehq.apps.dump_reload.couch.dump import _get_toggles_to_migrate, get_doc_ids_to_dump
from corehq.apps.dump_reload.couch.id_providers import DocTypeIDProvider
from corehq.apps.dump_reload.exceptions import DomainDumpError
from corehq.apps.users.models import CommCareUser, WebUser
from dimagi.utils.couch.database import iter_docs

EXCLUDE_MODELS_SQL = {
    # exclude always
    "app_manager.AppReleaseByLocation",
    "case_importer",
    "cloudcare.ApplicationAccess",
    "cloudcare.SQLAppGroup",
    "data_interfaces.CaseRuleSubmission",
    "data_interfaces.DomainCaseRuleRun",
    "domain_migration_flags.DomainMigrationProgress",
    "linked_domain.DomainLinkHistory",
    "domain_migration_flags.DomainMigrationProgress",
    "smsforms.SQLXFormsSession",
    "phonelog",
    "scheduling_partitioned.AlertScheduleInstance",
    "scheduling_partitioned.CaseAlertScheduleInstance",
    "scheduling_partitioned.CaseTimedScheduleInstance",
    "scheduling_partitioned.TimedScheduleInstance",
    "sms.MessagingEvent",
    "sms.MessagingSubEvent",
    "sms.QueuedSMS",
    "sms.SMS",
    "smsforms.SQLXFormsSession",

    # filter (mabye)
    "CaseExportInstance",
    "FormExportInstance",

    # filter
    "auth.User",
    "mobile_auth.SQLMobileAuthKeyRecord"
    "ota.DemoUserRestore",
    "locations.SQLLocation",
    "blobs.BlobMeta"
    "form_processor.XFormInstanceSQL",
    "form_processor.XFormOperationSQL",
    "form_processor.CommCareCaseSQL",
    "form_processor.CommCareCaseIndexSQL",
    "form_processor.CaseAttachmentSQL",
    "form_processor.CaseTransaction",
    "form_processor.LedgerValue",
    "form_processor.LedgerTransaction",
}

EXCLUDE_MODELS_COUCH = {
    "Repeater",
    "RepeatRecord",

    # filter maybe
    "Group",
    "ReportNotification",
    "ReportConfig",
    "FormExportInstance",
    "CaseExportInstance",
    "ExportInstance",
    "Application",  # builds up to 2018-03-06 12:33:32.617858

    # Filter
    "LinkedApplication",
    "MobileAuthKeyRecord",
    "WebUser",
    "CommCareUser",
}


def dump_couch_data(domain, user_ids, output, blob_meta_output):
    data = get_couch_data(domain, user_ids)
    counter = Counter()
    for obj in _extract_blob_meta(data, blob_meta_output):
        counter[obj["doc_type"]] += 1
        json.dump(obj.to_json(), output)
        output.write('\n')
    return counter


def get_couch_data(domain, user_ids):
    dumpers = [
        get_domain,
        get_toggles,
        unfiltered_docs,
        users,
        mobile_auth_records,
        application,
    ]
    for dumper in dumpers:
        for obj in dumper(domain, user_ids):
            yield obj


def get_domain(domain: str, user_ids: set):
    domain_obj = Domain.get_by_name(domain, strict=True)
    if not domain_obj:
        raise DomainDumpError("Domain not found: {}".format(domain))

    yield domain_obj.to_json()


def get_toggles(domain: str, user_ids: set):
    return _get_toggles_to_migrate(domain, user_ids)


def unfiltered_docs(domain, user_ids):
    for doc_class, doc_ids in get_doc_ids_to_dump(domain, EXCLUDE_MODELS_COUCH):
        couch_db = doc_class.get_db()
        yield from iter_docs(couch_db, doc_ids, chunksize=500)


def users(domain, user_ids):
    return iter_docs(CommCareUser.get_db(), user_ids, chunksize=500)


def mobile_auth_records(domain, user_ids):
    doc_class, doc_ids = DocTypeIDProvider('MobileAuthKeyRecord').get_doc_ids(domain)
    for doc in iter_docs(doc_class.get_db(), doc_ids, chunksize=500):
        if doc["user_id"] in user_ids:
            yield doc


def application(domain, web_user_ids, mobile_user_ids):
    # Application, LinkedApplication
    # exclude doc["is_auto_generated"]
    raise NotImplementedError


def _extract_blob_meta(docs_stream, output_stream):
    for doc in docs_stream:
        for meta in _get_blob_meta(doc):
            output_stream.write(",".join(meta))
        yield doc


def _get_blob_meta(doc):
    if "external_blobs" in doc:
        for meta in doc["external_blobs"].values():
            yield meta["blobmeta_id"], meta["key"]
