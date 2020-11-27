import json
from collections import Counter

from corehq.apps.domain.dbaccessors import get_doc_ids_in_domain_by_type
from corehq.apps.domain.models import Domain
from corehq.apps.dump_reload.couch.dump import _get_toggles_to_migrate, get_doc_ids_to_dump
from corehq.apps.dump_reload.exceptions import DomainDumpError
from corehq.apps.dump_reload.sql.serialization import JsonLinesSerializer
from corehq.apps.users.models import CommCareUser
from corehq.blobs.models import BlobMeta
from corehq.util.couch import get_document_class_by_doc_type
from dimagi.utils.couch.database import iter_docs

EXCLUDE_MODELS_COUCH = {
    "Repeater",
    "RepeatRecord",
    "Group",
    "CommCareMultimedia",  # new app being developed with it's own MM

    # filter maybe
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


def dump_couch_data(domain, context, output, blob_meta_output):
    data = get_couch_data(domain, context)
    counter = Counter()
    for obj in _extract_blob_meta(data, blob_meta_output):
        counter[obj["doc_type"]] += 1
        json.dump(obj, output)
        output.write('\n')
    return counter


def get_couch_data(domain, context):
    dumpers = [
        get_domain,
        get_toggles,
        unfiltered_docs,
        users,
        mobile_auth_records,
        application,
    ]
    for dumper in dumpers:
        for obj in dumper(domain, context):
            yield obj


def get_domain(domain: str, context):
    domain_obj = Domain.get_by_name(domain, strict=True)
    if not domain_obj:
        raise DomainDumpError("Domain not found: {}".format(domain))

    yield domain_obj.to_json()


def get_toggles(domain: str, context):
    return _get_toggles_to_migrate(domain, context.user_ids)


def unfiltered_docs(domain, context):
    for doc_class, doc_ids in get_doc_ids_to_dump(domain, EXCLUDE_MODELS_COUCH):
        couch_db = doc_class.get_db()
        yield from iter_docs(couch_db, doc_ids, chunksize=500)


def users(domain, context):
    return iter_docs(CommCareUser.get_db(), context.user_ids, chunksize=500)


def mobile_auth_records(domain, context):
    doc_class = get_document_class_by_doc_type('MobileAuthKeyRecord')
    doc_ids = get_doc_ids_in_domain_by_type(domain, 'MobileAuthKeyRecord')
    for doc in iter_docs(doc_class.get_db(), doc_ids, chunksize=500):
        if doc["user_id"] in context.user_ids:
            yield doc


def application(domain, web_user_ids, mobile_user_ids):
    """Excluding apps for now. A new app will be built."""
    # Application, LinkedApplication
    # exclude doc["is_auto_generated"]
    return []


def _extract_blob_meta(docs_stream, output_stream):
    for doc in docs_stream:
        JsonLinesSerializer().serialize(
            _get_blob_meta(doc),
            use_natural_foreign_keys=False,
            use_natural_primary_keys=True,
            stream=output_stream
        )
        yield doc


def _get_blob_meta(doc):
    if "external_blobs" in doc:
        for blob in doc["external_blobs"].values():
            try:
                yield BlobMeta.objects.partitioned_query(doc["_id"]).filter(
                    parent_id=doc["_id"], key=blob["key"]
                ).first()
            except BlobMeta.DoesNotExist:
                pass  # todo log?
