import json
import logging
from collections import Counter

from corehq.apps.domain.dbaccessors import get_doc_ids_in_domain_by_type
from corehq.apps.domain.models import Domain
from corehq.apps.dump_reload.couch.dump import _get_toggles_to_migrate, get_doc_ids_to_dump, DOC_PROVIDERS_BY_DOC_TYPE
from corehq.apps.dump_reload.exceptions import DomainDumpError
from corehq.apps.dump_reload.sql.serialization import JsonLinesSerializer
from corehq.apps.users.models import CommCareUser
from corehq.blobs.models import BlobMeta
from corehq.util.couch import get_document_class_by_doc_type
from dimagi.utils.couch.database import iter_docs


logger = logging.getLogger("couch_dump")


BLOB_META_STATS_KEY = "blob_meta"

EXCLUDE_COUCH_DOC_TYPES = {
    "Repeater",
    "RepeatRecord",
    "Group",
    "CommCareMultimedia",  # new app being developed with it's own MM
    "WebUser",  # excluding these since they are all internal Dimagi users

    # filter maybe
    "ReportNotification",
    "ReportConfig",
    "FormExportInstance",
    "CaseExportInstance",
    "ExportInstance",
    "Application",  # builds up to 2018-03-06 12:33:32.617858
    "LinkedApplication",  # new app being created
}

FILTERED_COUCH_DOC_TYPES = {
    "MobileAuthKeyRecord",
    "CommCareUser",
}

AVAILABLE_COUCH_TYPES = list(
    (set(DOC_PROVIDERS_BY_DOC_TYPE) - EXCLUDE_COUCH_DOC_TYPES) |
    FILTERED_COUCH_DOC_TYPES
)


def dump_domain_object(domain, context, output, blob_meta_output):
    data = get_domain(domain, context)
    return _dump_couch_data(data, output, blob_meta_output, meta_tag="Domain")


def dump_toggle_data(domain, context, output, blob_meta_output):
    data = get_toggles(domain, context)
    return _dump_couch_data(data, output, blob_meta_output, meta_tag="Toggle")


def dump_couch_data(domain, context, output, blob_meta_output):
    data = get_couch_data(domain, context)
    return _dump_couch_data(data, output, blob_meta_output)


def _dump_couch_data(data, output, blob_meta_output, meta_tag=None):
    counter = Counter()
    doc_types = {}
    current_doc_type = None

    for obj in _extract_blob_meta(data, blob_meta_output, counter):
        doc_type = obj["doc_type"]
        if doc_type != current_doc_type:
            if current_doc_type:
                logger.info(f"\tDump of {current_doc_type} complete: {counter[meta_tag]}")
                meta_tag = None  # reset for next doc_type
            logger.info(f"Starting dump of {doc_type}")
            current_doc_type = doc_type

        if not meta_tag:
            if doc_type not in doc_types:
                doc_types[doc_type] = get_document_class_by_doc_type(doc_type)
            meta_tag = '{}.{}'.format(doc_types[doc_type]._meta.app_label, doc_types[doc_type].__name__)

        counter[meta_tag] += 1
        if counter[meta_tag] and counter[meta_tag] % 500 == 0:
            logger.info(f"\t{doc_type} progress: {counter[meta_tag]}")

        total_progress = sum(counter.values())
        if total_progress % 1000 == 0:
            logger.info(f">>> Total couch progress: {total_progress}")
        json.dump(obj, output)
        output.write('\n')
    return counter


def get_couch_data(domain, context):
    dumper_map = {
        "CommCareUser": users,
        "MobileAuthKeyRecord": mobile_auth_records,
    }
    if context.types:
        dumpers = [
            dumper for doc_type, dumper in dumper_map.items()
            if doc_type in context.types
        ]
        dumpers.append(unfiltered_docs)
    else:
        dumpers = [unfiltered_docs] + list(dumper_map.values())

    for dumper in dumpers:
        yield from dumper(domain, context)


def get_domain(domain: str, context):
    domain_obj = Domain.get_by_name(domain, strict=True)
    if not domain_obj:
        raise DomainDumpError("Domain not found: {}".format(domain))

    yield domain_obj.to_json()


def get_toggles(domain: str, context):
    return _get_toggles_to_migrate(domain, context.usernames)


def unfiltered_docs(domain, context):
    excluded_types = EXCLUDE_COUCH_DOC_TYPES | FILTERED_COUCH_DOC_TYPES
    if context.types:
        excluded_types = set(context.types).intersection(EXCLUDE_COUCH_DOC_TYPES)
        if excluded_types:
            raise Exception(f"The following doc types are always excluded: {excluded_types}")
        all_doc_types = set(DOC_PROVIDERS_BY_DOC_TYPE.keys())
        excluded_types = all_doc_types - set(context.types) - FILTERED_COUCH_DOC_TYPES

    for doc_class, doc_ids in get_doc_ids_to_dump(domain, excluded_types):
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


def application(domain, context):
    """Excluding apps for now. A new app will be built."""
    # Application, LinkedApplication
    # exclude doc["is_auto_generated"]
    return []


def _extract_blob_meta(docs_stream, output_stream, counter):
    for doc in docs_stream:
        blobs = list(_get_blob_meta(doc))
        counter[BLOB_META_STATS_KEY] += len(blobs)
        JsonLinesSerializer().serialize(
            blobs,
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
