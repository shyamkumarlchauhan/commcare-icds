import itertools
import logging
from collections import Counter
from io import StringIO

from django.db import router

from corehq.apps.dump_reload.sql.dump import get_all_model_iterators_builders_for_domain, \
    get_objects_to_dump_from_builders, get_model_iterator_builders_to_dump, APP_LABELS_WITH_FILTER_KWARGS_TO_DUMP
from corehq.apps.dump_reload.sql.filters import FilteredModelIteratorBuilder, UsernameFilter, SimpleFilter, IDFilter
from corehq.apps.dump_reload.sql.serialization import JsonLinesSerializer
from corehq.apps.dump_reload.util import get_model_class
from corehq.blobs import CODES
from corehq.blobs.models import BlobMeta
from corehq.sql_db.config import plproxy_config
from corehq.sql_db.util import split_list_by_db_partition
from corehq.util.log import with_progress_bar
from custom.icds.data_management.state_dump.couch import BLOB_META_STATS_KEY
from dimagi.utils.chunked import chunked

logger = logging.getLogger("sql_dump")


ALWAYS_EXCLUDE_MODELS_SQL = {
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
    "ota.DemoUserRestore",  # can be re-generated
    "blobs.BlobMeta",  # these are exported along with other models
    "sms.PhoneNumber",  # these are exported along with other models
}

SHARDED_MODELS = {
    "form_processor.XFormInstanceSQL",
    "form_processor.XFormOperationSQL",
    "form_processor.CommCareCaseSQL",
    "form_processor.CommCareCaseIndexSQL",
    "form_processor.CaseAttachmentSQL",
    "form_processor.CaseTransaction",
    "form_processor.LedgerValue",
    "form_processor.LedgerTransaction",
}

FILTERED_SQL_MODELS = {
    "auth.User",
    "mobile_auth.SQLMobileAuthKeyRecord",
    "locations.SQLLocation",
} | SHARDED_MODELS


AVAILABLE_SQL_TYPES = (
    set(APP_LABELS_WITH_FILTER_KWARGS_TO_DUMP) -
    ALWAYS_EXCLUDE_MODELS_SQL -
    SHARDED_MODELS
) | {
    "form_processor.XFormInstanceSQL",
    "form_processor.CommCareCaseSQL"
}


def dump_simple_sql_data(domain, context, output, blob_meta_output):
    stats = Counter()
    data = get_simple_sql_data(domain, context, stats)
    JsonLinesSerializer().serialize(
        data,
        use_natural_foreign_keys=False,
        use_natural_primary_keys=True,
        stream=output
    )
    return stats


def dump_form_case_data(domain, context, output, blob_meta_output, limit_to_db=None):
    stats = Counter()
    data = get_form_case_data(domain, context, blob_meta_output, stats, limit_to_db=limit_to_db)
    JsonLinesSerializer().serialize(
        data,
        use_natural_foreign_keys=False,
        use_natural_primary_keys=True,
        stream=output
    )
    return stats


def get_simple_sql_data(domain, context, stats):
    exclude = ALWAYS_EXCLUDE_MODELS_SQL | FILTERED_SQL_MODELS
    if context.types:
        excluded_types = set(context.types).intersection(ALWAYS_EXCLUDE_MODELS_SQL)
        if excluded_types:
            raise Exception(f"The following types are always excluded: {excluded_types}")

        all_types = set(APP_LABELS_WITH_FILTER_KWARGS_TO_DUMP.keys())
        exclude = all_types - set(context.types) - FILTERED_SQL_MODELS

    exclude.add("blobs.BlobMeta")
    unfiltered_builders = get_model_iterator_builders_to_dump(domain, exclude)
    builders = [
        FilteredModelIteratorBuilder('auth.User', UsernameFilter(context.user_ids)),
        FilteredModelIteratorBuilder(
            "mobile_auth.SQLMobileAuthKeyRecord", IDFilter("user_id", context.user_ids)
        ),
        FilteredModelIteratorBuilder(
            "locations.SQLLocation", IDFilter("location_id", context.location_ids)
        ),
        FilteredModelIteratorBuilder(
            "sms.PhoneNumber", IDFilter("owner_id", context.user_ids)
        )
    ]
    if context.types:
        builders = [
            b for b in builders
            if b.model_label in context.types or b.model_label.split(".")[0] in context.types
        ]
    filtered_builders = get_prepared_builders(domain, builders)
    builders = list(itertools.chain(unfiltered_builders, filtered_builders))

    total_count = sum([query.count() for _, builder in builders for query in builder.querysets()])

    yield from with_progress_bar(
        get_objects_to_dump_from_builders(builders, stats, StringIO()),
        length=total_count,
        prefix="[sql] Dumping data from pgmain",
        oneline=False
    )


def get_prepared_builders(domain, builders, limit_to_db=None):
    for builder in builders:
        _, model_class = get_model_class(builder.model_label)
        yield from get_all_model_iterators_builders_for_domain(
            model_class, domain, [builder], limit_to_db=limit_to_db
        )


class AndFilter:
    def __init__(self, left, right):
        self.left = left
        self.right = right

    def get_filters(self, domain_name):
        for left in self.left.get_filters(domain_name):
            for right in self.right.get_filters(domain_name):
                yield left & right


class RelatedFilter:
    def __init__(self, related_field):
        self.related_field = related_field

    def get_filters(self, models):
        return {f"{self.related_field}__in": models}


class RelatedFilterById(RelatedFilter):
    def __init__(self, related_field, model_id_field):
        super().__init__(related_field)
        self.model_id_field = model_id_field

    def get_filters(self, models):
        return {f"{self.related_field}__in": [getattr(model, self.model_id_field) for model in models]}


def get_form_case_data(domain, context, blob_output, stats, limit_to_db=None):
    if not context.types or "form_processor.XFormInstanceSQL" in context.types:
        form_filter = AndFilter(IDFilter("user_id", context.user_ids), SimpleFilter("domain"))
        builders = [FilteredModelIteratorBuilder("form_processor.XFormInstanceSQL", form_filter)]
        related = [("form_processor.XFormOperationSQL", RelatedFilter("form"))]
        yield from _get_data_with_related(domain, builders, related, limit_to_db, stats, blob_output, 'forms')

    if not context.types or "form_processor.CommCareCaseSQL" in context.types:
        case_filter = AndFilter(IDFilter("owner_id", context.owner_ids), SimpleFilter("domain"))
        builders = [FilteredModelIteratorBuilder("form_processor.CommCareCaseSQL", case_filter),]
        related = [
            ("form_processor.CommCareCaseIndexSQL", RelatedFilter("case")),
            ("form_processor.CaseTransaction", RelatedFilter("case")),
            ("form_processor.LedgerValue", RelatedFilter("case")),
            ("form_processor.LedgerTransaction", RelatedFilter("case")),
            ("sms.PhoneNumber", RelatedFilterById("owner_id", "case_id")),
        ]
        yield from _get_data_with_related(domain, builders, related, limit_to_db, stats, blob_output, 'forms')


def _get_data_with_related(domain, builders, related_data_filters, limit_to_db, stats, blob_output, slug):
    prepared_builders = list(get_prepared_builders(domain, [builders], limit_to_db=limit_to_db))
    total_chunks = sum(
        len(prepared_builder.querysets())
        for _, prepared_builder in prepared_builders
    )

    def _get_iterations():
        for model_class, prepared_builder in prepared_builders:
            for iterator in prepared_builder.iterators():
                yield model_class, prepared_builder, iterator, related_data_filters

    generator = with_progress_bar(_get_iterations(), length=total_chunks, prefix=f"[sql] Dumping {slug}", oneline=False)
    for model_class, prepared_builder, iterator, related_models in generator:
        for chunk in chunked(iterator, 500, list):
            yield from chunk
            stats[prepared_builder.model_label] += len(chunk)
            dump_form_attachments(model_class, chunk, blob_output, stats)
            yield from get_related(prepared_builder, chunk, related_models, stats)


def get_related(builder, models, related_models, stats):
    for related_label, related_filter in related_models:
        _, model_class = get_model_class(related_label)
        using = builder.db_alias
        db_for_write = router.db_for_write(model_class, using=using)
        if db_for_write != using and db_for_write != plproxy_config.proxy_db:
            using = db_for_write
        related_models = model_class.objects.using(using).filter(
            **related_filter.get_filters(models)
        ).iterator()
        for model in related_models:
            stats[related_label] += 1
            yield model


def dump_form_attachments(model_class, models, output, stats):
    if model_class.__name__ != "XFormInstanceSQL":
        return

    data = get_form_attachments(models, stats)
    JsonLinesSerializer().serialize(
        data,
        use_natural_foreign_keys=False,
        use_natural_primary_keys=True,
        stream=output
    )


def get_form_attachments(models, stats):
    form_ids = [model.form_id for model in models]
    for db, parent_ids in split_list_by_db_partition(form_ids):
        meta = BlobMeta.objects.using(db).filter(
            type_code=CODES.form_xml, parent_id__in=parent_ids
        ).iterator()
        for obj in meta:
            stats[BLOB_META_STATS_KEY] += 1
            yield obj
