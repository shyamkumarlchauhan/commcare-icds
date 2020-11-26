import itertools
from collections import Counter
from io import StringIO

from corehq.apps.dump_reload.sql.dump import get_all_model_iterators_builders_for_domain, \
    get_objects_to_dump_from_builders, get_model_iterator_builders_to_dump
from corehq.apps.dump_reload.sql.filters import FilteredModelIteratorBuilder, UsernameFilter, SimpleFilter, IDFilter
from corehq.apps.dump_reload.sql.serialization import JsonLinesSerializer
from corehq.apps.dump_reload.util import get_model_class
from corehq.blobs import CODES
from corehq.blobs.models import BlobMeta
from corehq.sql_db.util import split_list_by_db_partition
from dimagi.utils.chunked import chunked

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
    "ota.DemoUserRestore",  # can be re-generated

    # filter
    "auth.User",
    "mobile_auth.SQLMobileAuthKeyRecord",
    "locations.SQLLocation",
    "blobs.BlobMeta",
    "form_processor.XFormInstanceSQL",
    "form_processor.XFormOperationSQL",
    "form_processor.CommCareCaseSQL",
    "form_processor.CommCareCaseIndexSQL",
    "form_processor.CaseAttachmentSQL",
    "form_processor.CaseTransaction",
    "form_processor.LedgerValue",
    "form_processor.LedgerTransaction",
}


def dump_simple_sql_data(domain, context, output):
    stats = Counter()
    data = get_simple_sql_data(domain, context, stats)
    JsonLinesSerializer().serialize(
        data,
        use_natural_foreign_keys=False,
        use_natural_primary_keys=True,
        stream=output
    )
    return stats


def dump_form_case_data(domain, context, output, blob_output):
    stats = Counter()
    data = get_form_case_data(domain, context, blob_output, stats)
    JsonLinesSerializer().serialize(
        data,
        use_natural_foreign_keys=False,
        use_natural_primary_keys=True,
        stream=output
    )
    return stats


def get_simple_sql_data(domain, context, stats):
    unfiltered_builders = get_model_iterator_builders_to_dump(domain, EXCLUDE_MODELS_SQL)
    filtered_builders = get_prepared_builders(domain, [
        FilteredModelIteratorBuilder('auth.User', UsernameFilter(context.user_ids)),
        FilteredModelIteratorBuilder(
            "mobile_auth.SQLMobileAuthKeyRecord", IDFilter("user_id", context.user_ids)
        ),
        FilteredModelIteratorBuilder(
            "locations.SQLLocation", IDFilter("location_id", context.location_ids)
        )
    ])
    builders = itertools.chain(unfiltered_builders, filtered_builders)
    yield from get_objects_to_dump_from_builders(builders, stats, StringIO())


def get_prepared_builders(domain, builders):
    for builder in builders:
        _, model_class = get_model_class(builder.model_label)
        yield from get_all_model_iterators_builders_for_domain(
            model_class, domain, [builder]
        )


class AndFilter:
    def __init__(self, left, right):
        self.left = left
        self.right = right

    def get_filters(self, domain_name):
        for left in self.left.get_filters(domain_name):
            for right in self.right.get_filters(domain_name):
                yield left & right


def get_form_case_data(domain, context, blob_output, stats):
    form_filter = AndFilter(IDFilter("user_id", context.user_ids), SimpleFilter("domain"))
    case_filter = AndFilter(IDFilter("owner_id", context.owner_ids), SimpleFilter("domain"))
    builders = [
        (
            FilteredModelIteratorBuilder("form_processor.XFormInstanceSQL", form_filter),
            [("form_processor.XFormOperationSQL", "form")]
        ),
        (
            FilteredModelIteratorBuilder("form_processor.CommCareCaseSQL", case_filter),
            [
                ("form_processor.CommCareCaseIndexSQL", "case"),
                ("form_processor.CaseTransaction", "case"),
                ("form_processor.LedgerValue", "case"),
                ("form_processor.LedgerTransaction", "case"),
            ]
        ),
     ]
    for builder, related_models in builders:
        for model_class, prepared_builder in get_prepared_builders(domain, [builder]):
            for iterator in prepared_builder.iterators():
                for chunk in chunked(iterator, 500, list):
                    yield from chunk
                    stats[model_class] += len(chunk)
                    dump_form_attachments(model_class, chunk, blob_output, stats)
                    yield from get_related(prepared_builder, chunk, related_models, stats)


def get_related(builder, models, related_models, stats):
    for related_label, related_field in related_models:
        _, model_class = get_model_class(related_label)
        related_models = model_class.objects.using(builder.db_alias).filter(
            **{f"{related_field}__in": models}
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
            stats['blobs.BlobMeta'] += 1
            yield obj
