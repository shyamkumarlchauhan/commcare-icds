from hashlib import md5
import os
from corehq.apps.translations.app_translations.download import (
    get_bulk_app_sheets_by_name,
    get_bulk_app_single_sheet_by_name,
)
from corehq.apps.translations.const import (
    MODULES_AND_FORMS_SHEET_NAME,
    SINGLE_SHEET_NAME,
)
from corehq.apps.translations.app_translations.utils import get_bulk_app_sheet_headers


def generate_audio_path(text, previous_path):
    path_hash = md5(text.encode()).hexdigest()[:6]
    file_path_arr = previous_path.split('/')
    filename_index = len(file_path_arr) - 1
    complete_filename, extension = os.path.splitext(os.path.basename(previous_path))
    filename_arr = complete_filename.split('-')
    if len(filename_arr) >= 1:
        filename_arr[len(filename_arr) - 1] = path_hash
    else:
        filename_arr = [path_hash]
    new_filename = '-'.join(filename_arr) + extension
    file_path_arr[filename_index] = new_filename
    return '/'.join(file_path_arr)


def prepare_older_rows_dict(old_rows, headers):
    all_rows = []
    for row in old_rows:
        row_dict = {}
        for index, value in enumerate(row):
            row_dict[headers[index]] = value
        all_rows.append(row_dict)
    return all_rows


def update_audio_path_if_required(current_row, old_row, langs):
    '''
    Compares the uploaded row with one already present on app,
    if only text is updated it will create a new audio path for that particular language.
    if text and audio path both are changes it will raise a ValueError exceptions
    '''
    for lang in langs:
        audio_header = f'audio_{lang}'
        default_header = f'default_{lang}'

        text_changed = current_row[default_header] != old_row[default_header]
        audio_path_changed = current_row[audio_header] != old_row[audio_header]

        if text_changed and audio_path_changed:
            raise ValueError(_(f"You cannot update text and audio path\
            simulatenouly for label \"{current_row['label']}\" "))

        if text_changed and current_row[audio_header]:
            new_audio_path = generate_audio_path(
                current_row[default_header],
                current_row[audio_header])
            current_row[audio_header] = new_audio_path


def get_older_rows(app, lang, sheet_name, is_single_sheet):
    headers = get_bulk_app_sheet_headers(
        app, single_sheet=is_single_sheet, lang=lang, eligible_for_transifex_only=True
    )
    if is_single_sheet:
        headers = list(headers[0][1])
        older_sheet_details = get_bulk_app_single_sheet_by_name(app, lang, eligible_for_transifex_only=True)
        all_older_rows = prepare_older_rows_dict(older_sheet_details[SINGLE_SHEET_NAME], headers)
        module_or_form = None
        modules_and_forms_rows = []
        sheet_map = {}
        for row in all_older_rows:
            if not row['case_property'] and not row['list_or_detail'] and not row['label']:
                modules_and_forms_rows.append(row)
            elif module_or_form != row['menu_or_form']:
                module_or_form = row['menu_or_form']
                sheet_map[module_or_form] = [row]
            else:
                sheet_map[module_or_form].append(row)
        older_rows = modules_and_forms_rows if sheet_name == MODULES_AND_FORMS_SHEET_NAME\
            else sheet_map[sheet_name]
    else:
        for header in headers:
            if header[0] == sheet_name:
                headers = header[1]
                break
        older_sheet_details = get_bulk_app_sheets_by_name(app, lang, eligible_for_transifex_only=True)
        older_rows = prepare_older_rows_dict(older_sheet_details[sheet_name], headers)
    return older_rows
