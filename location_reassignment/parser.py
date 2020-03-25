from collections import defaultdict

from corehq.util.workbook_json.excel import get_workbook
from custom.icds.location_reassignment.const import (
    EXTRACT_OPERATION,
    MERGE_OPERATION,
    MOVE_OPERATION,
    NEW_SITE_CODE_COLUMN,
    OLD_SITE_CODE_COLUMN,
    OPERATION_COLUMN,
    SPLIT_OPERATION,
    VALID_OPERATIONS,
)


class Parser(object):
    def __init__(self, workbook):
        """
        Receives a worksheet generated by custom.icds.location_reassignment.download.Download
        and generates an output like
        {
            location_type:
                'Merge': {
                    'New location site code': ['Old location site code', 'Old location site code']
                },
                'Split': {
                    'Old location site code': ['New location site code', 'New location site code']
                },
                'Rename': {
                    'New location site code': 'Old location site code'
                },
                'Extract': {
                    'New location site code': 'Old location site code'
                }
        }
        """
        self.workbook = workbook
        # mapping each location code to the type of operation requested for it
        self.requested_transitions = {}
        location_types = [ws.title for ws in workbook.worksheets]
        self.valid_transitions = {location_type: {
            MERGE_OPERATION: defaultdict(list),
            SPLIT_OPERATION: defaultdict(list),
            MOVE_OPERATION: {},
            EXTRACT_OPERATION: {},
        } for location_type in location_types}
        self.errors = []

    def parse(self):
        for worksheet in self.workbook.worksheets:
            location_type = worksheet.title
            for row in worksheet:
                operation = row.get(OPERATION_COLUMN)
                if not operation:
                    continue
                if operation not in VALID_OPERATIONS:
                    self.errors.append("Invalid Operation %s" % operation)
                    continue
                self._parse_row(row, location_type)
        return self.valid_transitions, self.errors

    def _parse_row(self, row, location_type):
        operation = row.get(OPERATION_COLUMN)
        old_site_code = row.get(OLD_SITE_CODE_COLUMN)
        new_site_code = row.get(NEW_SITE_CODE_COLUMN)
        if not old_site_code or not new_site_code:
            self.errors.append("Missing location code for %s, got old: '%s' and new: '%s'" % (
                operation, old_site_code, new_site_code
            ))
            return
        if old_site_code == new_site_code:
            self.errors.append("No change in location code for %s, got old: '%s' and new: '%s'" % (
                operation, old_site_code, new_site_code
            ))
            return
        if self._invalid_row(operation, old_site_code, new_site_code):
            return
        self._note_transition(operation, location_type, new_site_code, old_site_code)

    def _invalid_row(self, operation, old_site_code, new_site_code):
        invalid = False
        if old_site_code in self.requested_transitions:
            if self.requested_transitions.get(old_site_code) != operation:
                self.errors.append("Multiple transitions for %s, %s and %s" % (
                    old_site_code, self.requested_transitions.get(old_site_code), operation))
                invalid = True
        if new_site_code in self.requested_transitions:
            if self.requested_transitions.get(new_site_code) != operation:
                self.errors.append("Multiple transitions for %s, %s and %s" % (
                    new_site_code, self.requested_transitions.get(new_site_code), operation))
                invalid = True
        return invalid

    def _note_transition(self, operation, location_type, new_site_code, old_site_code):
        if operation == MERGE_OPERATION:
            self.valid_transitions[location_type][operation][new_site_code].append(old_site_code)
        elif operation == SPLIT_OPERATION:
            self.valid_transitions[location_type][operation][old_site_code].append(new_site_code)
        elif operation == MOVE_OPERATION:
            self.valid_transitions[location_type][operation][new_site_code] = old_site_code
        elif operation == EXTRACT_OPERATION:
            self.valid_transitions[location_type][operation][new_site_code] = old_site_code
        self.requested_transitions[old_site_code] = operation
        self.requested_transitions[new_site_code] = operation