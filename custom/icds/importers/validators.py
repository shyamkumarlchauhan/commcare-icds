import itertools
from collections import defaultdict
from random import random

from .const import (
    DASHBOARD,
    LOCATION_CODE_COLUMN,
    MOBILE,
    PASSWORD_COLUMN,
    ROLE_COLUMN,
    ROLES,
    USERNAME_COLUMN,
)


def validate_dashboard_users_upload(worksheet):
    """Validates dashboard users upload. Checks for
    1. unique usernames, passwords
    2. expected number of rows for different roles
    3. numeric location codes
    4. ensure username contain the corresponding location code

    :returns: list of error messages
    """
    column_names = worksheet[0]
    errors = []
    usernames = location_codes = roles = None

    username_col_name = _get_column_name(column_names, USERNAME_COLUMN)
    password_col_name = _get_column_name(column_names, PASSWORD_COLUMN)
    role_col_name = _get_column_name(column_names, ROLE_COLUMN)
    location_code_col_name = _get_column_name(column_names, LOCATION_CODE_COLUMN)

    # Use the elements of a specified row to represent individual columns
    worksheet.name_columns_by_row(0)

    if username_col_name:
        usernames = worksheet.column[username_col_name]
        errors.extend(__check_unique(worksheet, username_col_name))
        list_of_roles = ["dpo", "dpa", "dhd", "sbp"]
        errors.extend(__check_no_of_rows(list_of_roles, usernames))
        list_of_roles = ["cdpo", "bhd", "bpa"]
        errors.extend(__check_no_of_rows(list_of_roles, usernames))
    if password_col_name:
        errors.extend(__check_unique(worksheet, password_col_name))
    if role_col_name:
        roles = worksheet.column[role_col_name]
    if location_code_col_name:
        location_codes = worksheet.column[location_code_col_name]
        errors.extend(__check_numeric(worksheet, location_code_col_name))

    if usernames and location_codes:
        errors.extend(
            __check_numeric_part(DASHBOARD, usernames, location_codes,
                                 "username", "location code")
        )
    if usernames and roles:
        errors.extend(__check_roles(usernames, roles))

    return errors


def validate_mobile_users_upload(worksheet):
    """Validates mobile users upload. Checks for
    1. unique usernames, passwords
    2. numeric usernames and location codes
    3. ensure username contain the corresponding location code

    :returns: list of error messages
    """
    column_names = worksheet[0]
    errors = []

    username_col_name = _get_column_name(column_names, USERNAME_COLUMN)
    password_col_name = _get_column_name(column_names, PASSWORD_COLUMN)
    location_code_col_name = _get_column_name(column_names, LOCATION_CODE_COLUMN)

    # Use the elements of a specified row to represent individual columns
    worksheet.name_columns_by_row(0)

    if username_col_name:
        errors.extend(__check_unique(worksheet, username_col_name))
        errors.extend(__check_numeric(worksheet, username_col_name))
    if password_col_name:
        errors.extend(__check_unique(worksheet, password_col_name))
    if location_code_col_name:
        errors.extend(__check_numeric(worksheet, location_code_col_name))

    if username_col_name and location_code_col_name:
        usernames = worksheet.column[username_col_name]
        location_codes = worksheet.column[location_code_col_name]
        if usernames and location_codes:
            errors.extend(
                __check_numeric_part(
                    MOBILE, usernames, location_codes, username_col_name, location_code_col_name
                )
            )

    return errors


def validate_inventory_upload(mobile, inventory, district, number_of_awcs):
    """This function checks the inventory file for errors

    :param mobile: mobile users file
    :param inventory: inventory file
    :param district: India district file
    :param number_of_awcs: integer value extracted from the inventory file name
    :returns: list of error messages
    """
    rand = int(random() * 100)
    mobile_colname_list = mobile[0]
    mobile_username_index = mobile_password_index = None
    mobile_username = mobile_password = None
    for index, value in enumerate(mobile_colname_list):
        if "username" in str(value).lower():
            mobile_username_index = index
        if "password" in str(value).lower():
            mobile_password_index = index
        if "role" in str(value).lower():
            mobile_role_index = index
        if "location" and "code" in str(value).lower():
            mobile_location_code_index = index
    mobile.name_columns_by_row(0)

    if mobile_username_index is not None:
        mobile_username = mobile.column[mobile_colname_list[mobile_username_index]]
    if mobile_password_index is not None:
        mobile_password = mobile.column[mobile_colname_list[mobile_password_index]]

    inventory_colname_list = inventory[0]
    inventory_username_index = inventory_password_index = None
    inventory_state_code_index = inventory_state_name_index = None
    inventory_district_code_index = inventory_district_name_index = inventory_sub_district_name_index = None
    inventory_project_code_index = inventory_project_name_index = None
    inventory_sector_code_index = inventory_sector_name_index = None
    inventory_awc_name_index = inventory_awc_code_index = None
    inventory_aww_name_index = inventory_lgd_name_index = inventory_lgd_code_index = None
    inventory_username = inventory_password = None
    inventory_state_code = inventory_state_name = None
    inventory_district_code = inventory_district_name = None
    inventory_project_code = inventory_project_name = None
    inventory_sector_code = inventory_sector_name = None
    inventory_awc_code = inventory_awc_name = None
    inventory_lgd_code = inventory_lgd_name = None
    inventory_aww_name = inventory_sub_district_name = None
    for index, value in enumerate(inventory_colname_list):
        value = str(value).lower()
        if "username" in value:
            inventory_username_index = index
        if "password" in value:
            inventory_password_index = index
        if "sl" in value:
            inventory_sl_index = index
        if "state" in value and "code" in value:
            inventory_state_code_index = index
        if "state" in value and "name" in value:
            inventory_state_name_index = index
        if "district" in value and "code" in value:
            inventory_district_code_index = index
        if (
            "district" in value
            and "name" in value
            and "sub" not in value
        ):
            inventory_district_name_index = index
        if (
            "sub" in value
            and "district" in value
            and "name" in value
        ):
            inventory_sub_district_name_index = index
        if "project" in value and "code" in value:
            inventory_project_code_index = index
        if "project" in value and "name" in value:
            inventory_project_name_index = index
        if "sector" in value and "code" in value:
            inventory_sector_code_index = index
        if "sector" in value and "name" in value:
            inventory_sector_name_index = index
        if "awc" in value and "name" in value:
            inventory_awc_name_index = index
        if "awc" in value and "code" in value:
            inventory_awc_code_index = index
        if "awc" in value and "type" in value:
            inventory_awc_type_index = index
        if (
            "aww" in value
            and "name" in value
            and "helper" not in value
        ):
            inventory_aww_name_index = index
        if "city" in value and "name" in value:
            inventory_lgd_name_index = index
        if "lgd" in value and "code" in value:
            inventory_lgd_code_index = index

    inventory.name_columns_by_row(0)
    if inventory_username_index is not None:
        inventory_username = __get_column_till_awc(
            inventory, inventory_colname_list, inventory_username_index, number_of_awcs
        )
    if inventory_password_index is not None:
        inventory_password = __get_column_till_awc(
            inventory, inventory_colname_list, inventory_password_index, number_of_awcs
        )
    if inventory_state_code_index is not None:
        inventory_state_code = __get_column_till_awc(
            inventory, inventory_colname_list, inventory_state_code_index, number_of_awcs
        )
    if inventory_state_name_index is not None:
        inventory_state_name = __get_column_till_awc(
            inventory, inventory_colname_list, inventory_state_name_index, number_of_awcs
        )
    if inventory_district_code_index is not None:
        inventory_district_code = __get_column_till_awc(
            inventory, inventory_colname_list, inventory_district_code_index, number_of_awcs
        )
    if inventory_district_name_index is not None:
        inventory_district_name = __get_column_till_awc(
            inventory, inventory_colname_list, inventory_district_name_index, number_of_awcs
        )
    if inventory_sub_district_name_index is not None:
        inventory_sub_district_name = __get_column_till_awc(
            inventory, inventory_colname_list, inventory_sub_district_name_index, number_of_awcs
        )
    if inventory_project_code_index is not None:
        inventory_project_code = __get_column_till_awc(
            inventory, inventory_colname_list, inventory_project_code_index, number_of_awcs
        )
    if inventory_project_name_index is not None:
        inventory_project_name = __get_column_till_awc(
            inventory, inventory_colname_list, inventory_project_name_index, number_of_awcs
        )
    if inventory_sector_code_index is not None:
        inventory_sector_code = __get_column_till_awc(
            inventory, inventory_colname_list, inventory_sector_code_index, number_of_awcs
        )
    if inventory_sector_name_index is not None:
        inventory_sector_name = __get_column_till_awc(
            inventory, inventory_colname_list, inventory_sector_name_index, number_of_awcs
        )
    if inventory_awc_code_index is not None:
        inventory_awc_code = __get_column_till_awc(
            inventory, inventory_colname_list, inventory_awc_code_index, number_of_awcs
        )
    if inventory_awc_name_index is not None:
        inventory_awc_name = __get_column_till_awc(
            inventory, inventory_colname_list, inventory_awc_name_index, number_of_awcs
        )
    if inventory_lgd_code_index is not None:
        inventory_lgd_code = __get_column_till_awc(
            inventory, inventory_colname_list, inventory_lgd_code_index, number_of_awcs
        )
    if inventory_lgd_name_index is not None:
        inventory_lgd_name = __get_column_till_awc(
            inventory, inventory_colname_list, inventory_lgd_name_index, number_of_awcs
        )
    if inventory_aww_name_index is not None:
        inventory_aww_name = __get_column_till_awc(
            inventory, inventory_colname_list, inventory_aww_name_index, number_of_awcs
        )

    district_colname_list = district[0]
    district_district_name_index = district_sub_district_name_index = None
    district_district_name = district_sub_district_name = None
    for index, value in enumerate(district_colname_list):
        if "state" and "code" in str(value).lower():
            district_state_code_index = index
        if "state" and "name" in str(value).lower():
            district_state_name_index = index
        if "district" and "name" in str(value).lower():
            district_district_name_index = index
        if "sub" and "district" and "name" in str(value).lower():
            district_sub_district_name_index = index

    district.name_columns_by_row(0)
    if district_district_name_index is not None:
        district_district_name = district.column[
            district_colname_list[district_district_name_index]
        ]
    if district_sub_district_name_index is not None:
        district_sub_district_name = district.column[
            district_colname_list[district_sub_district_name_index]
        ]

    inventory_district_code_dict = defaultdict(list)
    inventory_project_code_dict = defaultdict(list)
    inventory_sector_code_dict = defaultdict(list)
    errors = []

    if mobile_username and mobile_password and inventory_username and inventory_password:
        if not (
            mobile_username[rand] == inventory_username[rand]
            and mobile_password[rand] == inventory_password[rand]
        ):
            errors.append(
                f"Username or password at row {str(rand + 2)} do not match in mobile and inventory file")

    inventory_state_code_first = None
    if inventory_state_code and inventory_state_name:
        inventory_state_code_first = inventory_state_code[1]
        inventory_state_name_first = inventory_state_name[1]
        for a, b in itertools.zip_longest(inventory_state_code, inventory_state_name):
            if a != inventory_state_code_first or b != inventory_state_name_first:
                errors.append(
                    f"State code or state name found to be different please check for state code {a} "
                    f"and state name {b}")

    if inventory_district_code and inventory_district_name:
        errors.extend(__check_blank(inventory_district_code, "District code"))
        errors.extend(__check_blank(inventory_district_name, "District name"))
        errors.extend(
            __check_no_of_digits(5, inventory_district_code,
                                 "inventory_district_code")
        )

        for code, name in itertools.zip_longest(
            inventory_district_code, inventory_district_name
        ):
            if name not in inventory_district_code_dict[code]:
                inventory_district_code_dict[code].append(name)
    print('district_map:')
    print(inventory_district_code_dict)
    for key, value in inventory_district_code_dict.items():
        if len(value) > 1:
            errors.append(
                f"District code {str(key)} is not uniquely mapped to the district name")

    if inventory_district_code and inventory_state_code_first:
        for index, district_code in enumerate(inventory_district_code):
            if district_code[:2] != inventory_state_code_first:
                errors.append(
                    f"District code does not match with state code at row {str(index)}")

    if inventory_project_code:
        errors.extend(__check_no_of_digits(
            7, inventory_project_code, "project code"))

    if inventory_project_code and inventory_project_name and inventory_district_name:
        for pcode, pname, dname in itertools.zip_longest(
            inventory_project_code, inventory_project_name, inventory_district_name
        ):
            if pname not in inventory_project_code_dict[pcode + "." + dname]:
                inventory_project_code_dict[pcode + "." + dname].append(pname)
    for key, value in inventory_project_code_dict.items():
        if len(value) > 1:
            errors.append(
                f"Project code {str(key)} is not uniquely mapped to the project name")

    if inventory_project_code and inventory_district_code:
        errors.extend(
            __check_starting_digits(
                5,
                inventory_project_code,
                "Project Code",
                inventory_district_code,
                "District code",
            )
        )
    if inventory_project_code:
        errors.extend(__check_blank(inventory_project_code, "Project code"))
    if inventory_project_name:
        errors.extend(__check_blank(inventory_project_name, "Project name"))
    if inventory_sector_code:
        errors.extend(__check_no_of_digits(
            9, inventory_sector_code, "sector code"))

    if inventory_sector_code and inventory_sector_name and inventory_district_name:
        for scode, sname, dname in itertools.zip_longest(
            inventory_sector_code, inventory_sector_name, inventory_district_name
        ):
            if sname not in inventory_sector_code_dict[scode + "." + dname]:
                inventory_sector_code_dict[scode + "." + dname].append(sname)
    for key, value in inventory_sector_code_dict.items():
        if len(value) > 1:
            errors.append(
                f"Sector code {str(key)} is not uniquely mapped to the sector name")

    if inventory_sector_code and inventory_project_code:
        errors.extend(
            __check_starting_digits(
                7,
                inventory_sector_code,
                "Sector Code",
                inventory_project_code,
                "Project code",
            )
        )
    if inventory_sector_code:
        errors.extend(__check_blank(inventory_sector_code, "Sector code"))
    if inventory_sector_name:
        errors.extend(__check_blank(inventory_sector_name, "Sector name"))
    if inventory_awc_code:
        errors.extend(__check_no_of_digits(11, inventory_awc_code, "awc code"))
        errors.extend(__check_blank(inventory_awc_code, "AWC code"))
    if inventory_awc_code_index is not None:
        errors.extend(
            __check_unique(
                inventory, inventory_colname_list[inventory_awc_code_index], number_of_awcs
            )
        )
    if inventory_awc_name:
        errors.extend(__check_blank(inventory_awc_name, "AWC name"))
    if inventory_lgd_code:
        errors.extend(__check_blank(inventory_lgd_code, "LGD code"))
        errors.extend(__check_no_of_digits(6, inventory_lgd_code, "lgd code"))
    if inventory_lgd_name:
        errors.extend(__check_blank(inventory_lgd_name, "LGD name"))
    if inventory_aww_name:
        errors.extend(__check_blank(inventory_aww_name, "AWW name"))
    if inventory_district_name and district_district_name:
        errors.extend(
            __find_dist_subdist("District", inventory_district_name,
                                district_district_name)
        )
    if inventory_sub_district_name and district_sub_district_name:
        errors.extend(
            __find_dist_subdist(
                "Sub-District", inventory_sub_district_name, district_sub_district_name
            )
        )
    return errors


def __check_unique(sheet, column_name, number_of_awcs=None):
    """Checks for duplicates

    :param number_of_awcs: Only to used for inventory file
    :returns: list of error messages
    """
    try:
        column = sheet.column[column_name][:int(number_of_awcs)]
    except:
        column = sheet.column[column_name]
    if len(column) == len(set(column)):
        return []

    errors = []
    tally = defaultdict(list)
    for i, item in enumerate(column):
        tally[item].append(i)
    for key, value in tally.items():
        if len(value) > 1:
            occurrences = [row + 2 for row in value]
            errors.append(
                f"Possible duplicates of value {str(key)} found in column {column_name}"
                f"at rows {','.join(occurrences)}")
    return errors


def __check_blank(rows, col_name):
    """This function checks for blank rows in the list provided and returns a list containing the errors

    Arguments:
        rows {list} -- A list containing all the rows from a particular column
        col_name {String} -- A String containing the name of the column for errors

    :returns: list of error messages
    """
    errors = []
    for index, value in enumerate(rows):
        if value == "":
            errors.append(
                f"{col_name} has blank value at row {str(index)}")
    return errors


def __check_numeric(worksheet, column_name):
    errors = []
    for index, value in enumerate(worksheet.column[column_name]):
        if not str(value).isnumeric():
            errors.append(
                f"{column_name} contains non-numeric characters at row {str(index + 2)} i.e {value}")
    return errors


def __check_no_of_rows(list_of_roles, column):
    """Function to check whether the no of rows for any role within the set of roles received
    is equal to each other or not

    Args:
        list_of_roles {list} --  A list containing all the roles
        column {list} -- Roles column list for checking

    Returns:
        {list} -- a list containing all the errors
    """
    value_dict = defaultdict(lambda: 0)
    for index, value in enumerate(column):
        current_role = value.split(".")[1].lower()
        if current_role in list_of_roles:
            value_dict[current_role] += 1
    if len(set(value_dict.values())) > 1:
        return [f"{','.join(list_of_roles)} are not equal"]
    else:
        return []


def __check_numeric_part(mode, column1, column2, column1_name, column2_name):
    """Function to compare the numeric part of first column value to 2nd column value

    Args:
        mode {string} -- "dashboard" for dashboard and "mobile" for mobile
        column1 {list} -- In case of dashboard the username column and in case of mobile any column
        column2 {list} -- Any other column to be compared with for eg location_code
        column1_name {string} -- column1 name to be added to the errors
        column2_name {string} -- column2 name to be added to the errors

    Returns:
        {list} -- a list containing all the errors
    """
    errors = []
    if len(column1) != len(column2):
        return [f"Number of entries in {column1_name} doesn't match with number of entries in {column2_name}"]
    if mode == DASHBOARD:
        for index, value in enumerate(column1):
            if value.split(".")[0] != column2[index]:
                errors.append(
                    f"Numeric part of {column1_name} doesn't match with {column2_name} at row {str(index + 2)}")
    elif mode == MOBILE:
        for index, value in enumerate(column1):
            if value != column2[index]:
                errors.append(
                    f"Numeric part of {column1_name} doesn't match with {column2_name} at row {str(index + 2)}")
    return errors


def __check_roles(username_column_list, role_column_list):
    """Function that checks if proper roles are defined to each username

    Args:
        username_column_list {list} -- The username column converted to a list
        role_column_list {list} -- The role column converted to a list

    Returns:
        {list} -- a list containing all the errors
    """
    errors = []
    if len(username_column_list) != len(role_column_list):
        return [f"Number of username entries doesn't match with number of role entries"]
    for index, username in enumerate(username_column_list):
        try:
            if username.split(".")[1].lower() not in ROLES[role_column_list[index].lower()]:
                errors.append(
                    f"{role_column_list[index]} role not set right at row {str(index + 2)}")
        except Exception as e:
            errors.append(
                f"{role_column_list[index]} role not set right at row {str(index + 2)}")
    return errors


def __get_column_till_awc(inventory, inventory_colname_list, column_index, number_of_awcs):
    """A function that returns a list of values from the data in a particular column from the inventory excel sheet,
    from first row till nth row (n = number_of_awcs).

    Args:
        inventory {pyexcel.sheet.Sheet} -- The inventory file sheet uploaded
        inventory_colname_list {iterable list} -- The list of column names from inventory sheet
        column_index {int} -- The index in colname list corresponding to the required column
        number_of_awcs {str} -- The no of rows to be extracted, in string form

    Returns:
        {list} -- a list containing all the errors
    """
    number_of_awcs = int(number_of_awcs)
    return inventory.column[inventory_colname_list[column_index]][:number_of_awcs]


def __check_no_of_digits(no_of_digits, column, column_name):
    """Function to check if all numeric entries in a particular column
    are composed of a given fixed number of digits

    Args:
        no_of_digits {int} -- The given fixed no of digits all numeric entries should be composed of
        column {list} -- The list of column values for which the numeric entries are to be checked
        column_name {string} -- The column name string to be added to the errors

    Returns:
        {list} -- a list containing all the errors
    """
    errors = []
    for index, value in enumerate(column):
        if len(str(value)) != no_of_digits and str(value).isnumeric():
            errors.append(
                f"{column_name} is not {str(no_of_digits)} digits long at row {str(index + 2)}")
    return errors


def __check_starting_digits(no_of_digits, column1, column1_str, column2, column2_str):
    """A function that checks the starting digits of column1 with the value in column2.
        The no of starting digits is defined using the no_of_digits parameter.

    Args:
        no_of_digits {int} -- The no of starting digits that are to be compared
        column1 {list} -- First column to check the starting digits of
        column1_str {string} -- Column1 name to add to the errors
        column2 {list} -- Second column to compare the extracted digits with
        column2_str {string} -- Column2 name to add to the errors

    Returns:
        {list} -- A list containing all the errors
    """
    errors = []
    for element1, element2 in itertools.zip_longest(column1, column2):
        if element1[:no_of_digits] != element2:
            errors.append(
                f"The starting digits of {column1_str} {str(element1)} "
                f"do not match with {column2_str} {str(element2)}")
    return errors


def __find_dist_subdist(colname, inventory_dist_subdist, district_dist_subdist):
    """Function to check if the district and sub district are correctly mapped in the district and inventory file

    Args:
        colname {string} -- The column name string that needs to be added to the errors
        inventory_dist_subdist {list} -- The district or sub-district list from the inventory file
        district_dist_subdist {list} -- The district or sub-district list from the district file

    Returns:
        {list} -- A list containing all the errors
    """
    errors = []
    for dist_subdist in inventory_dist_subdist:
        if dist_subdist not in district_dist_subdist:
            errors.append(
                f"{colname} named {str(dist_subdist)} not found in India district file")
    return errors


def _get_column_name(column_names, column_name):
    for index, value in enumerate(column_names):
        col_name = str(value)
        if column_name in col_name.lower():
            return index
