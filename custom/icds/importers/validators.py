from random import random
from collections import defaultdict
import itertools


def check_dashboard(dashboard):
    """This function checks the dashboard file for errors

    Arguments:
        dashboard {pyexcel.sheet} -- A pyexcel sheet of the dashboard users file

    Returns:
        list -- A list containing all the errors
    """
    colname_list = dashboard[0]
    errorlist = []
    username_index = password_index = role_index = location_code_index = None
    username = location_code = role = None
    role_dict = {
        "dpo": ["dpo"],
        "dhd": ["dhd", "dpa", "sbp"],
        "shd": ["sbp", "shd", "cta"],
        "cdpo": ["cdpo"],
        "bhd": ["bhd", "bpa"],
        "sicds": ["nod", "ncd", "bcc", "sdc", "mne", "fm", "pa", "acc"],
    }
    for index, value in enumerate(colname_list):
        if "username" in str(value).lower():
            username_index = index
        if "password" in str(value).lower():
            password_index = index
        if "role" in str(value).lower():
            role_index = index
        if "location_code" in str(value).lower():
            location_code_index = index
    dashboard.name_columns_by_row(0)

    if username_index is not None:
        username = dashboard.column[str(colname_list[username_index])]
        errorlist.extend(__check_unique(dashboard, colname_list[username_index]))
        list_of_roles = ["dpo", "dpa", "dhd", "sbp"]
        errorlist.extend(__check_no_of_rows(list_of_roles, username))
        list_of_roles = ["cdpo", "bhd", "bpa"]
        errorlist.extend(__check_no_of_rows(list_of_roles, username))
    if password_index is not None:
        errorlist.extend(__check_unique(dashboard, colname_list[password_index]))
    if role_index is not None:
        role = dashboard.column[str(colname_list[role_index])]
    if location_code_index is not None:
        location_code = dashboard.column[str(colname_list[location_code_index])]
        errorlist.extend(__check_string("num", location_code, "location_code"))

    if username and location_code:
        errorlist.extend(
            __check_numeric_part("dashboard", username, location_code,
                                 "username", "location code")
        )
    if username and role:
        errorlist.extend(__check_roles(role_dict, username, role))

    return errorlist


def check_mobile(mobile):
    """This function checks the mobile file for errors

    Arguments:
        mobile {pyexcel.sheet} -- A pyexcel sheet of the mobile users file

    Returns:
        list -- A list containing all the errors
    """
    colname_list = mobile[0]
    errorlist = []
    username_index = password_index = location_code_index = None
    username = location_code = None
    for index, value in enumerate(colname_list):
        if "username" in str(value).lower():
            username_index = index
        if "password" in str(value).lower():
            password_index = index
        if "role" in str(value).lower():
            pass
        if "location_code" in str(value).lower():
            location_code_index = index
    mobile.name_columns_by_row(0)

    if username_index is not None:
        username = mobile.column[colname_list[username_index]]
        errorlist.extend(__check_unique(mobile, colname_list[username_index]))
        errorlist.extend(__check_string("num", username, "username"))
    if password_index is not None:
        errorlist.extend(__check_unique(mobile, colname_list[password_index]))
    if location_code_index is not None:
        location_code = mobile.column[colname_list[location_code_index]]
        errorlist.extend(__check_string("num", location_code, "location_code"))

    if username and location_code:
        errorlist.extend(
            __check_numeric_part(
                "mobile", username, location_code, "username", "location code"
            )
        )

    return errorlist


def check_inventory(mobile, inventory, district, awcno):
    """This function checks the inventory file for errors

    Arguments:
        mobile {pyexcel.sheet} -- A pyexcel sheet of the mobile users file
        inventory {pyexcel.sheet} -- A pyexcel sheet of the inventory file
        district {pyexcel.sheet} -- A pyexcel sheet of the India district file
        awcno {int} -- A integer value extracted from the inventory file name

    Returns:
        list -- A list containing all the errors
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
            inventory, inventory_colname_list, inventory_username_index, awcno
        )
    if inventory_password_index is not None:
        inventory_password = __get_column_till_awc(
            inventory, inventory_colname_list, inventory_password_index, awcno
        )
    if inventory_state_code_index is not None:
        inventory_state_code = __get_column_till_awc(
            inventory, inventory_colname_list, inventory_state_code_index, awcno
        )
    if inventory_state_name_index is not None:
        inventory_state_name = __get_column_till_awc(
            inventory, inventory_colname_list, inventory_state_name_index, awcno
        )
    if inventory_district_code_index is not None:
        inventory_district_code = __get_column_till_awc(
            inventory, inventory_colname_list, inventory_district_code_index, awcno
        )
    if inventory_district_name_index is not None:
        inventory_district_name = __get_column_till_awc(
            inventory, inventory_colname_list, inventory_district_name_index, awcno
        )
    if inventory_sub_district_name_index is not None:
        inventory_sub_district_name = __get_column_till_awc(
            inventory, inventory_colname_list, inventory_sub_district_name_index, awcno
        )
    if inventory_project_code_index is not None:
        inventory_project_code = __get_column_till_awc(
            inventory, inventory_colname_list, inventory_project_code_index, awcno
        )
    if inventory_project_name_index is not None:
        inventory_project_name = __get_column_till_awc(
            inventory, inventory_colname_list, inventory_project_name_index, awcno
        )
    if inventory_sector_code_index is not None:
        inventory_sector_code = __get_column_till_awc(
            inventory, inventory_colname_list, inventory_sector_code_index, awcno
        )
    if inventory_sector_name_index is not None:
        inventory_sector_name = __get_column_till_awc(
            inventory, inventory_colname_list, inventory_sector_name_index, awcno
        )
    if inventory_awc_code_index is not None:
        inventory_awc_code = __get_column_till_awc(
            inventory, inventory_colname_list, inventory_awc_code_index, awcno
        )
    if inventory_awc_name_index is not None:
        inventory_awc_name = __get_column_till_awc(
            inventory, inventory_colname_list, inventory_awc_name_index, awcno
        )
    if inventory_lgd_code_index is not None:
        inventory_lgd_code = __get_column_till_awc(
            inventory, inventory_colname_list, inventory_lgd_code_index, awcno
        )
    if inventory_lgd_name_index is not None:
        inventory_lgd_name = __get_column_till_awc(
            inventory, inventory_colname_list, inventory_lgd_name_index, awcno
        )
    if inventory_aww_name_index is not None:
        inventory_aww_name = __get_column_till_awc(
            inventory, inventory_colname_list, inventory_aww_name_index, awcno
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
    errorlist = []

    if mobile_username and mobile_password and inventory_username and inventory_password:
        if not (
            mobile_username[rand] == inventory_username[rand]
            and mobile_password[rand] == inventory_password[rand]
        ):
            errorlist.append(
                f'''Username or password at row {str(rand + 2)} do not match in moblie and inventory file''')

    inventory_state_code_first = None
    if inventory_state_code and inventory_state_name:
        inventory_state_code_first = inventory_state_code[1]
        inventory_state_name_first = inventory_state_name[1]
        for a, b in itertools.zip_longest(inventory_state_code, inventory_state_name):
            if a != inventory_state_code_first or b != inventory_state_name_first:
                errorlist.append(
                    f'''State_code or state name found to be different please check for state code {str(a)} 
                        and state name {str(b)}''')

    if inventory_district_code and inventory_district_name:
        errorlist.extend(__check_blank(inventory_district_code, "District code"))
        errorlist.extend(__check_blank(inventory_district_name, "District name"))
        errorlist.extend(
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
            errorlist.append(
                f'''District code {str(key)} is not uniquely mapped to the district name''')

    if inventory_district_code and inventory_state_code_first:
        for index, district_code in enumerate(inventory_district_code):
            if district_code[:2] != inventory_state_code_first:
                errorlist.append(
                    f'''District code does not match with state code at row {str(index)}''')

    if inventory_project_code:
        errorlist.extend(__check_no_of_digits(
            7, inventory_project_code, "project code"))

    if inventory_project_code and inventory_project_name and inventory_district_name:
        for pcode, pname, dname in itertools.zip_longest(
            inventory_project_code, inventory_project_name, inventory_district_name
        ):
            if pname not in inventory_project_code_dict[pcode + "." + dname]:
                inventory_project_code_dict[pcode + "." + dname].append(pname)
    for key, value in inventory_project_code_dict.items():
        if len(value) > 1:
            errorlist.append(
                f'''Project code {str(key)} is not uniquely mapped to the project name''')

    if inventory_project_code and inventory_district_code:
        errorlist.extend(
            __check_starting_digits(
                5,
                inventory_project_code,
                "Project Code",
                inventory_district_code,
                "District code",
            )
        )
    if inventory_project_code:
        errorlist.extend(__check_blank(inventory_project_code, "Project code"))
    if inventory_project_name:
        errorlist.extend(__check_blank(inventory_project_name, "Project name"))
    if inventory_sector_code:
        errorlist.extend(__check_no_of_digits(
            9, inventory_sector_code, "sector code"))

    if inventory_sector_code and inventory_sector_name and inventory_district_name:
        for scode, sname, dname in itertools.zip_longest(
            inventory_sector_code, inventory_sector_name, inventory_district_name
        ):
            if sname not in inventory_sector_code_dict[scode + "." + dname]:
                inventory_sector_code_dict[scode + "." + dname].append(sname)
    for key, value in inventory_sector_code_dict.items():
        if len(value) > 1:
            errorlist.append(
                f'''Sector code {str(key)} is not uniquely mapped to the sector name''')

    if inventory_sector_code and inventory_project_code:
        errorlist.extend(
            __check_starting_digits(
                7,
                inventory_sector_code,
                "Sector Code",
                inventory_project_code,
                "Project code",
            )
        )
    if inventory_sector_code:
        errorlist.extend(__check_blank(inventory_sector_code, "Sector code"))
    if inventory_sector_name:
        errorlist.extend(__check_blank(inventory_sector_name, "Sector name"))
    if inventory_awc_code:
        errorlist.extend(__check_no_of_digits(11, inventory_awc_code, "awc code"))
        errorlist.extend(__check_blank(inventory_awc_code, "AWC code"))
    if inventory_awc_code_index is not None:
        errorlist.extend(
            __check_unique(
                inventory, inventory_colname_list[inventory_awc_code_index], awcno=awcno
            )
        )
    if inventory_awc_name:
        errorlist.extend(__check_blank(inventory_awc_name, "AWC name"))
    if inventory_lgd_code:
        errorlist.extend(__check_blank(inventory_lgd_code, "LGD code"))
        errorlist.extend(__check_no_of_digits(6, inventory_lgd_code, "lgd code"))
    if inventory_lgd_name:
        errorlist.extend(__check_blank(inventory_lgd_name, "LGD name"))
    if inventory_aww_name:
        errorlist.extend(__check_blank(inventory_aww_name, "AWW name"))
    if inventory_district_name and district_district_name:
        errorlist.extend(
            __find_dist_subdist("District", inventory_district_name,
                                district_district_name)
        )
    if inventory_sub_district_name and district_sub_district_name:
        errorlist.extend(
            __find_dist_subdist(
                "Sub-District", inventory_sub_district_name, district_sub_district_name
            )
        )
    return errorlist


def __check_unique(sheet, field, awcno=None):
    """This function checks for duplicates in a given column and returns a list with all the errors

    Arguments:
        sheet {pyexcel.sheet} -- A pyexcel worksheet is passed to the function.
        field {string} -- A string specifying the column name in which duplicate check is to be done.

    Keyword Arguments:
        awcno {int} -- The no of awc rows only to used for inventory file (default: {None})

    Returns:
        {list} -- A list containing all the errors i.e the duplicates
    """
    try:
        column = sheet.column[field][:int(awcno)]
    except:
        column = sheet.column[field]
    errorlist = []
    seen = []
    if len(column) == len(set(column)):
        return errorlist
    else:
        tally = defaultdict(list)
        for i, item in enumerate(column):
            tally[item].append(i)
        for key, value in tally.items():
            if len(value) > 1:
                for row in value:
                    row += 2
                    seen.append(row)
                errorlist.append(
                    f'''Possible duplicates of value {str(key)} found in column {str(field)} 
                        at rows {str(seen).strip("[]")}''')
                seen = []

        return errorlist


def __check_blank(rows, col_name):
    """This function checks for blank rows in the list provided and returns a list containing the errors

    Arguments:
        rows {list} -- A list containing all the rows from a particular column
        col_name {String} -- A String containing the name of the column for errorlist

    Returns:
        {list} -- A list containing all the errors i.e the blanks
    """
    errorlist = []
    for index, value in enumerate(rows):
        if value == "":
            errorlist.append(
                f'''{col_name} has blank value at row {str(index)}''')
    return errorlist


def __check_string(mode, column, column_name):
    """Function to check if a column contains only alphabets of numeric values

    Args:
        mode {string} -- "alpha" or "num" are the two values
        column {list} -- "column to check for values"
        column_name {string} -- "column name to be added to the errorlist while adding error"
    Returns:
        {list} -- a list containing all the errors

    """
    errorlist = []
    check_flags = []
    error_keyword = ''
    if mode == "num":
        check_flags = [str(value).isnumeric() for value in column]
        error_keyword = 'non-numeric characters'
    elif mode == "alpha":
        check_flags = [str(value).isalpha() for value in column]
        error_keyword = 'non-alphabet characters'

    for index, check_flag in enumerate(check_flags):
        if not check_flag:
            errorlist.append(
                f'''{column_name} contains {error_keyword} at row {str(index + 2)} i.e {column[index]}''')

    return errorlist


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
        return [f'''{str(list_of_roles).strip("[]")} are not equal''']
    else:
        return []


def __check_numeric_part(mode, column1, column2, column1_name, column2_name):
    """Function to compare the numeric part of first column value to 2nd column value

    Args:
        mode {string} -- "dashboard" for dashboard and "mobile" for mobile
        column1 {list} -- In case of dashboard the username column and in case of mobile any column
        column2 {list} -- Any other column to be compared with for eg location_code
        column1_name {string} -- column1 name to be added to the errorlist
        column2_name {string} -- column2 name to be added to the errorlist

    Returns:
        {list} -- a list containing all the errors
    """
    errorlist = []
    if len(column1) != len(column2):
        return [f'''Number of entries in {column1_name} doesn't match with number of entries in {column2_name}''']
    if mode == "dashboard":
        for index, value in enumerate(column1):
            if value.split(".")[0] != column2[index]:
                errorlist.append(
                    f'''Numeric part of {column1_name} doesn't match with {column2_name} at row {str(index + 2)}''')
    elif mode == "mobile":
        for index, value in enumerate(column1):
            if value != column2[index]:
                errorlist.append(
                    f'''Numeric part of {column1_name} doesn't match with {column2_name} at row {str(index + 2)}''')
    return errorlist


def __check_roles(role_dict, username_column_list, role_column_list):
    """Function that checks if proper roles are defined to each username

    Args:
        role_dict {dictionary} -- A dictionary containing all the roles and their auth levels
        username_column_list {list} -- The username column converted to a list
        role_column_list {list} -- The role column converted to a list

    Returns:
        {list} -- a list containing all the errors
    """
    errorlist = []
    if len(username_column_list) != len(role_column_list):
        return [f'''Number of username entries doesn't match with number of role entries''']
    for index, username in enumerate(username_column_list):
        try:
            if username.split(".")[1].lower() not in role_dict[role_column_list[index].lower()]:
                errorlist.append(
                    f'''{role_column_list[index]} role not set right at row {str(index + 2)}''')
        except Exception as e:
            errorlist.append(
                f'''{role_column_list[index]} role not set right at row {str(index + 2)}''')
    return errorlist


def __get_column_till_awc(inventory, inventory_colname_list, column_index, awcno):
    """A function that returns a list of values from the data in a particular column from the inventory excel sheet,
    from first row till nth row (n = awcno).

    Args:
        inventory {pyexcel.sheet.Sheet} -- The inventory file sheet uploaded
        inventory_colname_list {iterable list} -- The list of column names from inventory sheet
        column_index {int} -- The index in colname list corresponding to the required column
        awcno {str} -- The no of rows to be extracted, in string form

    Returns:
        {list} -- a list containing all the errors
    """
    awcno = int(awcno)
    return inventory.column[inventory_colname_list[column_index]][:awcno]


def __check_no_of_digits(no_of_digits, column, column_name):
    """Function to check if all numeric entries in a particular column
    are composed of a given fixed number of digits

    Args:
        no_of_digits {int} -- The given fixed no of digits all numeric entries should be composed of
        column {list} -- The list of column values for which the numeric entries are to be checked
        column_name {string} -- The column name string to be added to the errorlist

    Returns:
        {list} -- a list containing all the errors
    """
    errorlist = []
    for index, value in enumerate(column):
        if len(str(value)) != no_of_digits and str(value).isnumeric():
            errorlist.append(
                f'''{column_name} is not {str(no_of_digits)} digits long at row {str(index + 2)}''')
    return errorlist


def __check_starting_digits(no_of_digits, column1, column1_str, column2, column2_str):
    """A function that checks the starting digits of column1 with the value in column2.
        The no of starting digits is defined using the no_of_digits parameter.

    Args:
        no_of_digits {int} -- The no of starting digits that are to be compared
        column1 {list} -- First column to check the starting digits of
        column1_str {string} -- Column1 name to add to the errorlist
        column2 {list} -- Second column to compare the extracted digits with
        column2_str {string} -- Column2 name to add to the errorlist

    Returns:
        {list} -- A list containing all the errors
    """
    errorlist = []
    for element1, element2 in itertools.zip_longest(column1, column2):
        if element1[:no_of_digits] != element2:
            errorlist.append(
                f'''The starting digits of {column1_str} {str(element1)} 
                    do not match with {column2_str} {str(element2)}''')
    return errorlist


def __find_dist_subdist(colname, inventory_dist_subdist, district_dist_subdist):
    """Function to check if the district and sub district are correctly mapped in the district and inventory file

    Args:
        colname {string} -- The column name string that needs to be added to the errorlist
        inventory_dist_subdist {list} -- The district or sub-district list from the inventory file
        district_dist_subdist {list} -- The district or sub-district list from the district file

    Returns:
        {list} -- A list containing all the errors
    """
    errorlist = []
    for dist_subdist in inventory_dist_subdist:
        if dist_subdist not in district_dist_subdist:
            errorlist.append(
                f'''{colname} named {str(dist_subdist)} not found in India district file''')
    return errorlist
