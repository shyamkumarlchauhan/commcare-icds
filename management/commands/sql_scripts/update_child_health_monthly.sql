CREATE OR REPLACE FUNCTION update_child_health_monthly(date) RETURNS VOID AS
$BODY$
DECLARE
  _tablename text;
  _ucr_child_monthly_table text;
  _start_date date;
  _ucr_child_health_cases_table text;
BEGIN
  _start_date = date_trunc('MONTH', $1)::DATE;
  _tablename := 'child_health_monthly' || '_' || _start_date;
  EXECUTE 'SELECT table_name FROM ucr_table_name_mapping WHERE table_type = ' || quote_literal('child_health_monthly') INTO _ucr_child_monthly_table;
  EXECUTE 'SELECT table_name FROM ucr_table_name_mapping WHERE table_type = ' || quote_literal('child_list') INTO _ucr_child_health_cases_table;

  EXECUTE 'UPDATE ' || quote_ident(_tablename) || ' chm_monthly SET ' ||
      'dob = child_health_monthly.dob, ' ||
      'sex = child_health_monthly.sex  ' ||
  'FROM ' || quote_ident(_ucr_child_monthly_table) || ' child_health_monthly ' ||
  'WHERE chm_monthly.case_id = child_health_monthly.case_id AND chm_monthly.month = ' || quote_literal(_start_date);

  EXECUTE 'UPDATE ' || quote_ident(_tablename) || ' chm_monthly SET ' ||
      'person_name = ucr_case.person_name, ' ||
      'mother_name = ucr_case.mother_name  ' ||
  'FROM ' || quote_ident(_ucr_child_health_cases_table) || ' ucr_case ' ||
  'WHERE chm_monthly.case_id = ucr_case.case_id AND chm_monthly.month = ' || quote_literal(_start_date);
END;
$BODY$
LANGUAGE plpgsql;
