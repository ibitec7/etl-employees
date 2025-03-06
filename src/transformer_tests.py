import pytest

def test_df_dept(df_dept,departments):
    df_departments = set(df_dept.dept_name.unique())
    diff = df_departments.difference(departments)
    assert len(diff) == 0
    assert df_dept.shape[0] == 9

def test_df_dept_emp(df_dept_emp, departments):
    assert len(df_dept_emp.dept_no.unique()) == 9
    assert len(df_dept_emp.emp_no.unique()) == 300024

def test_df_dept_man(df_dept_man, df_dept_emp):
    assert len(df_dept_man.dept_no.unique()) == 9
    assert set(df_dept_emp.emp_no.unique()).issuperset(set(df_dept_man.emp_no.unique())) == True

def test_df_employees(df_employees):
    assert df_employees.emp_no.duplicated().any() == False
    assert len(df_employees.emp_no.unique()) == 300024
    assert len(df_employees.gender.unique()) == 2
    assert set(df_employees.gender.unique()).difference(["M","F"]) == set()

def test_df_salaries(df_salaries):
    assert len(df_salaries.emp_no.unique()) == 300024

def test_df_titles(df_titles):
    assert len(df_titles.emp_no.unique()) == 300024

    