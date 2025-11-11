import pytest
import re


def test_file_not_empty(df):
    assert df.shape[0] > 0, "Data Entity is empty!" 


@pytest.mark.validate_csv
@pytest.mark.xfail
def test_duplicates(df):
    assert df.duplicated().sum() == 0, "Duplicates are found!" 


@pytest.mark.validate_csv
def test_validate_schema_attributes(schemas):
    actual_schema, expected_schema = schemas
    assert set(actual_schema.keys()) == set(expected_schema.keys()), "Attribute names do not match!"


@pytest.mark.validate_csv
def test_validate_schema_datatypes(schemas):
    actual_schema, expected_schema = schemas
    for item in expected_schema:
        assert actual_schema[item] == expected_schema[item], "Data Types do not match!"


@pytest.mark.validate_csv
@pytest.mark.skip
def test_age_column_valid(df):
    assert ((df['age'] >= 0) & (df['age'] <= 100)).all(), "Age is invalid!" 

@pytest.mark.validate_csv
def test_email_column_valid(df):
    email_pattern = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+.[a-zA-Z]+$"
    assert df['email'].str.match(email_pattern).all(), "Email is invalid!" 


@pytest.mark.parametrize("id, is_active",[(1,False),(2, True)])
def test_active_players(df,id,is_active):
    df_for_assertion = df[df['id'] == id]
    assert df_for_assertion.shape[0] > 0,"Predefined ID is missing!" 
    assert (df_for_assertion['is_active'] == is_active).all(),"ID active status is not as expected!" 


def test_active_player_two(df):
    df_for_player_two = df[df['id'] == 2]
    assert df_for_player_two.shape[0] > 0,"Predefined ID is missing!" 
    assert (df_for_player_two['is_active'] == True).all(),"Player two status is not Active!"
