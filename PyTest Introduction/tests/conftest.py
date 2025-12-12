import pytest
import pandas as pd

# Fixture to read the CSV file
@pytest.fixture(scope="session")
def df(path_to_file='src/data/data.csv'):
    """
    Loads a CSV file into a pandas DataFrame.
    """
    return pd.read_csv(path_to_file)
    
 
# Fixture to validate the schema of the file
@pytest.fixture(scope="session")
def expected_schema():
    return {
        'id': 'int64',
        'name': 'object',
        'age': 'int64',
        'email': 'object',
        'is_active': 'bool',
    }

@pytest.fixture(scope="session")
def actual_schema(df):
    return {col: str(df[col].dtype) for col in df.columns}

@pytest.fixture(scope="session")
def schemas(actual_schema, expected_schema):
    return actual_schema,expected_schema

# Pytest hook to mark unmarked tests with a custom mark
def pytest_collection_modifyitems(config, items):
   for item in items:
       if not getattr(item, "own_markers", []):
        item.add_marker(pytest.mark.unmarked)

