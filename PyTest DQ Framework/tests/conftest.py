import pytest
from src.connectors.postgres.postgres_connector import PostgresConnectorContextManager
from src.data_quality.data_quality_validation_library import DataQualityLibrary
from src.connectors.file_system.parquet_reader import ParquetReader

def pytest_addoption(parser):
    parser.addoption("--db_host", action="store", default="localhost", help="Database host")
    parser.addoption("--db_port", action="store", default="5434", help="Database port")
    parser.addoption("--db_name", action="store", default="mydatabase", help="Database name")
    parser.addoption("--db_user", action="store", default="myuser", help="Database user")
    parser.addoption("--db_password", action="store", default="mypassword", help="Database password")


def pytest_configure(config):
    """
    Validates that all required command-line options are provided.
    """
    required_options = [
        "--db_user", "--db_password"
    ]
    for option in required_options:
        if not config.getoption(option):
            pytest.fail(f"Missing required option: {option}")

@pytest.fixture(scope='session')
def db_connection(request):
    db_host = request.config.getoption("--db_host")
    db_name = request.config.getoption("--db_name")
    db_port = request.config.getoption("--db_port")
    db_user = request.config.getoption("--db_user")
    db_password = request.config.getoption("--db_password")
    try:
        with PostgresConnectorContextManager(
                                            db_host=db_host,
                                            db_name=db_name,
                                            db_port=db_port,
                                            db_user=db_user,
                                            db_password=db_password
                                            ) as db_connector:
            yield db_connector
    except Exception as e:
        pytest.fail(f"Failed to initialize PostgresConnectorContextManager: {e}")


@pytest.fixture(scope='session')
def parquet_reader(request):
    try:
        reader = ParquetReader()
        yield reader
    except Exception as e:
        pytest.fail(f"Failed to initialize ParquetReader: {e}")
    finally:
        del reader


@pytest.fixture(scope='session')
def data_quality_library():
    try:
        data_quality_library = DataQualityLibrary()
        yield data_quality_library
    except Exception as e:
        pytest.fail(f"Failed to initialize DataQualityLibrary: {e}")
    finally:
        del data_quality_library


@pytest.fixture(scope='module')
def target_data(parquet_reader):
    target_path = '/parquet_data/facility_name_min_time_spent_per_visit_date'
    target_data = parquet_reader.process(target_path, include_subfolders=True)
    return target_data

   
@pytest.fixture(scope='module')
def source_data(db_connection):
    source_query = """
    SELECT
        f.facility_name,
        v.visit_timestamp::date AS visit_date,
        MIN(v.duration_minutes) AS min_time_spent
    FROM
        visits v
    JOIN facilities f 
        ON f.id = v.facility_id
    GROUP BY
        f.facility_name,
        visit_date
    UNION ALL  -- misstake
    SELECT
        f.facility_name,
        v.visit_timestamp::date AS visit_date,
        MIN(v.duration_minutes) AS min_time_spent
    FROM
        visits v
    JOIN facilities f 
        ON f.id = v.facility_id
    WHERE
        f.facility_type = 'Clinic' 
    GROUP BY
        f.facility_name,
        visit_date;
    """
    source_data = db_connection.get_data_sql(source_query)
    return source_data
