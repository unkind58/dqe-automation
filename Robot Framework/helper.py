from robot.libraries.BuiltIn import BuiltIn
from robot.api.deco import keyword
from robot.api import logger
from selenium.webdriver.common.by import By
import pyarrow.parquet as pq
import pandas as pd


def standardize_df_data_types(df):
    # Convert 'Facility Type' to string
    if 'Facility Type' in df.columns:
        df['Facility Type'] = df['Facility Type'].astype(str).str.strip()
    # Convert 'Visit Date' to date
    if 'Visit Date' in df.columns:
        df['Visit Date'] = pd.to_datetime(df['Visit Date']).dt.date
    # Convert 'Average Time Spent' to float
    if 'Average Time Spent' in df.columns:
        df['Average Time Spent'] = df['Average Time Spent'].astype(float)
    return df


@keyword
def read_plotly_table_to_dataframe_by_locator(locator, headers):
    seleniumlib = BuiltIn().get_library_instance('SeleniumLibrary')
    driver = seleniumlib.driver
    element = driver.find_element(By.CSS_SELECTOR, locator)
    raw_text = element.text.strip().split('\n')
    logger.info(f"Extracted raw_text: {raw_text}")

    # Predefined column names taken from 'header' variable
    # Fetch indexes 
    facility_type = raw_text.index(headers[0])
    visit_date = raw_text.index(headers[1])
    avg_time_spent = raw_text.index(headers[2])

    # Fetch rows for each of the columns
    facility_type_col = raw_text[:facility_type]
    visit_date_col = raw_text[facility_type+1:visit_date]
    avg_time_spent_col = raw_text[visit_date+1:avg_time_spent]
    rows = list(zip(facility_type_col, visit_date_col, avg_time_spent_col))

    df = pd.DataFrame(rows, columns=headers)
    logger.info('\n' + df.to_string(index=False))
    return df


@keyword
def read_parquet_dataset_with_date_filter(parquet_folder, filter_date, headers=None):
    dataset = pq.ParquetDataset(parquet_folder)
    table = dataset.read()
    df = table.to_pandas()

    # Rename columns to conform with HTML report and drop other columns
    if headers:
        parquet_cols = list(df.columns[:len(headers)])
        rename_map = dict(zip(parquet_cols, headers))
        df = df.rename(columns=rename_map)
        logger.info(f"Renamed columns: {df.columns}")

        # Drop extra columns not in 'headers' variable
        df = df[[col for col in headers if col in df.columns]].copy()

    # Check for date column for filtering DataFrame
    date_col = None
    if 'Visit Date' in df.columns:
        date_col = 'Visit Date'
    elif 'visit_date' in df.columns:
        date_col = 'visit_date'
        
    if date_col:
        df[date_col] = pd.to_datetime(df[date_col])
        df = df[df[date_col] == pd.to_datetime(filter_date)]
        logger.info(f"Filtered DataFrame for date {filter_date}:\n{df}")
    else:
        logger.info(f"No 'Visit Date' or 'visit_date' column was found.")
    return df


@keyword
def compare_dataframes(df1, df2):
    df1 = standardize_df_data_types(df1)
    df2 = standardize_df_data_types(df2)
    match = df1.equals(df2)
    diff = None
    
    # df1.to_csv("results/df1_before_comparison.csv", index=False, encoding="utf-8")
    # df2.to_csv("results/df2_before_comparison.csv", index=False, encoding="utf-8")
    
    if not match:
        diff = {
            'df1_not_in_df2': df1[~df1.apply(tuple,1).isin(df2.apply(tuple,1))].to_dict('records'),
            'df2_not_in_df1': df2[~df2.apply(tuple,1).isin(df1.apply(tuple,1))].to_dict('records')
        }
    logger.info(f"Comparison result: {match}, Differences: {diff}")
    return {'match': match, 'diff': diff}
