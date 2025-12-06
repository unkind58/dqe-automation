*** Settings ***
Library           SeleniumLibrary
Library           helper.py
Suite Setup       Open Browser To Report
Suite Teardown    Close Browser


*** Variables ***
${REPORT_FILE}		${CURDIR}/reports/report.html
${PARQUET_FOLDER}	${CURDIR}/facility_type_avg_time_spent_per_visit_date/
${CHROME_DRIVER}	${CURDIR}/drivers/chromedriver.exe
${FILTER_DATE}		2025-10-30
${LOCATOR}			.table
@{HEADERS}			Facility Type    Visit Date    Average Time Spent


*** Test Cases ***
Compare Report Table With Parquet Data
    Sleep    5s
    ${df_html}=		Read Plotly Table To Dataframe By Locator  ${LOCATOR}	 ${HEADERS}
    ${df_parquet}=	Read Parquet Dataset With Date Filter    ${PARQUET_FOLDER}    ${FILTER_DATE}    ${HEADERS}
    ${result}=    	Compare Dataframes    ${df_html}    ${df_parquet}
    Should Be True	${result['match']}    DataFrames do not match!
    Log    			Differences: ${result['diff']}

	Close Browser
	

*** Keywords ***
Open Browser To Report
    Open Browser    file://${REPORT_FILE}    Chrome    executable_path=${CHROME_DRIVER}
    Maximize Browser Window