from download_html import ScrapData
from stage_to_db import StageToDB
from create_tables import CreateFactDimTables
from data_quality import DataQualityTests
import os
import pyodbc
import sqlalchemy
from sqlalchemy import create_engine

base_dir = os.getcwd()

download_path = os.path.join(base_dir, 'data/raw_html')
import_path =  os.path.join(base_dir, 'data/raw_html/live.mystocks.co.ke/live.mystocks.co.ke/price_list')

db_connection_string = 'postgresql://postgres:postgres@localhost:5432/financialdata'

#db_connection_string = f"mssql+pyodbc://findata"

'''
# Call function to scrape data
download_data = ScrapData(
    start_date = '20190801',
    end_date = '20190807',
    path = download_path
)
download_data.download_html()
'''

# call function to stage downloaded html data
stage_data = StageToDB(
    path = import_path,
    files = os.listdir(import_path),
    db_connection_string = db_connection_string,
    if_table_exists_argument = 'append',
    table_name = 'daily_trading_data'
).stage_to_db()

'''
# call function to stage company details data
stage_sector = StageToDB(
    path = import_path,
    files = os.listdir(import_path),
    db_connection_string = db_connection_string,
    if_table_exists_argument = 'append',
    table_name = 'daily_trading_data'
).stage_sector_data()

# Call function to crate fact table
stock_data_table_load = CreateFactDimTables(
    db_connection_string = db_connection_string,
    if_table_exists_argument = 'append'
).create_stock_data_transactions_fact_table()

# call function to create company details dim table
company_data_table_load = CreateFactDimTables(
    db_connection_string = db_connection_string,
    if_table_exists_argument = 'replace'
).create_company_dim_table()

# call function to create a master calendar dimension table
company_date_table_load = CreateFactDimTables(
    db_connection_string = db_connection_string,
    if_table_exists_argument = 'replace'
).create_date_dim_table()

# Perform data quality checks on fact table
dq_test = DataQualityTests(
    db_connection_string = db_connection_string
).test_staging()

# Perform data quality test on master calandar
dq_test_date_range = DataQualityTests(
    db_connection_string = db_connection_string
).test_date_dim_completeness()

'''
