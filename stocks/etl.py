from download_html import ScrapData
from stage_to_db import StageToDB
from create_tables import CreateFactDimTables
from data_quality import DataQualityTests
import os
import pyodbc

import sqlalchemy

download_path = r'.\data\raw_html'
import_path =  r'.\data\raw_html\live.mystocks.co.ke\live.mystocks.co.ke\price_list'

'''
conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=KEDMUTIA;'
                      'Database=FinancialData;'
                      'Trusted_Connection=yes;')
'''

db_connection_string = f"mssql+pyodbc://findata"

download_data = ScrapData(
    start_date = '20200801',
    end_date = '20200807',
    path = download_path
)
download_data.download_html()

stage_data = StageToDB(
    path = import_path,
    files = os.listdir(import_path),
    db_connection_string = db_connection_string,
    if_table_exists_argument = 'append',
    table_name = 'daily_trading_data'
).stage_to_db()

stage_sector = StageToDB(
    path = import_path,
    files = os.listdir(import_path),
    db_connection_string = db_connection_string,
    if_table_exists_argument = 'append',
    table_name = 'daily_trading_data'
).stage_sector_data()

stock_data_table_load = CreateFactDimTables(
    db_connection_string = db_connection_string,
    if_table_exists_argument = 'append'
).create_stock_data_transactions_fact_table()

company_data_table_load = CreateFactDimTables(
    db_connection_string = db_connection_string,
    if_table_exists_argument = 'replace'
).create_company_dim_table()

company_date_table_load = CreateFactDimTables(
    db_connection_string = db_connection_string,
    if_table_exists_argument = 'replace'
).create_date_dim_table()

dq_test = DataQualityTests(
    db_connection_string = db_connection_string
).test_staging()

dq_test_date_range = DataQualityTests(
    db_connection_string = db_connection_string
).test_date_dim_completeness()




