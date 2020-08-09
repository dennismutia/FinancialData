from download_html import ScrapData
from stage_to_db import StageToDB
import os

download_path = r'.\data\raw_html'
import_path =  r'.\data\raw_html\live.mystocks.co.ke\live.mystocks.co.ke\price_list'
db_connection_string = r'mssql+pyodbc://KEDMUTIA/FinancialData?driver=SQL+Server+Native+Client+11.0'

download_data = ScrapData(
    start_date = '20200803',
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

