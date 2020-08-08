import os
import pandas as pd
import datetime
import pyodbc
import lxml
from sqlalchemy import create_engine

db_connection_string = r'mssql+pyodbc://KEDMUTIA/FinancialData?driver=SQL+Server+Native+Client+11.0'
if_table_exists_argument = 'append'
table_name = 'daily_trading_data'

path = r'C:\Users\dmutia\OneDrive - Deloitte (O365D)\Personal\The-Data-Company\raw_html\live.mystocks.co.ke\live.mystocks.co.ke\price_list'
files = os.listdir(path)
file = files[0]

def stage_to_db(path=path, file=file, db_connection_string=db_connection_string):
    data = pd.read_html(os.path.join(path,file))[3]

    # Remove top level in multilevel column index
    data.columns = data.columns.droplevel()

    # Select relevant columns
    data = data.iloc[:,[0,1,4,5,6,7,11,12]]

    # Convert amount fields to numeric
    columns_to_convert_to_numeric = ['Low','High', 'Price', 'Previous', 'Volume', 'Adjusted Price']
    data[columns_to_convert_to_numeric] = data[columns_to_convert_to_numeric].apply(pd.to_numeric,errors='coerce')
    data = data[data.Low.notnull()]

    # Add filename and date
    data['filename'] = file
    data['extraction_date'] = datetime.datetime.fromtimestamp(os.path.getctime(os.path.join(path,file))).strftime('%Y-%m-%d %H:%M:%S')

    # write to db
    engine = create_engine(db_connection_string)
    data.to_sql(table_name, engine, if_exists=if_table_exists_argument, chunksize=1000)


from download_html import ScrapData