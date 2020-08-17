import os
import numpy as np
import pandas as pd
import datetime
import pyodbc
import lxml
from sqlalchemy import create_engine
import sqlalchemy

class StageToDB:
    '''
    This class stages the daily stock data in the html files to a database table

    input:
    path                        :   path to the location of files to be stages
    files                       :   a list containing the files to be imported
    db_connection_string        :   odbc connection string to sql server database
    if_table_exists_argument    :   argument on what to do if the table created already exists, replace or append
    table_name                  :   the name of the table to be created
    '''
    def __init__(self, path, files, db_connection_string,  if_table_exists_argument, table_name):
        self.path = path
        self.files = files
        self.db_connection_string = db_connection_string
        self.if_table_exists_argument = if_table_exists_argument
        self.table_name = table_name

    def stage_to_db(self):
        '''
        Stage html files
        '''
        for file in self.files:
            data = pd.read_html(os.path.join(self.path, file))[3]

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
            data['price_date'] = datetime.datetime.strptime(file[10:18],'%Y%m%d').strftime('%Y-%m-%d')
            data['extraction_date'] = datetime.datetime.fromtimestamp(os.path.getctime(os.path.join(self.path, file))).strftime('%Y-%m-%d %H:%M:%S')

            # write to db
            engine = create_engine(self.db_connection_string)
            data.to_sql(self.table_name, engine, if_exists=self.if_table_exists_argument, chunksize=1000, index=False)

    def stage_sector_data(self):
        '''
        Stage excel file with company details
        '''
        sectors = pd.read_excel(r'.\data\isin-codes_2020.xlsx', sheet_name='Sheet1')
        engine = create_engine(self.db_connection_string)
        sectors.to_sql('s_SectorData', engine, if_exists=self.if_table_exists_argument, chunksize=1000)