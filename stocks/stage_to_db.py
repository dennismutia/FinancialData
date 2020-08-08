import os
import pandas as pd
import datetime
import pyodbc
import lxml
from sqlalchemy import create_engine

class StageToDB:
    def __init__(self, path, files, db_connection_string,  if_table_exists_argument, table_name):
        self.path = path
        self.files = files
        self.db_connection_string = db_connection_string
        self.if_table_exists_argument = if_table_exists_argument
        self.table_name = table_name

    def stage_to_db(self):
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
            data['extraction_date'] = datetime.datetime.fromtimestamp(os.path.getctime(os.path.join(self.path, file))).strftime('%Y-%m-%d %H:%M:%S')

            # write to db
            engine = create_engine(self.db_connection_string)
            data.to_sql(self.table_name, engine, if_exists=self.if_table_exists_argument, chunksize=1000)
            