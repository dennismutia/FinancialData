import os
import pyodbc
import sqlalchemy
#from sqlalchemy import create_engine
import pandas as pd

db_connection_string = f"mssql+pyodbc://findata"

class CreateFactDimTables:
    def __init__(self, db_connection_string, if_table_exists_argument):
        self.db_connection_string = db_connection_string
        self.if_table_exists_argument = if_table_exists_argument

    def create_company_dim_table(self):
        query = '''
        SELECT 
            DISTINCT d.[CODE], d.NAME, s.SECTOR      
        FROM [FinancialData].[dbo].[daily_trading_data] d
        JOIN s_SectorData s ON d.CODE=s.[TRADING SYMBOL]
        '''
        df = pd.read_sql(query, self.db_connection_string)
        df.to_sql('d_CompanyDetails', self.db_connection_string, if_exists=self.if_table_exists_argument, chunksize=1000, index=False)

    def create_stock_data_transactions_fact_table(self):
        query = '''
        SELECT
            CODE as code, Low, High, Price, Previous, [Adjusted Price] as adjusted_price, price_date
        FROM [FinancialData].[dbo].[daily_trading_data]
        '''
        df = pd.read_sql(query, self.db_connection_string)
        df.to_sql('f_StockPrice', self.db_connection_string, if_exists=self.if_table_exists_argument, chunksize=1000, index=False)

    def create_date_dim_table(self):
        sql_query = 'select min(price_date) as start_date, max(price_date) as end_date from daily_trading_data'
        date_range = pd.read_sql(sql_query, db_connection_string)
        start_date = date_range['start_date'][0]
        end_date = date_range['end_date'][0]
        df = pd.DataFrame({"Date": pd.date_range(start_date, end_date)})
        df["Day"] = df.Date.dt.day_name()
        df['week'] = df.Date.dt.week
        df['month'] = df.Date.dt.month
        df['quarter'] = df.Date.dt.quarter
        df['year'] = df.Date.dt.year
        df["year_half"] = (df.quarter + 1) // 2
        df.to_sql('d_Dates', self.db_connection_string, if_exists=self.if_table_exists_argument, chunksize=1000, index=False)
       