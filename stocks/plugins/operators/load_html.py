import os
import pandas as pd
import datetime
import pyodbc
import lxml

folder = r'C:\Users\dmutia\OneDrive - Deloitte (O365D)\Personal\The-Data-Company\raw_html\live.mystocks.co.ke\live.mystocks.co.ke\price_list'
filename = '630518dc__20200804.html'
data = pd.read_html(os.path.join(folder,filename))[3]

# Remove top level in multilevel column index
data.columns = data.columns.droplevel()

# Select relevant columns
data = data.iloc[:,[0,1,4,5,6,7,11,12]]

# Convert amount fields to numeric
columns_to_convert_to_numeric = ['Low','High', 'Price', 'Previous', 'Volume', 'Adjusted Price']
data[columns_to_convert_to_numeric] = data[columns_to_convert_to_numeric].apply(pd.to_numeric,errors='coerce')
data = data[data.Low.notnull()]

# Add filename and date
data['filename'] = filename
data['extraction_date'] = datetime.datetime.now()