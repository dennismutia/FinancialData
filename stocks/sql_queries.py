
# Create tables
security_prices_table_create = ("""
SELECT 
      [CODE]
      ,[Low]
      ,[High]
      ,[Price]
      ,[Previous]
      ,[Volume]
      ,[Adjusted Price]
      ,CAST([price_date] AS DATE) as price_date
      ,CAST([extraction_date] AS DATE) as extraction_date
  FROM [FinancialData].[dbo].[daily_trading_data]
;
""")

company_codes_table_create = ("""
SELECT 
      DISTINCT [CODE]
      ,[NAME]
  FROM [FinancialData].[dbo].[daily_trading_data]
);
""")
import datetime

def create_date_table(start='2000-01-01', end='2050-12-31'):
  df = pd.DataFrame({"Date": pd.date_range(start, end)})
  df["Day"] = df.Date.strftime("%A")
  df["Week"] = df.Date.dt.weekofyear
  df["Quarter"] = df.Date.dt.quarter
  df["Year"] = df.Date.dt.year
  df["Year_half"] = (df.Quarter + 1) // 2
  return df

start='2000-01-01' 
end='2050-12-31'
df = pd.DataFrame({"Date": pd.date_range(start, end)})
df["Day"] = df.Date.dt.day_name()

df.head()



# Query lists
create_table_queries = [security_prices_table_create, company_codes_table_create]