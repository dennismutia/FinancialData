# Project Summary: DEND Capstone Project - Stock Data ETL

## Introduction
This project scrapes end of day Kenyan security prices data from a website and creates an ETL pipeline.

## Data Sources
1. daily stock data: html files scrapped from https://live.mystocks.co.ke/price_list/
2. company data: excel file downloaded from https://www.nse.co.ke/listed-companies/list.html

## Project files
- download_html.py: scrapes web data and stores in a folder as html files
- stage_to_db.py: crates a staging table and reads the html data into the database table created
- create_tables.py: reads data from the staged tables and creates fact and dimension tables
- data folder: contains html files scraped from the web

## Data Model
The data model uses a star schema with 1 fact table and 2 dimension tables

fact stock price\

![fact stock price](/fact_StockPrice.png)

dim company details\

![dim company details](/dim_CompanyDetails.png)

dim dates\

![dim dates](/dim_Dates.png)

## Technologies
The following technologies were chosen for this project:
* Microsoft SQL Server 2019
* Python 3.7

## Build Instructions
1. Clone the repository
2. Navigate to the root folder and run `pip install -r requirements.txt` to install the required python packages in the requirements.txt file
3. Edit the start_date and end_date variables in *stocks\etl.py* file to specify the date ranges to be scraped by the etl script.
4. Run `python etl.py` to execute the etl.py file which calls the modules in the stocks folder to execute an etl process

## Suggested future improvements
- Configure project to use *Apache Airflow*. Currently using windows machine and configuring airflow is a chore but can be done using windows subsystem for linux.
- Consider using *S3* to store the raw html files for scale, and *a Redshift cluster* for dimension modelling. 