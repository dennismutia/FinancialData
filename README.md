# Project Summary: DEND Capstone Project - Stock Data ETL

## Introduction
This project scrapes end of day Kenyan security prices data from a website and creates a data model from an ETL pipeline.

The project follows the following steps:

## Step 1: Scope the project and gather data
The scope of this project is to create an ETL process that pulls securities data from a website and creates a data model to enable users analyze the price movements for different securities in the Nairobi Securities exchange. It will cover only the Kenyan securities market but can be expanded to other stock exchanges as well.

The objective is to create a data model that enables users to analyze stock data per sector, or per day or company.

The intended outcome is a *star schema* with date and company dimensions and a stock price fact table.

Two different datasets were used in this project as gathered from the following sources:
- daily stock data: html files scrapped from https://live.mystocks.co.ke/price_list/ for stocks traded in the Nairobi Securities Exchange
- company data: excel file downloaded from https://www.nse.co.ke/listed-companies/list.html containing details of companies listed on the Nairobi Securities Exchange

When *etl.py* is run, it calls the [scrap data](stocks/download_html.py) module to download html files.

## Step 2: Explore and assess the data
After downloading the html files, use pandas to assess the structure of the data in the html files and explore ways of converting it to tabular data. This is done in [staging link](stocks/stage_to_db.py) 
The clean data is then loaded onto a staging table in the database.

## Step 3: Define the data model

Below is the data mode defined for this project. It is a star schema with 1 fact and 2 dimension tables.

fact stock price

|field  	    |type	    |description                        |
|---------------|-----------|-----------------------------------|
|code	        |varchar	|company ticker symbol              |
|Low	        |float	    |lowest price during the day        |
|High	        |float	    |highest price during the day       |
|Price	        |float	    |closing stock price for the day    |
|Previous	    |float	    |previous day closing price         |
|adjusted_price	|float	    |adjsuted price                     |
|price_date	    |date       |date the price applies to          |


dim company details

|field	|type	    |description                    |
|-------|-----------|-------------------------------|
|code	|varchar	|company ticker symbol          |
|name	|varchar	|company name                   |
|sector	|varchar	|sector the company belongs to  |


dim dates

|field	    |type	|description        |
|-----------|-------|-------------------|
|date	    |date	|date               |
|day	    |int	|day name           |
|week	    |int	|week number        |
|month	    |int	|month number       |
|quarter	|int	|quarter of the year|
|year	    |int	|year               |
|year_half	|int	|half of the year   |


## Step 4: Run ETL to model the data

The following technologies have been used to run the ETL process:
* Microsoft SQL Server 2019 :   It is a suitable RDBMS if running on windows and you already have access to a sql server. It is also cheaper to prototype on a sql server before migrating to a cloud solution such as AWS S3 and Redshift. In my case, I found it faster to develop on SQL server since is was already installed and configured and intergrated well with Active Directory for auth.
* Python 3.7                :   Python is preferred for easy intergration with Apache Airflow in the future. Python also also comes with pandas library for easy data cleansing of raw html files.

The above technologies are also very widely used in the industry especially for prototyping and building scalable applications. Support and documentation is highly and readily available online. SQL server also has a free version for prototyping and building non-production databases.

After setting up the build environment, execute the followin steps to run the etl pipeline.
1. Create a database in SQL server whether the data will be loaded into.
2. Create an ODBC connection to the database created above. Call it *findata*.
3. Clone the repository
4. Navigate to the root folder and run `pip install -r requirements.txt` to install the required python packages in the requirements.txt file
5. Edit the start_date and end_date variables in *stocks\etl.py* file to specify the date ranges to be scraped by the etl script.
6. Run `python etl.py` to execute the etl.py file which calls the modules in the stocks folder to execute an etl process

### Logical scenarios
**The data was increased by 100x** . A scalable deployment of sql server would have to be used, eg. a cloud solution, or resources increased to accomodate the increase size in data. Another approach is to use an auto-scaling solution such as AWS Redshift and S3.
**The pipelines would be run on a daily basis by 7 am every day**. This can be configured using Apache Airflow DAG definitions and set up for backfills as well.
**The database needed to be accessed by 100+ people**. If the deployment is done on an auto-scaling architecture, this should be perfectly possible.

## Project files
- download_html.py: scrapes web data and stores in a folder as html files
- stage_to_db.py: crates a staging table and reads the html data into the database table created
- create_tables.py: reads data from the staged tables and creates fact and dimension tables
- data folder: contains html files scraped from the web


## Suggested future improvements
- Refactor project to use *Apache Airflow*. Currently using windows machine and configuring airflow is a chore but can be done using windows subsystem for linux.
- Consider using *S3* to store the raw html files for scale, and *a Redshift cluster* for dimension modelling. 