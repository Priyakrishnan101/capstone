# Capstone project for credit card loan application

## Overview ##
This credit card application project is based on ETL to extract data content from Json file and do data transformation and finally loaded into database. Loaded data will be viewed by the customer based on the menu option selected.

## Project Summary ##
step 1: Extract Data
step 2: Expolre the data
step 3: Transform

## Scope ##
The project is one provided by PerScholas to showcase the learnings of the student throughout the program. There are four datasets as follows to complete the project.

a) CDW_SAPP_CUSTOMER.JSON: This file has the existing customer details.
b) CDW_SAPP_CREDITCARD.JSON: This file contains all credit card transaction information.
c) CDW_SAPP_BRANCH.JSON: Each branch’s information and details are recorded in this file
d)API Endpoint: https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json

Python is the main language used to complete the project. The libraries used to perform ETL are Pandas and Pyspark.Data was transformed from json format to table format using Pyspark. These tables are explored using Pandas and Pyspark to gain an understanding of the data and transformed according to the mapping document provided. Pyspark was then used to build the ETL pipeline. The data sources provided have been cleaned, transformed to create new features and then saved as the data tables. Also the data is read from the database, and plotted into graphs using python library Matplotlib.

## ETL Flow Diagram ##
![Project_flow_diagram](https://user-images.githubusercontent.com/118327237/224128947-7797d4d4-1733-43a3-8f33-f0f490378e5e.jpg)

## 1.Extract Data ##
The CDW_SAPP_CUSTOMER.JSON,CDW_SAPP_BRANCH.JSON files are extracted into Spark DataFrames and CDW_SAPP_BRANCH.JSON is extracted into Pandas DataFrame. The Loan application Data API is accessed using REST
API by sending an HTTP request and processing the response.

## 2. Explore Data ##
 Using Spark methods show(),columns(),and Pandas method info(),describe(), head() the data is explored

 ## 3. Transform ##
 The data is transformed according to the maooing document provided.

 1. Converted the first and last name to title case using initcap() and middle name to lower case using lower() in spark withcolumn()
 2. Concatenated Apt no and street name using concat()
 3. Formatted the phone number in Regex.
 4. Dropped the columns after cancatenation.
 5. Change the datatype of column using cast() and astype() in pandas.
 6. Convert the date format in to specified (YYYYMMDD) format.
 7. Replaced null values in zipcode column into 99999.

 ## 4. Loading ##
 After transformation data is loaded into MariaDB using SparkSQL connection. Also used python database connectivity for customer modification module.

 ## 5. Data Viualization ##
 Data is read from database usig spark SQL read.format. The data is loaded into pandas dataframe and used Matplotlib for platting the graph.

## GRAPHS ##

![1-High_tr_rate](https://user-images.githubusercontent.com/118327237/224129248-6cbeca42-1529-418a-8d92-04fbd0e43b77.jpg)
![2-state-with-high-cust](https://user-images.githubusercontent.com/118327237/224129270-5c49306c-ffa5-4ad2-b1b7-6690fcb16a7e.jpg)
![3-Top-10-cust](https://user-images.githubusercontent.com/118327237/224129286-6d077e32-72ed-43b9-8dd3-a3f77b530a53.jpg)
![5-married-male-rej](https://user-images.githubusercontent.com/118327237/224129386-1879284d-acb8-4209-abc7-36c780e832af.jpg)
![7-healthcare-branch](https://user-images.githubusercontent.com/118327237/224129413-618e529d-bd9d-409d-bd31-99710562e14c.jpg)
![6-top-3-months](https://user-images.githubusercontent.com/118327237/224129429-b25c268c-da73-4b1b-a89a-0a1ba4b68866.jpg)

## Technologies Used ##
- Python 3.10
- Apache Spark
- Python(Pandas,Matplotlib)
- Maria DB

## Local setup ##
- clone the project to your local directory


## Contributor ##
Padmapriya Radhakrishnan



