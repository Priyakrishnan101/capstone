# Capstone project for credit card loan application

## Overview ##
This credit card application project is based on ETL to extract data content from Json file and do data transformation and finally loaded into database. Loaded data will be viewed by the customer based on the menu option selected.

## Project Summary ##
step 1: Extract Data
step 2: Expolre the data

## Scope ##
The project is one provided by PerScholas to showcase the learnings of the student throughout the program. There are four datasets as follows to complete the project.

a) CDW_SAPP_CUSTOMER.JSON: This file has the existing customer details.
b) CDW_SAPP_CREDITCARD.JSON: This file contains all credit card transaction information.
c) CDW_SAPP_BRANCH.JSON: Each branch’s information and details are recorded in this file
d)API Endpoint: https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json

Python is the main language used to complete the project. The libraries used to perform ETL are Pandas and Pyspark.Data was transformed from json format to table format using Pyspark. These tables are explored using Pandas and Pyspark to gain an understanding of the data and transformed according to the mapping document provided. Pyspark was then used to build the ETL pipeline. The data sources provided have been cleaned, transformed to create new features and then saved as the data tables. Also the data is read from the database, and plotted into graphs using python library Matplotlib.

## Project Flow Diagram ##
![Project Flow Diagram](project_flow_diagram.jpg)

## Technologies Used ##
- Python 3.10
- Apache Spark
- Python(Pandas,Matplotlib)
- Maria DB

## Local setup ##
- clone the project to your local directory


## Contributor ##
Padmapriya Radhakrishnan



