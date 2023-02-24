import findspark
findspark.init()

import pyspark

from secrets1 import username
from secrets1 import password

from pyspark.sql.functions import*
from pyspark.sql import SparkSession
import pandas as pd
import json
import re
from datetime import datetime
import mysql.connector as mariadb
import pyinputplus as pyinput
#%matplotlib inline
import fontstyle

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np

#create spark session
spark = SparkSession.builder.master("local[*]").appName("Capstone app").getOrCreate()

#Extract data from JSON to DataFrame
def branch_extract():
    print("Extraxting branch data...")
    branch_df = spark.read.json("json_files/cdw_sapp_branch.json")
    return branch_df
def customer_extract():
    print("Extraxting customer data...")
    customer_df = spark.read.json("json_files/cdw_sapp_custmer.json")
    return customer_df
def credit_extract():
    print("Extraxting credit data...")
    credit_df = spark.read.json("json_files/cdw_sapp_credit.json")
    return credit_df
def credit_pandas_extract():
    print("Extraxting credit data...")
    #Read credit_json file into pandas dataframe
    credit_df_pandas = pd.read_json("json_files/cdw_sapp_credit.json", lines=True)
    return credit_df_pandas


#Transformation of customer_df according to mapping document
def customer_transform(customer_df):
    print("Transformation of customer_df according to mapping document...")
    #convert First Name to Title case, Middle name in lower case, Last name in Title case
    customer_transform_df = customer_df.withColumn("FIRST_NAME", initcap(customer_df["FIRST_NAME"]))\
                             .withColumn("MIDDLE_NAME", lower(customer_df["MIDDLE_NAME"]))  \
                             .withColumn("LAST_NAME", initcap(customer_df["LAST_NAME"]))  
                             

    #Concatenate apartment no and street name of customer's residence with comma as a separator
    customer_transform_df = customer_transform_df.withColumn("FULL_STREET_ADDRESS", \
                            concat_ws(",",customer_df["APT_NO"],customer_df["STREET_NAME"]))

    #Drop column APT_NO and STREET_NAME
    customer_transform_df = customer_transform_df.drop("APT_NO","STREET_NAME")

    #change the data type of SSN,CUST_ZIP,CUST_PHONE,LAST_UPDATED
    customer_transform_df = customer_transform_df \
      .withColumn("SSN" ,customer_df["SSN"].cast('int'))   \
      .withColumn("CUST_ZIP",customer_df["CUST_ZIP"].cast('int'))   \
      .withColumn("CUST_PHONE",customer_df["CUST_PHONE"].cast('string'))   \
      .withColumn("LAST_UPDATED",customer_df["LAST_UPDATED"].cast('timestamp')) 

    #change the format of the phone number
    customer_transform_df = customer_transform_df.withColumn("CUST_PHONE", regexp_replace(col("CUST_PHONE") ,\
                             "(\\d{3})(\\d{4})" , "(155)$1-$2" ) )
    return customer_transform_df


#Transformation of branch_df according to mapping document
def branch_transform(branch_df):
    print("Transformation of branch data according to mapping document...")
    #change the data type of BRANCH_CODE,BRANCH_ZIP,LAST_UPDATED
    branch_transform_df = branch_df \
       .withColumn("BRANCH_CODE" ,branch_df["BRANCH_CODE"].cast('int'))   \
       .withColumn("BRANCH_ZIP",branch_df["BRANCH_ZIP"].cast('int'))    \
       .withColumn("LAST_UPDATED",branch_df["LAST_UPDATED"].cast('timestamp')) 

    #change the format of the phone number
    branch_transform_df = branch_transform_df.withColumn("BRANCH_PHONE", regexp_replace(col("BRANCH_PHONE") ,\
                          "(\\d{3})(\\d{3})(\\d{4})" , "($1)$2-$3" ) )

    # If source value is null load default value (99999)
    branch_transform_df = branch_transform_df.na.fill(value=99999,subset=["BRANCH_ZIP"])
    return branch_transform_df


def branch_transform1(branch_df):
    print("Transformation of branch data according to mapping document...")
    branch_df.createOrReplaceTempView("branchview")
    branch_transform_df = spark.sql("SELECT CAST(BRANCH_CODE AS INT), BRANCH_NAME, BRANCH_STREET, BRANCH_CITY, \
                          BRANCH_STATE, CAST(IF(BRANCH_ZIP IS NULL, '99999', BRANCH_ZIP) AS INT) AS BRANCH_ZIP, \
                          CONCAT('(', SUBSTR(BRANCH_PHONE, 1, 3), ')', SUBSTR(BRANCH_PHONE, 4,3), '-', SUBSTR(BRANCH_PHONE, 7, 4)) AS BRANCH_PHONE, \
                          CAST(LAST_UPDATED AS TIMESTAMP) FROM BRANCHVIEW")
    return branch_transform_df

#Transformation of credit_df according to mapping document - pandas

def credit_transform(credit_df_pandas):
    print("Transformation of credit data according to mapping document...")
    #change the data type of DAY,MONTH,YEAR
    credit_transform_df_pandas = credit_df_pandas.astype({"DAY":'str',"MONTH":'str',"YEAR":'str',"CREDIT_CARD_NO":'str'})
    credit_transform_df_pandas['DAY'] = credit_transform_df_pandas["DAY"].str.pad(2,side='left',fillchar='0')
    credit_transform_df_pandas['MONTH'] = credit_transform_df_pandas["MONTH"].str.pad(2,side='left',fillchar='0')
    
    #Converting DAY, MONTH, YEAR into a TIMEID (YYYYMMDD)
    credit_transform_df_pandas['TIMEID'] = credit_transform_df_pandas["YEAR"] + credit_transform_df_pandas["MONTH"] \
                                            + credit_transform_df_pandas["DAY"]
    
    #Drop column APT_NO and STREET_NAME
    credit_transform_df_pandas.drop(["DAY","MONTH","YEAR"],axis = 1,inplace = True)
    
    #Rename column CREDIT_CARD_NO to CUST_CC_NO 
    credit_transform_df_pandas.rename(columns = {"CREDIT_CARD_NO":"CUST_CC_NO"}, inplace = True)

    #converting pandas Dataframe to pyspark Dataframe
    credit_transform_df = spark.createDataFrame(credit_transform_df_pandas)

    #change the data type of BRANCH_CODE,CUST_SSN,TRANSACTION_ID
    credit_transform_df = credit_transform_df \
    .withColumn("BRANCH_CODE",credit_transform_df["BRANCH_CODE"].cast('int'))   \
    .withColumn("CUST_SSN",credit_transform_df["CUST_SSN"].cast('int'))   \
    .withColumn("TRANSACTION_ID",credit_transform_df["TRANSACTION_ID"].cast('int')) 

    return credit_transform_df

#Data loading into Database

def branch_load(branch_transform_df):
       print("Loading transformed branch data into Database...")
       branch_transform_df.write.format("jdbc") \
       .mode("append") \
       .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
       .option("createTableColumnTypes", "BRANCH_NAME VARCHAR(30), BRANCH_CITY VARCHAR(30),\
              BRANCH_STREET VARCHAR(30), BRANCH_STATE VARCHAR(30) ,BRANCH_PHONE VARCHAR(14) ")  \
       .option("dbtable", "creditcard_capstone.CDW_SAPP_BRANCH") \
       .option("user", username) \
       .option("password", password) \
       .option("characterEncoding","UTF-8") \
       .option("useUnicode", "true") \
       .save()

def credit_load(credit_transform_df):
       print("Loading transformed credit data into Database...")
       credit_transform_df.write.format("jdbc") \
        .mode("append") \
        .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
        .option("createTableColumnTypes", "CUST_CC_NO VARCHAR(30), TRANSACTION_TYPE VARCHAR(30),\
               TIMEID VARCHAR(30)")  \
        .option("dbtable", "creditcard_capstone.CDW_SAPP_CREDIT_CARD") \
        .option("user", username) \
        .option("password", password) \
        .option("characterEncoding","UTF-8") \
        .option("useUnicode", "true") \
        .save()

def customer_load(customer_transform_df):
       print("Loading transformed customer data into Database...")
       customer_transform_df.write.format("jdbc") \
        .mode("append") \
        .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
        .option("createTableColumnTypes", "FIRST_NAME VARCHAR(30), MIDDLE_NAME VARCHAR(30),\
               LAST_NAME VARCHAR(30),CREDIT_CARD_NO VARCHAR(30),FULL_STREET_ADDRESS VARCHAR(50), \
               CUST_CITY VARCHAR(30),CUST_STATE VARCHAR(30),CUST_COUNTRY VARCHAR(30),  \
               CUST_PHONE VARCHAR(30),CUST_EMAIL VARCHAR(30)")  \
        .option("dbtable", "creditcard_capstone.CDW_SAPP_CUSTOMER") \
        .option("user", username) \
        .option("password", password) \
        .option("characterEncoding","UTF-8") \
        .option("useUnicode", "true") \
        .save()


if __name__ == "__main__":      
    # Log that you have started the ETL process
    print("ETL Job Started \n")

    # Log that you have started the Extract step
    print("Extract phase Started \n\n")

    # Call the Extract function
    extracted_customer_data = customer_extract()
    extracted_branch_data = branch_extract()
    extracted_credit_data = credit_pandas_extract()

    # Log that you have completed the Extract step
    print("Extract phase Ended \n")

    # Log that you have started the Transform step
    print("Transform phase Started \n\n")

    # Call the Transform function
    customer_transformed_data = customer_transform(extracted_customer_data)
    branch_transformed_data = branch_transform(extracted_branch_data)
    credit_transformed_data = credit_transform(extracted_credit_data)

    # Log that you have completed the Transform step
    print("Transform phase Ended \n")

    # Log that you have started the Load step
    print("Load phase Started \n\n")

    # Call the Load function
    customer_load(customer_transformed_data)
    branch_load(branch_transformed_data)
    credit_load(credit_transformed_data)


    # Log that you have completed the Load step
    print("Load phase Ended \n")

    # Log that you have completed the ETL process
    print("ETL Job Ended \n\n")