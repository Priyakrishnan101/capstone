import findspark
findspark.init()

import pyspark

from secrets1 import username
from secrets1 import password

from pyspark.sql.functions import*
from pyspark.sql import SparkSession
import pandas as pd
import requests
import re
#from datetime import datetime
import mysql.connector as mariadb
import pyinputplus as pyinput
#%matplotlib inline
import fontstyle

import matplotlib.pyplot as plt
#import numpy as np

#create spark session
spark = SparkSession.builder.master("local[*]").appName("Capstone app").getOrCreate()

#Extract data from JSON to DataFrame
def branch_extract():
    print("\tExtraxting branch data...")
    branch_df = spark.read.json("json_files/cdw_sapp_branch.json")
    return branch_df
def customer_extract():
    print("\tExtraxting customer data...")
    customer_df = spark.read.json("json_files/cdw_sapp_custmer.json")
    return customer_df
def credit_extract():
    print("\tExtraxting credit data...")
    credit_df = spark.read.json("json_files/cdw_sapp_credit.json")
    return credit_df
def credit_pandas_extract():
    print("\tExtraxting credit data...")
    #Read credit_json file into pandas dataframe
    credit_df_pandas = None
    try:
        credit_df_pandas = pd.read_json("json_files/cdw_sapp_credit.json", lines=True)
    except Exception as e:
        print(f"Exception in credit_pandas_extract: {e}")
        raise Exception(e)    
    return credit_df_pandas

#Loan Application Data - Module
def loan_app():
    print("\tAccessing LOAN application Data API ")
    url = "https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json"
    print("\n\tStatus code for the loan API: ",requests.get(url).status_code)
    loan_api_list = requests.get(url).json()
    loan_df=spark.createDataFrame(loan_api_list)
    return loan_df



#Transformation of customer_df according to mapping document
def customer_transform(customer_df):
    print("\tTransformation of customer_df according to mapping document...")
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
    print("\tTransformation of branch data according to mapping document...")
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
    print("\tTransformation of branch data according to mapping document...")
    branch_df.createOrReplaceTempView("branchview")
    branch_transform_df = spark.sql("SELECT CAST(BRANCH_CODE AS INT), BRANCH_NAME, BRANCH_STREET, BRANCH_CITY, \
                          BRANCH_STATE, CAST(IF(BRANCH_ZIP IS NULL, '99999', BRANCH_ZIP) AS INT) AS BRANCH_ZIP, \
                          CONCAT('(', SUBSTR(BRANCH_PHONE, 1, 3), ')', SUBSTR(BRANCH_PHONE, 4,3), '-', SUBSTR(BRANCH_PHONE, 7, 4)) AS BRANCH_PHONE, \
                          CAST(LAST_UPDATED AS TIMESTAMP) FROM BRANCHVIEW")
    return branch_transform_df



#Transformation of credit_df according to mapping document - pandas
def credit_transform(credit_df_pandas):
    print("\tTransformation of credit data according to mapping document...")
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
       print("\tLoading transformed branch data into Database...")
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
       print("\tLoading transformed credit data into Database...")
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
       print("\tLoading transformed customer data into Database...")
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



def loan_load(loan_df):
    print("\n\tLoading loan data into Database....")
    loan_df.write.format("jdbc") \
    .mode("append") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("dbtable", "creditcard_capstone.CDW_SAPP_loan_application") \
    .option("user", username) \
    .option("password", password) \
    .option("characterEncoding","UTF-8") \
    .option("useUnicode", "True") \
    .save()
    

#Functional Requirements - Application Front-End

#load from database
def load_branch_from_db():
    branch_df=spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",\
                                     user=username,\
                                     password=password,\
                                     url="jdbc:mysql://localhost:3306/companyabc_db",\
                                     dbtable="creditcard_capstone.CDW_SAPP_BRANCH").load()
    return branch_df


def load_credit_from_db():
    credit_df=spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",\
                                     user=username,\
                                     password=password,\
                                     url="jdbc:mysql://localhost:3306/companyabc_db",\
                                     dbtable="creditcard_capstone.CDW_SAPP_CREDIT_CARD").load()
    return credit_df


def load_customer_from_db():
    customer_df=spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",\
                                     user=username,\
                                     password=password,\
                                     url="jdbc:mysql://localhost:3306/companyabc_db",\
                                     dbtable="creditcard_capstone.CDW_SAPP_CUSTOMER").load()
    return customer_df


def load_loan_from_db():
    loan_df=spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",\
                                     user=username,\
                                     password=password,\
                                     url="jdbc:mysql://localhost:3306/companyabc_db",\
                                     dbtable="creditcard_capstone.CDW_SAPP_loan_application").load()
    return loan_df


#create view from Spark DataFrame
def create_branch_view():
    branch_df = load_branch_from_db()
    branchview = branch_df.createOrReplaceTempView("branchview")
    return branchview
def create_customer_view():
    customer_df = load_customer_from_db()
    customerview = customer_df.createOrReplaceTempView("customerview")
    return customerview
def create_credit_view():
    credit_df = load_credit_from_db()
    creditview = credit_df.createOrReplaceTempView("creditview")
    return creditview
    

#validation functions
def common_validator(display_text_user, validator_method):
    is_valid_input = True
    while is_valid_input:
        user_input_value = input(display_text_user)
        is_valid_input = validator_method(user_input_value)
        if is_valid_input:
            return user_input_value
        else:
            print('\tInvalid input')
            is_valid_input = True
            
#Transaction type (to check in the transaction type list) validator
def tr_type_validator(display_text_user, validator_method,validator_list):
    is_valid_input = True
    while is_valid_input:
        user_input_value = input(display_text_user).title()
        is_valid_input = validator_method(user_input_value,validator_list)
        if is_valid_input:
            return user_input_value
        else:
            print('\tInvalid input')
            is_valid_input = True

# State (to check from the branch state list) validator
def state_validator(display_text_user, validator_method,validator_list):
    is_valid_input = True
    while is_valid_input:
        user_input_value = input(display_text_user).upper()
        is_valid_input = validator_method(user_input_value,validator_list)
        if is_valid_input:
            return user_input_value
        else:
            print('\tInvalid input')
            is_valid_input = True

#First name,Middle name and last name validator
def name_validator(display_text_user, validator_method,variable):
    is_valid_input = True
    while is_valid_input:
        if variable == "MIDDLE_NAME":
            user_input_value = input(display_text_user).lower()
        else:
            user_input_value = input(display_text_user).title()
        is_valid_input = validator_method(user_input_value)
        if is_valid_input:
            return user_input_value
        else:
            print('\tInvalid input')
            is_valid_input = True

# Home number and street address validator
def street_validator(validator_method):
    is_valid_input = True
    while is_valid_input:
        home_no = input("\tPlease enter home or APT number: ")
        street_address = input("\tPlease enter street address: ").title()
        user_input_value = home_no +','+ street_address
        is_valid_input = validator_method(home_no,street_address)
        if is_valid_input:
            return user_input_value
        else:
            print('\tInvalid input')
            is_valid_input = True


def validate_zip_code(zip_code):
    pattern = r'\b\d{5}\b'
    return bool(re.match(pattern, str(zip_code)))

def validate_month(month):
    return True if month.isnumeric() and int(month) in list(range(1,13))  and len(month) > 0 and  len(month) <3 else False

def validate_year(year):
    return True if year.isnumeric() and int(year)==2018 else False

def validate_cc_no(cc_number):
    return True if cc_number.isnumeric() and len(cc_number) == 16 else False

def validate_ph_no(ph_no):
    return True if ph_no.isnumeric() and len(ph_no) == 10 else False

def validate_transaction_type(tr_type,tr_type_list):
    return True if tr_type.isalpha() and tr_type in tr_type_list else False

def validate_transaction_type(tr_type,tr_type_list):
    return True if tr_type.isalpha() and tr_type in tr_type_list else False

def validate_state(state_input,state_list):
    return True if state_input.isalpha() and state_input in state_list else False

def validate_name(name):
    return True if name.isalpha() and len(name) <20 else False

def validate_email(email):
    pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'
    return bool(re.match(pattern, str(email)))

def validate_street(home_no,street):
    return True if home_no.isnumeric() else False

def validate_city(city):
    pattern = r"^[a-zA-Z\s]+$"
    return bool(re.match(pattern, str(city)))
    #return True if city.isalpha() and len(city) <20 else False

def validate_state_common(state):
    pattern = r"^[A-Za-z]{2}$"
    return bool(re.match(pattern, str(state)))

def validate_country(country):
    return True if country.lower() == 'united states' else False

def validate_date(input_date):
    pattern = r"^2018-\d{2}-\d{2}$$"
    return bool(re.match(pattern, str(input_date)))


#Transaction Details Module
#display the transactions made by customers living in a given zip code for a given month and year.
#  Order by descending order.
def transaction_for_zipcode():
        creditview= create_credit_view()
        customerview= create_customer_view()
        zipcode_input = common_validator("Enter 5-digit zip_code: ", validate_zip_code)
        month_input = common_validator("Enter month(in digits): ", validate_month)
        year_input = common_validator("Enter year: ", validate_year)

        #add padding for the month variable
        month_input = month_input.rjust(2,'0')

        query = f"select concat(FIRST_NAME,' ',MIDDLE_NAME,' ',LAST_NAME) as FULL_NAME, \
                TRANSACTION_ID,TRANSACTION_TYPE,round(TRANSACTION_VALUE,2) as TRANSACTION_VALUE, \
                concat(FULL_STREET_ADDRESS,' ',CUST_CITY,' ',CUST_STATE) as ADDRESS, CUST_PHONE, \
                CUST_ZIP,TIMEID as TRANSACTION_DATE from creditview AS credit JOIN customerview\
                AS customer on customer.CREDIT_CARD_NO = credit.CUST_CC_NO WHERE \
                customer.CUST_ZIP = {zipcode_input} AND month(to_timestamp(TIMEID,'yyyyMMdd')) = '{month_input}' \
                AND YEAR(to_timestamp(TIMEID,'yyyyMMdd')) = '{year_input}' order by day(credit.TIMEID) desc"       
        spark.sql(query).show(truncate = False)


#display the number and total values of transactions for a given type.
def transaction_value_by_number():
    creditview= create_credit_view()
    #get unique transaction type list
    tr_type_query = f"select distinct(TRANSACTION_TYPE) from creditview"
    tr_type_df= spark.sql(tr_type_query)
    tr_type_list = tr_type_df.select('TRANSACTION_TYPE').rdd.map(lambda row : row[0]).collect()

    print("Transaction types: ",tr_type_list)
    tr_type_input = tr_type_validator("Enter Transaction Type: ", validate_transaction_type,tr_type_list)

    query = f"SELECT count(TRANSACTION_ID), round(SUM(TRANSACTION_VALUE),2) as TRANSACTION_VALUE \
                 FROM creditview \
                 WHERE TRANSACTION_TYPE = '{tr_type_input}' \
                 GROUP BY TRANSACTION_TYPE"
    spark.sql(query).show()	


#display the number and total values of transactions for branches in a given state
def transaction_branch_state():
    creditview= create_credit_view()
    branchview = create_branch_view()
    #get state list
    state_query = f"select distinct(BRANCH_STATE) from branchview"
    state_df= spark.sql(state_query)
    state_list =state_df.select('BRANCH_STATE').rdd.map(lambda row : row[0]).collect()

    print("state code list: ", state_list)
    state_input = state_validator("Enter two letter state code from the list: ", validate_state,state_list)

    # query = f"SELECT branch_state,credit.TRANSACTION_ID, SUM(credit.TRANSACTION_VALUE) \
                #  FROM cdw_sapp_branch AS branch INNER JOIN cdw_sapp_credit_card AS credit \
                #  ON branch.BRANCH_CODE = credit.BRANCH_CODE WHERE branch_state= '{state}' \
                #  GROUP BY credit.TRANSACTION_ID GROUP BY branch.branch_code"
    query= f"SELECT count(TRANSACTION_ID) as NUMBER_OF_TRANSACTIONS,round(sum(TRANSACTION_VALUE),2) as TOTAL_RANSACTIONS FROM branchview \
             AS branch INNER JOIN creditview AS credit ON branch.BRANCH_CODE = credit.BRANCH_CODE \
             WHERE BRANCH_STATE= '{state_input}' group by branch.BRANCH_NAME"
    spark.sql(query).show()	


# Customer Details Module
#check the existing account details of a customer.
def account_details():
    customerview = create_customer_view()
    #check the existing account details of a customer.
    cc_no_input = common_validator("\tEnter 16-digit credit card number: ", validate_cc_no)

    query= f"SELECT concat(FIRST_NAME,' ',MIDDLE_NAME,' ',LAST_NAME) as FULL_NAME,\
             FULL_STREET_ADDRESS,CUST_CITY as CITY, CUST_STATE as STATE,\
             CUST_COUNTRY as COUNTRY,CUST_ZIP as ZIP_CODE,CUST_PHONE as PHONE_NUMBER,CUST_EMAIL as EMAIL\
             from customerview where CREDIT_CARD_NO = '{cc_no_input}'"

    spark.sql(query).show(truncate = False)
    return cc_no_input

#Display customer details given the credit_card_number
def customer_display(cc_no_input):
    customerview = create_customer_view()
    query= f"SELECT concat(FIRST_NAME,' ',MIDDLE_NAME,' ',LAST_NAME) as FULL_NAME,\
          FULL_STREET_ADDRESS,CUST_CITY as CITY, CUST_STATE as STATE,\
          CUST_COUNTRY as COUNTRY,CUST_ZIP as ZIP_CODE,CUST_PHONE as PHONE_NUMBER,CUST_EMAIL as EMAIL\
          from customerview where CREDIT_CARD_NO = '{cc_no_input}'"
    spark.sql(query).show(truncate = False)


#Establish Database connectivity and update
def update_customer(update_query_variable,user_update_input,credit_card_no_input):
    con=mariadb.connect(host="localhost",user=username,password=password,database="creditcard_capstone")
    cur=con.cursor()
    st=f"update cdw_sapp_customer set {update_query_variable} = '{user_update_input}'\
         where CREDIT_CARD_NO = '{credit_card_no_input}'"
    cur.execute(st)
    con.commit()
    cur.close()
    con.close()

def modify_customer():
    #modify the existing account details of a customer
    #check the existing account details of a customer.
    acc_no = account_details()
    #Get user input
    while True:
        print('''\n\tEnter choice from below to update:
         1.Update First Name
         2.Update Last Name
         3.Update Middle Name
         4.Update phone Number
         5.Update Email
         6.Update Full_Street_Address
         7.Update city
         8.Update state
         9.Update Contry
         10.Update Zip_Code
         11.Display 
         12.Exit''')
        user_choice = pyinput.inputInt("\tPlease enter your choice from option 1 to 12:")
        if str(user_choice).isspace():
            print(" Invalid user choice ")
            user_choice = 0

        elif int(user_choice) in list(range(1,13)) ==False:
            print("\tInvalid user choice ")
            user_choice=0

        elif int(user_choice) == 1:
            update_query_variable = "FIRST_NAME"
            display_text = "\tEnter First Name to update: "
            first_name_input = name_validator(display_text,validate_name,update_query_variable)
            update_customer(update_query_variable,first_name_input,acc_no)
            print("\n\tUpdated First Name!")

        elif int(user_choice) == 2:
            update_query_variable = "LAST_NAME"
            display_text = "\tEnter Last Name to update: "
            last_name_input = name_validator(display_text,validate_name,update_query_variable)
            update_customer(update_query_variable,last_name_input,acc_no)
            print("\n\tUpdated Last Name!")

        elif int(user_choice) == 3:
            update_query_variable = "MIDDLE_NAME"
            display_text = "\tEnter Middle Name to update: "
            middle_name_input = name_validator(display_text,validate_name,update_query_variable)
            update_customer(update_query_variable,middle_name_input,acc_no)
            print("\n\tUpdated Middle Name!")

        elif int(user_choice) == 4:
            update_query_variable = "CUST_PHONE"
            display_text = "\tEnter 10 digit phone number toupdate: "
            phone_no_input = common_validator(display_text,validate_ph_no)
            phone_no_input= f"({phone_no_input[:3]}){phone_no_input[3:6]}-{phone_no_input[6:10]}"
            update_customer(update_query_variable,phone_no_input,acc_no)
            print("\n\tUpdated Phone Number!")

        elif int(user_choice) == 5:
            update_query_variable = "CUST_EMAIL to update"
            display_text = "\tEnter Email-id: "
            email_input = common_validator(display_text,validate_email)
            update_customer(update_query_variable,email_input,acc_no)
            print("\n\tUpdated Email-id!")

        elif int(user_choice) == 6:
            update_query_variable = "FULL_STREET_ADDRESS to update"
            street_input = street_validator(validate_street)
            update_customer(update_query_variable,street_input,acc_no)
            print("\n\tUpdated Full Street Address!")

        elif int(user_choice) == 7:
            update_query_variable = "CUST_CITY"
            display_text = "\tEnter City to update: "
            city_input = common_validator(display_text,validate_city)
            update_customer(update_query_variable,city_input,acc_no)
            print("\n\tUpdated City!")

        elif int(user_choice) == 8:
            states_list = [ 'AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA',
           'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME',
           'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM',
           'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX',
           'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY']
            print("\n\tUnited States state code list: ")
            print(states_list)
            update_query_variable = "CUST_STATE"
            display_text = "\tEnter two letter State code to update: "
            state_input = state_validator(display_text,validate_state,states_list)
            state_input = state_input.upper()
            update_customer(update_query_variable,state_input,acc_no)
            print("\n\tUpdated State!")

        elif int(user_choice) == 9:
            update_query_variable = "CUST_COUNTRY"
            display_text = "\tEnter country to update: "
            country_input = common_validator(display_text,validate_country)
            country_input = country_input.title()
            update_customer(update_query_variable,country_input,acc_no)
            print("\n\tUpdated Country!")

        elif int(user_choice) == 10:
            update_query_variable = "CUST_ZIP"
            display_text = "\tEnter 5-digit ZIP code to update: "
            zipcode_input = common_validator(display_text,validate_zip_code)
            update_customer(update_query_variable,zipcode_input,acc_no)
            print("\n\tUpdated Zip_Code!")

        elif int(user_choice) == 11:
            customer_display(acc_no)
            
        elif int(user_choice) == 12:
            print("\n\tExiting from customer-update menu...")
            break
        else:
            print("\tInvalid choice")
#generate a monthly bill for a credit card number for a given month and year.
def monthly_bill():
    creditview= create_credit_view()
    credit_card_input = common_validator("\tEnter 16-digit credit card number: ", validate_cc_no)
    month_input = common_validator("\tEnter month(in digits): ", validate_month)
    year_input = common_validator("\tEnter year: ", validate_year)

    
    qurey = query = f"SELECT CUST_CC_NO, ROUND(SUM(TRANSACTION_VALUE),2) as Monthly_Bill FROM creditview \
                      WHERE MONTH(to_date(TIMEID, 'yyyyMMdd')) = {month_input} AND \
                      YEAR(to_date(TIMEID, 'yyyyMMdd')) = {year_input} and CUST_CC_NO={credit_card_input} \
                      GROUP BY CUST_CC_NO"
    spark.sql(query).show()


#display the transactions made by a customer between two dates. Order by year, month, and day in descending order.
def transactions_by_dates():
        creditview= create_credit_view()
        credit_card_input = common_validator("\tEnter 16-digit credit card number: ", validate_cc_no)
        start_date_input = common_validator("\tEnter End Date in the format(YYYY-MM-DD): ", validate_date)
        end_date_input = common_validator("\tEnter End Date in the format(YYYY-MM-DD): ", validate_date)

        query = f"SELECT TRANSACTION_ID,CUST_CC_NO,BRANCH_CODE,TRANSACTION_TYPE,TRANSACTION_VALUE, \
                TIMEID FROM creditview \
                WHERE to_date(TIMEID, 'yyyyMMdd') >= '{start_date_input}' \
                AND to_date(TIMEID, 'yyyyMMdd') <= '{end_date_input}' \
                AND cust_cc_no = {credit_card_input} ORDER BY \
                YEAR(to_date(TIMEID, 'yyyyMMdd')) DESC, MONTH(to_date(TIMEID, 'yyyyMMdd')) DESC, \
                DAY(to_date(TIMEID, 'yyyyMMdd')) DESC"
        spark.sql(query).show()


#Data analysis and Visualization

# def load_to_pandas(branch_df,credit_df,customer_df):
    # branch_df_pandas = branch_df.toPandas()
    # credit_df_pandas = credit_df.toPandas()
    # customer_df_pandas = customer_df.toPandas()
    # return (branch_df_pandas,credit_df_pandas,customer_df_pandas)

# load data to pandas dataframe
def load_to_branch_pandas():
    branch_df = load_branch_from_db()
    branch_df_pandas = branch_df.toPandas()
    return branch_df_pandas

def load_to_credit_pandas():
    credit_df = load_credit_from_db()
    credit_df_pandas = credit_df.toPandas()
    return credit_df_pandas

def load_to_customer_pandas():
    customer_df = load_customer_from_db()
    customer_df_pandas = customer_df.toPandas()
    return customer_df_pandas

#plot which transaction type has a high rate of transactions
def high_transaction_rate_type():
    credit_df_pandas = load_to_credit_pandas()
    tr_rate_df = credit_df_pandas[["TRANSACTION_TYPE","TRANSACTION_ID"]]
    tr_rate_df = tr_rate_df['TRANSACTION_TYPE'].value_counts().sort_values(ascending = True)
    #tr_rate_df = tr_rate_df.groupby('TRANSACTION_TYPE')["TRANSACTION_ID"].count().sort_values(ascending = False)
    values = []
    for index, value in enumerate(tr_rate_df):
        values.append(value)
    max_value = values[6]
    clrs = ['grey' if (y < max_value) else 'LightSeaGreen' for y in values ]
    tr_rate_df.plot(x = "TRANSACTION_TYPE", y="TRANSACTION_ID", kind = "barh",color = clrs, figsize=(6, 4),bottom = 0.355)
    plt.ylabel("Transaction Type")
    plt.xlabel("number of Transaction ID's")
    plt.title("Transaction Type with high Transaction Rate")
    # # annotate value labels to each country
    for index, value in enumerate(tr_rate_df): 
        label = format(int(value), ',') # format int with commas

    # place text at the end of bar (subtracting 47000 from x, and 0.1 from y to make it fit within the bar)
        plt.annotate(label, xy=(value - 650, index - 0.10), color='Black')
    plt.show()


#plot which state has a high number of customers.
def high_customers_state():
    customer_df_pandas = load_to_customer_pandas()
    st_cust_df = customer_df_pandas[["CUST_STATE","CREDIT_CARD_NO"]]
    st_cust_df = st_cust_df.groupby('CUST_STATE')['CREDIT_CARD_NO'].nunique().sort_values(ascending=False)
    st_cust_df.plot(x = st_cust_df.index, y = st_cust_df.values, kind = "bar",color='SteelBlue',figsize=(10, 4))
    #st_cust_df.plot(x= st_cust_df.index, y = st_cust_df.values, kind='line', linestyle = "dashed",figsize=(10, 4),marker = 'o',linewidth = 3,grid = True, \
                    #markerfacecolor = "Black") # pass a tuple (x, y) size
    plt.xlabel("States")
    plt.ylabel("Customers")
    plt.title("State with high number of customers")
    plt.grid(linestyle='dashed', axis="both",zorder=0.99,alpha = 0.15)

    plt.annotate('',  # s: str. will leave it blank for no text
             xy=(1, 94),  # place head of the arrow at point (year 2012 , pop 70)
             xytext=(12, 40),  # place base of the arrow at point (year 2008 , pop 20)
             xycoords='data',  # will use the coordinate system of the object being annotated
             arrowprops=dict(arrowstyle='->', connectionstyle='arc3', color='coral', lw=2)
             )

    # Annotate Text
    plt.annotate('2008 - state with high custmers',  # text to display
             xy=(3.5, 50),  # start the text at at point (year 2008 , pop 30)
             rotation=-27,  # based on trial and error to match the arrow
             va='bottom',  # want the text to be vertically 'bottom' aligned
             ha='left',  # want the text to be horizontally 'left' algned.
             )

    plt.show()

#sum of all transactions for the top 10 customers,and which customer has the highest transaction amount.
def top_ten_customers():
    customer_df_pandas = load_to_customer_pandas()   
    credit_df_pandas = load_to_credit_pandas() 
    top_cust_df = pd.merge(customer_df_pandas,credit_df_pandas,left_on='SSN',right_on='CUST_SSN',how="inner")
    top_cust_df = top_cust_df[["CUST_SSN","TRANSACTION_VALUE","FIRST_NAME","LAST_NAME"]]
    top_cust_df["FULL_NAME"] = top_cust_df["FIRST_NAME"]+" "+top_cust_df["LAST_NAME"]
    top_cust_df = top_cust_df.groupby('FULL_NAME')['TRANSACTION_VALUE'].sum().sort_values(ascending=True).tail(10)
    #top_cust_df.reset_index(inplace = True)
    values = []
    for index, value in enumerate(top_cust_df):
        values.append(value)
    max_value = values[9]
    clrs = ['Teal' if (y < max_value) else 'coral' for y in values ]
    top_cust_df.plot(y = "FULL_NAME", x="TRANSACTION_VALUE", kind = "barh",color= clrs,figsize=(6, 4))
    plt.ylabel("CUSTOMERS")
    plt.xlabel("TRANSACTION_VALUE")
    plt.title("Top 10 customers with highest transaction amount")
    plt.grid(linestyle='dashed', axis="both",zorder=0.99,alpha = 0.15)
    for index, value in enumerate(top_cust_df): 
        label = float(value) # format int with commas
    # place text at the end of bar (subtracting 47000 from x, and 0.1 from y to make it fit within the bar)
        plt.annotate(label, xy=(value - 850, index - 0.2), color='Black')
    
    plt.show()

#plot the top three months with the largest transaction data
def three_months_largest_transaction():
    credit_df_pandas = load_to_credit_pandas() 
    largest_tr_df = credit_df_pandas[["TIMEID","TRANSACTION_VALUE"]]
    largest_tr_df = largest_tr_df.astype({"TIMEID":'datetime64'})
    largest_tr_df['MONTH'] = largest_tr_df['TIMEID'].dt.month_name()
    largest_tr_df.drop(columns = 'TIMEID',inplace= True)
    largest_tr_df = largest_tr_df.groupby('MONTH')['TRANSACTION_VALUE'].sum().sort_values(ascending=False).head(3)
    fig = plt.figure() # create figure
    ax0 = fig.add_subplot(1, 2, 1) # add subplot 1 (1 row, 2 columns, first plot)
    ax1 = fig.add_subplot(1, 2, 2) # add subplot 2 (1 row, 2 columns, second plot). 

    # Subplot 1: Box plot
    largest_tr_df.plot(kind='bar',width = 0.4, color='SteelBlue', figsize=(20, 6), ax=ax0) # add to subplot 1
    for index,value in enumerate(largest_tr_df.values):
        ax0.text(index,value+1, str(value),ha = 'center')
    ax0.set_title('Top three months with the largest transaction data')
    ax0.set_xlabel('Months')
    ax0.set_ylabel('Transaction Value')

    # Subplot 2: Line plot
    largest_tr_df.plot(kind='line',linestyle = "dashed", figsize=(20, 6),marker = 'o',markerfacecolor = "grey", ax=ax1) # add to subplot 2
    #st_cust_df.plot(x= st_cust_df.index, y = st_cust_df.values, kind='line', linestyle = "dashed",figsize=(10, 4),marker = 'o',linewidth = 3,grid = True, \
    #           markerfacecolor = "Black") # pass a tuple (x, y) size
    ax1.set_title ('Top three months with the largest transaction data')
    ax1.set_ylabel('Transaction Value')
    ax1.set_xlabel('Months')
    plt.legend()

    plt.show()

#Converting spark dataframe to pandas dataframe to plot
def loan_to_pandas():
    loan_df = load_loan_from_db()
    loan_df_pandas = loan_df.toPandas()
    return loan_df_pandas

#percentage of applications approved for self-employed applicants.
def approved_self_employeed():
    loan_df_pandas = loan_to_pandas()
    color_list = ['Teal','lightBlue']
    label_value = ['Not Approved','Approved']
    explode_list =[0.1,0]
    df=loan_df_pandas[loan_df_pandas['Application_Status']== 'Y'][['Self_Employed']].value_counts()
    df.plot(kind='pie',
       figsize=(5, 6),
       autopct='%1.1f%%',
       startangle=90,    
       shadow=True,  
       labels = label_value, 
       colors = color_list,
       explode=explode_list   
         )
    plt.title('Percentage of applications approved for self-employed applicants', y=1.12) 
    plt.legend(labels=label_value, bbox_to_anchor=(1.5,1.1),loc='upper right')
    plt.show()

#percentage of rejection for married male applicants.
def married_male_applicants_rejection():
    loan_df_pandas = loan_to_pandas()
    color_list = ['Coral', 'Teal', 'SteelBlue', 'lightskyblue']
    explode_list =[0.1,0,0,0]
    df = loan_df_pandas[['Gender','Married']][loan_df_pandas['Application_Status'] == 'N'].value_counts()
    label_value=['Married Male','Unarried Male','Unmarried Female','Married Feale']
    df.plot(kind='pie',
           figsize=(5, 6),
           autopct='%1.1f%%',
           startangle=30,    
           shadow = True, 
           labels = label_value,    
           colors = color_list,
           explode=explode_list
           )
    plt.title('            percentage of rejection for married male applicants                ', y=1.12) 
    plt.legend(labels=label_value,bbox_to_anchor=(1.5,1.025),loc='upper right') 
    plt.show()


#which branch processed the highest total dollar value of healthcare transactions.
def branch_highest_healthcare_tr():
    credit_df_pandas = load_to_credit_pandas() 
    high_health_df = credit_df_pandas[credit_df_pandas["TRANSACTION_TYPE"]=="Healthcare"].groupby("BRANCH_CODE")\
                    .sum()["TRANSACTION_VALUE"].sort_values(ascending=True).head(10)
    values = []
    for index, value in enumerate(high_health_df):
       values.append(value)
    max_value = values[9]
    clrs = ['LightSeaGreen' if (y < max_value) else 'SteelBlue' for y in values ]
    high_health_df.plot(x=high_health_df.index, y = high_health_df.values,color = clrs,kind = "barh",figsize=(6,4))
    plt.xlabel("BRANCH CODE")
    plt.ylabel("TRANSACTION AMOUNT")
    plt.title("Branch code with highest value for Healthcare")
    for index, value in enumerate(high_health_df): 
        label = int(value)# format int with commas
        # place text at the end of bar (subtracting 47000 from x, and 0.1 from y to make it fit within the bar)
        plt.annotate(label, xy=(value - 250, index - 0.2), color='Black')
    
    plt.show()
    


def function_before_exiting():
    spark.stop()



#Menu functions
def main_menu(): 
    while True:
        print(fontstyle.apply("\n\tMAIN MENU",'bold/Italic/UNDERLINE'))
        print(fontstyle.apply('''\n\t
        1.Transaction Details
        2.Customer Details
        3.Data Visualization
        4.Exit''','Italic'))

        front_end_choice = pyinput.inputInt(fontstyle.apply("\n\tPlease enter your choice from main now: ",'Italic'))
        if front_end_choice == 1:
            transaction_details_menu()
        elif front_end_choice == 2:
            customer_details_menu()
        elif front_end_choice == 3:
            data_visualization_menu()
        elif front_end_choice == 4:
            print("\tExiting from main menu.....")
            function_before_exiting()
            break
        else:
            print("\tPlease enter valid choice.....")


def transaction_details_menu():
    print(fontstyle.apply("\n\tYou have chosen to see transaction details.....",'Italic'))
    while True:
        print(fontstyle.apply('''\n\tPlease select from the below option: 
        1. Transactions made by customers - given zip_code, month and year 
        2. Display the number and total transaction value - given type
        3. Display the total number and transaction value for branches - given state
        4. Go back to main menu(Exit from Transaction details module)
        5. Exit from main menu''','Italic'))

        user_choice = pyinput.inputInt(fontstyle.apply("\n\tEnter your choice from transaction menu: ",'Italic'))  
        if user_choice == 1:
            transaction_for_zipcode()
        elif user_choice == 2:
            transaction_value_by_number()
        elif user_choice == 3:
            transaction_branch_state()
        elif user_choice == 4:
            print(fontstyle.apply("\n\tExiting from Transaction Details.....",'Italic'))
            print(fontstyle.apply("\n\tGoing back to main menu.....",'Italic'))
            break
        elif user_choice == 5:
            print(fontstyle.apply("\nExiting from main menu",'Italic'))
            function_before_exiting()
            exit()
        else:
            print("\tPlease enter valid choice...... ")
    
    
def customer_details_menu():
    print(fontstyle.apply("\n\tYou have chosen to see customer details.....",'Italic'))
    
    while True:
        print(fontstyle.apply('''\n\tPlease select from the below option: 
        1. Check existing account details of a customer
        2. Modify the existing account details of a customer
        3. Display Monthly bill for a credit card number - given month and year
        4. Display the transactions made by a customer between two dates
        5. Go back to front-end-module (Exit from Customer details module)
        6. Exit from Front-end menu''','Italic'))
        user_choice = pyinput.inputInt(fontstyle.apply("\n\tEnter your choice from customer details menu: ",'Italic'))   
        if user_choice == 1:
            account_details()
        elif user_choice == 2:
            modify_customer()
        elif user_choice == 3:
            monthly_bill()
        elif user_choice == 4:
            transactions_by_dates()
        elif user_choice == 5:
            print(fontstyle.apply("\n\tExiting from Customer Details.....",'Italic'))
            break
        elif user_choice == 6:
            print(fontstyle.apply("\n\tExiting from main menu",'Italic'))
            function_before_exiting()
            exit()
        else:
            print(fontstyle.apply("\tPlease enter a valid choice...... ",'Italic'))

def data_visualization_menu():
    print(fontstyle.apply("\n\tYou have chosen to see data visualization.....",'Italic'))
    
    while True:
        print(fontstyle.apply('''\n\tPlease select from the below option: 
        1. Transaction type that has a high rate oftransactions
        2. State that has a high number of customers
        3. Top 10 customers,and the customer that has the highest transaction amount
        4. Percentage of applications approved for self-employed applicants - LOAN Dataset
        5. percentage of rejection for married male applicants - LOAN Dataset
        6. Top three months with the largest transaction data
        7. branch processed the highest total dollar value of healthcare transactions
        8. Go back to main menu (Exit from Customer details module)
        9. Exit from Front-end menu''','Italic'))
        user_choice = pyinput.inputInt(fontstyle.apply("\n\tEnter your choice from data visualization menu: ",'Italic'))   
        if user_choice == 1:
            high_transaction_rate_type()
        elif user_choice == 2:
            high_customers_state()
        elif user_choice == 3:
            top_ten_customers()
        elif user_choice == 4:
            approved_self_employeed()
        elif user_choice == 5:
            married_male_applicants_rejection()
        elif user_choice == 6:
            three_months_largest_transaction()
        elif user_choice == 7:
            branch_highest_healthcare_tr()
        elif user_choice == 8:
            print(fontstyle.apply("\n\tExiting from Customer Details.....",'Italic'))
            break
        elif user_choice == 9:
            print(fontstyle.apply("\n\tExiting from main menu",'Italic'))
            function_before_exiting()
            exit()
        else:
            print(fontstyle.apply("\tPlease enter a valid choice...... ",'Italic'))



if __name__ == "__main__":      
    print(fontstyle.apply(" \t\t CAPSTONE PROJECT \n\n",'bold/UNDERLINE'))

    print("\tCREDITCARD SYSTEM DATABASE \n\n")
    # Log that you have started the ETL process
    print("\tETL Job Started \n")

    # Log that you have started the Extract step
    print("\tExtract phase Started \n")

    # Call the Extract function
    extracted_customer_data = customer_extract()
    extracted_branch_data = branch_extract()
    extracted_credit_data = credit_pandas_extract()

    #xLog that you have completed the Extract step
    print("\n\tExtract phase Ended")

    # Log that you have started the Transform step
    print("\n\tTransform phase Started \n")

    # Call the Transform function
    customer_transformed_data = customer_transform(extracted_customer_data)
    branch_transformed_data = branch_transform(extracted_branch_data)
    credit_transformed_data = credit_transform(extracted_credit_data)

    # Log that you have completed the Transform step
    print("\tTransform phase Ended \n")

    # Log that you have started the Load step
    print("\tLoad phase Started \n")

    # Call the Load function
    customer_load(customer_transformed_data)
    branch_load(branch_transformed_data)
    credit_load(credit_transformed_data)


    # Log that you have completed the Load step
    print("\tLoad phase Ended \n")

    # Log that you have completed the ETL process
    print("\tETL Job Ended \n")

    #loan-api-module
    print ("\n\tLOAN application Data API")
    loan_df= loan_app()
    loan_load(loan_df)

    main_menu()


    

