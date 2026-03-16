# =============================================================================
# PySpark / Databricks Data Engineering Framework
# =============================================================================
# PURPOSE  : Generic reusable ingestion & transformation utilities for
#            Databricks / PySpark pipelines.  Supports on-prem JDBC, Azure
#            Blob Storage, Snowflake, Salesforce, REST APIs, and Unity Catalog.
#
# SANITISED : All client-specific names, server addresses, secret keys, API
#             URLs, and organisation identifiers have been replaced with
#             generic placeholders (e.g. <SECRET_SCOPE>, <YOUR_SERVER>,
#             env_dev / env_uat / env_prd).
#
# COMMENTED-OUT SECTIONS
#   - Module-level SparkSession / dbutils initialisation  (requires Databricks)
#   - Secrets retrieval block                             (requires Databricks)
#   - Environment variable assignment block               (requires Databricks)
#   These blocks are preserved for reference and can be re-enabled once a
#   Databricks workspace is available.
#
# TODO / SETUP GUIDE  →  see notes/framework_progress.md
# =============================================================================

# Databricks notebook source
import sys, datetime, json, pytz, os, re, math, requests, io
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime,timedelta
from time import sleep
from pyspark.sql import functions as F
import pandas as pd
import hashlib
from azure.storage.blob import BlobServiceClient
from simple_salesforce import Salesforce, SalesforceLogin, format_external_id
import snowflake.connector
from delta.tables import *
from databricks import sql
import boto3
import splunklib
import splunklib.client as client
import splunklib.results as results
import xml.etree.ElementTree as ET
from pyspark.context import SparkContext

# COMMAND ----------

# MAGIC %md
# MAGIC ## GLOBAL Functions

# COMMAND ----------

# MAGIC %md
# MAGIC def dml_operation_df(source_df, target_df, pk_columns, mode, compare_columns=None):
# MAGIC
# MAGIC def GetValueFromDataframe(_df,columnName):
# MAGIC
# MAGIC def GetTime(timezone = 'Chicago'):
# MAGIC
# MAGIC def julian_to_timestamp(julian_date):
# MAGIC
# MAGIC def epoch_to_datetime(epoch_time):
# MAGIC
# MAGIC def col_rename(df,pattern = "[^a-zA-Z0-9_]", replace = ""):
# MAGIC
# MAGIC def truncate_dataframe(df: DataFrame) -> DataFrame:
# MAGIC
# MAGIC def column_clear(df, col_name):
# MAGIC
# MAGIC def uppercase_columns(df: DataFrame) -> DataFrame:
# MAGIC
# MAGIC def trim_column_values(spark_dataframe: DataFrame, column_name: str, max_length: int)
# MAGIC
# MAGIC def find_and_replace(df, find, replace):
# MAGIC
# MAGIC def column_retitle(df, reference_df):
# MAGIC
# MAGIC def df_column_rename(df, old_list, new_list):
# MAGIC
# MAGIC def add_column_prefix(df, prefix):
# MAGIC
# MAGIC def add_prefix_suffix(df, include_dtypes = ['string','timestamp'], concat_string = '"', include_header = "false"):
# MAGIC
# MAGIC def to_string_datatype(df):
# MAGIC
# MAGIC def generate_update_setString(column_list, update_query_keys):
# MAGIC
# MAGIC def get_notebook_name():
# MAGIC
# MAGIC def remove_null_from_dictionary(incoming_records):
# MAGIC
# MAGIC find_and_replace_within_values(df_regex, find, replace):
# MAGIC
# MAGIC def time_delay(duration, time_unit):

# COMMAND ----------

# DBTITLE 1,Global Function
# NOTE: Module-level SparkSession creation is commented out — requires a
#       running Spark/Databricks environment.  Uncomment when running inside
#       Databricks or after configuring a local PySpark session.
# spark = SparkSession.builder.getOrCreate()

##############################################################################################
#Needed to set spark config
##############################################################################################
def set_spark_configs():
    spark.conf.set("spark.sql.ansi.enabled", "true")
    #print("spark.sql.ansi.enabled is set to true")
    return 

###################################################################################################################################
# Needed to make the Wheel File 
###################################################################################################################################
def get_dbutils(spark):
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
        except ImportError:
            import IPython
            dbutils = IPython.get_ipython().user_ns["dbutils"]
        return dbutils

# NOTE: dbutils is only available inside Databricks.  The lines below are
#       commented out so the module can be imported / inspected locally.
# dbutils = get_dbutils(spark)
# spark  = SparkSession.builder.getOrCreate()

###################################################################################################################################
# Function to get a specific value from a table
###################################################################################################################################
def GetValueFromDataframe(_df,columnName):
    """
    Function to get a specific value from a table
    :df: single row dataframe that needs to be passed
    :columnName: specifc column name
    :return: A specific value from a column and row
    """
    for row in _df.collect():       
        return row[columnName].strip()
      
###################################################################################################################################
# Function called to get the CST start time of read and write operations 
###################################################################################################################################
def GetTime(timezone = 'Chicago', strform = "%H:%M:%S"):
    """
    Function to get the current time of a city in America
    :timezone: Any city in America eg. New_York
    :return: Current time
    """
    tz_CST = pytz.timezone('America/'+ timezone)
    datetime_CST = datetime.now(tz_CST)
    current_time = datetime_CST.strftime(strform)
    return current_time

###################################################################################################################################
# Function to convert Julian date to formatted timestamp
###################################################################################################################################
def julian_to_timestamp(julian_date):
    timestamp_with_ms = julian_date
    dt = datetime.fromtimestamp(timestamp_with_ms / 1000)
    formatted_time = dt.strftime('%Y-%m-%d %H:%M:%S.%f')
    return formatted_time


###################################################################################################################################
# Function to convert epoch date to formatted timestamp
###################################################################################################################################
def epoch_to_datetime(epoch_time):
    format_str="%Y-%m-%d %H:%M:%S"
    dt = datetime.fromtimestamp(epoch_time)
    formatted_dt = dt.strftime(format_str)
    return formatted_dt
  
###################################################################################################################################
# Loop through the column names and remove special characters and spaces with whatever character you want or "" for nothing
###################################################################################################################################
def col_rename(df,pattern = "[^a-zA-Z0-9_]", replace = ""):
    """
    Function to rename column names and remove special characters and spaces with whatever character you want or "" for nothing
    :df: dataframe that needs to be passed
    :pattern: Anything that will be accepted
    :replace: Can also be "_"
    :return: df with renamed columns
    """
    for col_name in df.columns:
        new_col_name = re.sub(pattern, replace , col_name)
        df = df.withColumnRenamed(col_name, new_col_name)
    return df



def truncate_dataframe(df: DataFrame) -> DataFrame:
    """
    Truncate a PySpark DataFrame by creating an empty DataFrame with the same schema.

    :param df: Input PySpark DataFrame
    :return: Empty PySpark DataFrame with the same schema as the input DataFrame
    """
    return df.sparkSession.createDataFrame([], df.schema)



def column_clear(df, col_name):
    """
    Clear out the data in the specified column of a PySpark DataFrame.

    Args:
        df (pyspark.sql.DataFrame): The input DataFrame.
        col_name (str): The name of the column to clear out.

    Returns:
        pyspark.sql.DataFrame: The modified DataFrame with the specified column cleared out.
    """
    # Use the `withColumn` method to replace the column with a new column filled with null values
    df = df.withColumn(col_name, lit(None).cast(df.schema[col_name].dataType))
    
    return df

def uppercase_columns(df: DataFrame) -> DataFrame:
    """
    Uppercase all column names of a PySpark DataFrame.

    :param df: Input PySpark DataFrame
    :return: PySpark DataFrame with uppercase column names
    """
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, col_name.upper())
    return df

def trim_column_values(spark_dataframe: DataFrame, column_name: str, max_length: int) -> DataFrame:
    """
    Trims the values in the specified column of the spark_dataframe if they exceed the max_length.

    Args:
        spark_dataframe (pyspark.sql.DataFrame): The PySpark DataFrame with the column to trim.
        column_name (str): The name of the column to trim.
        max_length (int): The maximum number of characters allowed for each value in the column.

    Returns:
        pyspark.sql.DataFrame: The PySpark DataFrame with the trimmed column values.
    """

    if column_name not in spark_dataframe.columns:
        raise ValueError(f"Column '{column_name}' not found in the DataFrame.")

    # Define a UDF (User Defined Function) to trim the column values
    @udf(returnType=StringType())
    def trim_value(value: str) -> str:
        if value is not None and len(value) > max_length:
            return value[:max_length]
        else:
            return value

    trimmed_df = spark_dataframe.withColumn(column_name, trim_value(col(column_name)))

    return trimmed_df 

def find_and_replace(df, find, replace):
    """
    Replace all values that are 'None' with nulls in the dataframe.
    :param df: Input PySpark dataframe.
    :param find: what needs to be replaced
    :param replace: what you want to replace with 
    :return: Dataframe with replaced values
    """
    return df.replace(find, replace)

def column_retitle(df, reference_df):
    """
    Retitle columns of a dataframe based on a reference dataframe.
    :param df: Input PySpark dataframe whose columns you want to retitle.
    :param reference_df: Input PySpark dataframe as a reference to retitle column names
    """
    # Get the column names from the reference DataFrame
    reference_columns = reference_df.columns

    # Rename the columns in the input DataFrame
    renamed_df = df.select([col(column).alias(reference_columns[index]) for index, column in enumerate(df.columns)])

    return renamed_df

###################################################################################################################################
# Function to rename existing columns of given dataframe with new list of columns
###################################################################################################################################
def df_column_rename(df, old_list, new_list):    
    """
    Function to rename existing columns of given dataframe with new list of columns
       Parameters:
       :df (dataframe): Input dataframe on which column rename will be occured,
       :old_list (list): Existing column list in dataframe,
       :new_list (list): New column list which needs to replacce old column list
       Returns:
       :df (dataframe): Returning value is dataframe with new column name
       Note: Position of new column name should be equal to old column in their respective lists.
    """
    if len(old_list) == len(new_list):
        for (old,new) in zip(old_list, new_list):
            df = df.withColumnRenamed(old, new)
    else:
        raise TypeError("Please check the number of columns in old and new list. Both should be having equal number")
    return df

###################################################################################################################################
# Function to add prefix to the dataframe column names
###################################################################################################################################
def add_column_prefix(df, prefix):
    """Function to add prefix to the dataframe column names
       Parameters:
       :df (dataframe): Input dataframe on which column names will be renamed with prefixed string value,
       :prefix (string): It is string value, which get prefixed to all the column names in dataframe,       
       Returns:
       :df_new (dataframe): Returning value is dataframe with new column names
    """
    old_columns = df.columns
    new_columns = [prefix+old for old in old_columns]
    df_new = df_column_rename(df, old_columns, new_columns)
    return df_new

###################################################################################################################################
# Function to add constant value as suffix and prefix to the required column values
###################################################################################################################################
def add_prefix_suffix(df, include_dtypes = ['string','timestamp'], concat_string = '"', include_header = "false"):
    """
    Adds a constant value as a suffix and prefix to the values of specific columns in a DataFrame.
    
    Parameters:
    :param df (DataFrame): The input DataFrame with the columns to be updated.
    :param include_dtypes (list[str]): A list of column data type(s) to be included for addition of prefix/suffix values.
                                       Default values are 'string' and 'timestamp'.
    :param concat_string (str): The string value to be added as a prefix and suffix. Default is double-quote (").
    :param include_header (str): If "True", column headers will also have the prefix/suffix added. 
                                 Default is "False". Accepts either "True" or "False" as a string.

    Returns:
    :return df (DataFrame): The DataFrame with constant prefix/suffix values added.
    """
    if (include_header.lower() == "true"):
        for h_column in df.columns:
            df = df.withColumnRenamed(h_column, str(concat_string+h_column+concat_string))
    for col_name,d_type in df.dtypes:
        if any(include_dtype in d_type for include_dtype in include_dtypes):
            if d_type == "string":
                df = df.withColumn(col_name, concat(lit(concat_string), col(col_name), lit(concat_string)))
            else:
                df = df.withColumn(col_name, col(col_name).cast(StringType())) \
                       .withColumn(col_name, concat(lit(concat_string), col(col_name), lit(concat_string)))                  
    return df
    
###################################################################################################################################
# Function to convert all the datatypes of dataframe(df) to StringType()
###################################################################################################################################
def to_string_datatype(df):
    """Function to convert all the datatypes of dataframe(df) to StringType()
    """    
    for col_name, d_type in df.dtypes:
        if d_type != 'string':
            df = df.withColumn(col_name, col(col_name).cast(StringType()))      
    return df

###################################################################################################################################
# Function to generate update set string
###################################################################################################################################
def generate_update_setString(column_list, update_query_keys):
    """Function to generate update set string      
       Parameters:
       :column_list (list): Contains the list of columns, which are required to SQL update set and where condition,
       :update_query_keys (list): Contains the list of key columns, which are required to SQL update where condition,       
       Returns:
       :set_string (string): string contains the SQL formatted Query
    """
    set_string = ""
    where_string= " where "    
    for key_column in update_query_keys:        
        if key_column in column_list:
            column_list.remove(key_column)
        else:
            raise TypeError("Key column not present in the column_list")
    if (len(column_list) >= 1) and (len(update_query_keys) >= 1):
        for set_column in column_list:
            set_string += set_column+"=$${"+set_column+"}$$,"
        for key_column in update_query_keys:
            where_string += key_column+"=$${"+key_column+"}$$ and "
    else:
        raise TypeError("Dataframe should contains atleast two columns to proceed with update command. One or more columns will be used in where condition and rest will be in set condition")
    set_string = set_string[:-1]
    where_string = where_string[:-5]
    set_string += where_string
    return set_string

###########################################################################################################################################################
# Function to get the notebook name
###########################################################################################################################################################
def get_notebook_name():
    """Function to get the notebook name
    """    
    notebook_path, notebook_name = '',''    
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    notebook_name = (notebook_path.split('/')[-1]).split('.')[0]
    return notebook_name

###########################################################################################################################################################
# Function to remove null values from a given dictionary
###########################################################################################################################################################

def remove_null_from_dictionary(incoming_records):
    """Function to remove null values from a given dictionary
        Parameters:
            :incoming_records (list): Contains the list dictionary records with null values,
        Returns:
            :transformed_records (list): Contains the list dictionary records without null values.   
    """
    transformed_records = []
    for dict_record in incoming_records:
        transformed_record = {key:value for key,value in dict_record.items() if value != None}
        transformed_records.append(transformed_record)        
    return transformed_records

###########################################################################################################################################################
# Function to find and replace the substring values
###########################################################################################################################################################
def find_and_replace_within_values(df_regex, find, replace):
    for column in df_regex.columns:
        if (df_regex.schema[column].dataType) == StringType():
            df_regex = df_regex.withColumn(column, regexp_replace(col(column), find, replace))
    return df_regex

###########################################################################################################################################################
# Function to add a time delay
###########################################################################################################################################################
def time_delay(duration, time_unit):
    """
    Delays the process for a given duration and time unit.

    :param duration: int - The amount of time to delay.
    :param time_unit: str - The time unit for the duration. Acceptable values are 'hr', 'min', 'sec', 'ms'.
    """
    time_unit= time_unit.lower()
    if time_unit == 'hr':
        delay_seconds = duration * 3600  # 1 hour = 3600 seconds
    elif time_unit == 'min':
        delay_seconds = duration * 60    # 1 minute = 60 seconds
    elif time_unit == 'sec':
        delay_seconds = duration         # 1 second = 1 second
    elif time_unit == 'ms':
        delay_seconds = duration / 1000  # 1 millisecond = 0.001 second
    else:
        raise ValueError("Invalid time unit. Choose from 'hours', 'minutes', 'seconds', or 'milliseconds'.")
    print(f"Delay of {duration} {time_unit}")
    sleep(delay_seconds)


def dml_operation_df(source_df, target_df, pk_columns, mode, compare_columns=None):
    """
    Process source and target DataFrames based on the specified mode.
    
    :param source_df: The source DataFrame
    :param target_df: The target DataFrame
    :param pk_columns: List of column names that form the primary key
    :param mode: Operation mode ('insert', 'update', or 'delete')
    :param compare_columns: List of column names to compare (used only in 'update' mode)
    :return: A new DataFrame based on the specified mode
    """
    # Check if pk_columns is a list and not empty
    if not isinstance(pk_columns, list) or len(pk_columns) == 0:
        raise ValueError("pk_columns must be a non-empty list of column names")

    # Check if all pk_columns exist in both source_df and target_df
    source_columns = set(source_df.columns)
    target_columns = set(target_df.columns)
    missing_pk_source = set(pk_columns) - source_columns
    missing_pk_target = set(pk_columns) - target_columns

    if missing_pk_source:
        raise ValueError(f"Primary key column(s) {missing_pk_source} not found in source DataFrame")
    if missing_pk_target:
        raise ValueError(f"Primary key column(s) {missing_pk_target} not found in target DataFrame")

    # Ensure primary key columns are not null in both DataFrames
    condition = ' AND '.join([f'{column} IS NOT NULL' for column in pk_columns])
        
    try:
        source_df = source_df.filter(condition)
        target_df = target_df.filter(condition)
    except Exception as e:
        raise ValueError(f"Error filtering DataFrames: {str(e)}")

    print("Executing: " + mode)

    if mode == 'insert':
        try:
            result_df = source_df.join(target_df, pk_columns, "left_anti")
        except Exception as e:
            raise ValueError(f"Error performing left anti join for insert mode: {str(e)}")

    elif mode == 'update':
        if compare_columns is None:
            compare_columns = [c for c in source_df.columns if c not in pk_columns]
        else:
            # Check if all compare_columns exist in both source_df and target_df
            missing_compare_source = set(compare_columns) - source_columns
            missing_compare_target = set(compare_columns) - target_columns

            if missing_compare_source:
                raise ValueError(f"Compare column(s) {missing_compare_source} not found in source DataFrame")
            if missing_compare_target:
                raise ValueError(f"Compare column(s) {missing_compare_target} not found in target DataFrame")

        try:
            joined_df = source_df.alias('source').join(target_df.alias('target'), pk_columns, "inner")

            conditions = [
                (col(f"source.{c}") != col(f"target.{c}")) |
                (col(f"source.{c}").isNull() & col(f"target.{c}").isNotNull()) |
                (col(f"source.{c}").isNotNull() & col(f"target.{c}").isNull())
                for c in compare_columns
            ]

            change_condition = conditions[0]
            for condition in conditions[1:]:
                change_condition = change_condition | condition

            result_df = joined_df.withColumn(
                "flag",
                when(change_condition, "update").otherwise("ignore")
            )

            select_expr = [f"source.{c}" for c in source_df.columns] + ["flag"]
            result_df = result_df.select(*select_expr)

        except Exception as e:
            raise ValueError(f"Error performing update comparison: {str(e)}")

    elif mode == 'delete':
        try:
            target_df = target_df.select(pk_columns)
            result_df = target_df.join(source_df, pk_columns, "inner")
        except Exception as e:
            raise ValueError(f"Error performing inner join for delete mode: {str(e)}")

    else:
        raise ValueError("Invalid mode. Must be 'insert', 'update', or 'delete'.")

    return result_df

###########################################################################################################################################################
# Function to calculate dataframe size
###########################################################################################################################################################
def calculate_df_size(df):
    """
    Calculates the approximate size of a DataFrame in bytes.
    Uses column data types to estimate size without using deprecated RDD or pandas operations.
    
    Args:
        df (DataFrame): Spark DataFrame to estimate size for
        
    Returns:
        int: Estimated size in bytes
    """
    # Get row count
    row_count = df.count()
    
    if row_count == 0:
        return 0
    
    # Simple size estimations by data type
    row_size = 0
    for field in df.schema.fields:
        data_type = str(field.dataType)
        
        # Assign sizes based on data type
        if 'StringType' in data_type:
            # For string columns, use fixed average size
            # Real strings vary in length, but 40 bytes is a reasonable average
            row_size += 40
        elif 'IntegerType' in data_type:
            row_size += 4
        elif 'LongType' in data_type or 'DoubleType' in data_type:
            row_size += 8
        elif 'BooleanType' in data_type:
            row_size += 1
        elif 'DateType' in data_type:
            row_size += 4
        elif 'TimestampType' in data_type:
            row_size += 8
        elif 'BinaryType' in data_type:
            # Binary types vary widely, use a reasonable default
            row_size += 50
        elif 'DecimalType' in data_type:
            row_size += 16
        elif 'ArrayType' in data_type or 'MapType' in data_type or 'StructType' in data_type:
            # Complex types - use a reasonable fixed size estimate
            row_size += 64
        else:
            # Default for any other types
            row_size += 16
    
    # Add per-row overhead
    row_size += 16
    
    # Calculate total size
    total_size = row_count * row_size
    
    # Add overhead for Spark internal structures (30%)
    return int(total_size * 1.3)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Informatica Functions
# MAGIC
# MAGIC def infa_router(df, conditions):
# MAGIC
# MAGIC def infa_joiner(left_df: DataFrame, right_df: DataFrame, join_type: str, join_keys: list) -> DataFrame:
# MAGIC
# MAGIC def md5_checksum(df_md5, column_list, checksum_name, date_format_string = "yyyyMMdd HH:mm:ss", delimiter = "|"):
# MAGIC
# MAGIC def generate_insert_update_delete_flags(df_flag, flag_dictionary):
# MAGIC
# MAGIC def generate_insert_update_delete_dataframes(df, dataframe_dictinary):
# MAGIC
# MAGIC def lookup_dataframe(df, df_lkp, src_key_columns, lkp_key_columns, lkp_return_columns, on_match = 'anyone_row'):

# COMMAND ----------

###################################################################################################################################
# INFORMATICA FUNCTIONS 
# Router function
###################################################################################################################################
def infa_router(df, conditions):
    """
    Filters and routes data in a PySpark DataFrame based on specified conditions.

    Parameters:
        df (pyspark.sql.DataFrame): Input DataFrame to filter and route
        conditions (list): A list of tuples containing the condition expressions and their corresponding output DataFrame names.
                           Each tuple should contain the condition expression as a string and the output DataFrame name as a string.

    Returns:
        df_array: A list containing the output DataFrames for each condition expression.
    """
    df_array = []
    for condition in conditions:
        output_df = df.filter(expr(condition))
        df_array.append(output_df)
    return df_array

###################################################################################################################################
# Joiner function 
###################################################################################################################################
def infa_joiner(left_df: DataFrame, right_df: DataFrame, join_type: str, join_keys: list) -> DataFrame:
    """
    Function to join two DataFrames in PySpark
    
    Parameters:
    left_df: Left DataFrame
    right_df: Right DataFrame
    join_type: Join type ("inner", "outer", "left", or "right")
    join_keys: List of join keys
    
    Returns:
    Resulting joined DataFrame
    """
    
    # Perform the join
    joined_df = left_df.join(right_df, join_keys, join_type)
    
    # Return the resulting DataFrame
    return joined_df

###################################################################################################################################
# Function to generate md5_checksum values to given set of columns in a dataframe
###################################################################################################################################
def md5_checksum(df_md5, column_list, checksum_name, date_format_string = "yyyyMMdd HH:mm:ss", delimiter = "|"):
    """Function to generate md5_checksum values to given set of columns in a dataframe
       Parameters:
       :df_md5 (dataframe): Input dataframe which contains the list of columns required for md5_checksum,
       :column_list (list): Column_list contains the list of names which are included in md5_checksum,
       :date_format_string (string): Format to which timestamp or date get convert as string. Default value is 'yyyyMMdd HH:mm:ss'
       :delimiter (string): Used for delimiting column values in concat function. Default value is '|'
       
       Returns:
       :df_md5 (dataframe): Returning value is dataframe with md5_chechsum values in new column(checksum_name) 
    """
    md5_colname = ''
    md5_column_list = []
    colnames_dtypes = df_md5.dtypes
    # converting non string datatype values to string datatype    
    for colname,dtype in colnames_dtypes:
        if colname in column_list:
            md5_colname = 'MD5_'+colname        
            if (dtype == 'string'):
                df_md5 = df_md5.withColumn(md5_colname, df_md5[colname])                
            elif (dtype in ['timestamp','date']):
                df_md5 = df_md5.withColumn(md5_colname, date_format(df_md5[colname], date_format_string))
            else:
                df_md5 = df_md5.withColumn(md5_colname, df_md5[colname].cast(StringType()))    
    md5_column_list = ['MD5_'+colname for colname in column_list]
    df_md5 = df_md5.withColumn(checksum_name, md5(concat_ws(delimiter, *(df_md5[md5_column_list])).cast('BINARY')))
    df_md5 = df_md5.drop(*md5_column_list)
    return df_md5

###################################################################################################################################
# Function to generate insert, update and delete flags for respective records based on the conditions
###################################################################################################################################
def generate_insert_update_delete_flags(df_flag, flag_dictionary):
   """Function to generate insert, update and delete flags for respective records based on the conditions    
       Parameters:
       :df_flag (dataframe): Input dataframe which contains key and checksum columns,
       :flag_dictionary (dictionary): Contains the set of column values in the form of below string and list dictionary keys,
       :src_key_column (string): Source data key column name
       :tgt_key_column (string): Destination data key column name
       :src_checksum_name (string) : Source data md5_checksum column name
       :tgt_checksum_name (string) : Destination data md5_checksum column name
       :tgt_change_flag (string) : Destination change flag column name
       :flag_name (list) : list contains the flag colmun names to be added to input dataframe after generating flag logic's.
                          Column names always in order of (Insert, Update, Delete) flag
       :flag_values (list) : list contains the include and exclude flag values ex: [1,0] or ['Y','N']       
       Returns:
       :df_flag (dataframe): Returning value is dataframe with Insert, Update and Delete flag columns.
   """
   src_key_column = flag_dictionary["src_key_column"]
   tgt_key_column = flag_dictionary["tgt_key_column"]
   src_checksum_name = flag_dictionary["src_checksum_name"]
   tgt_checksum_name = flag_dictionary["tgt_checksum_name"]
   tgt_change_flag = flag_dictionary["tgt_change_flag"]
   flag_names = flag_dictionary["flag_names"]
   flag_values = flag_dictionary["flag_values"]

   if len(flag_names) == 3:
      insert_flag = flag_names[0]
      update_flag = flag_names[1]
      delete_flag = flag_names[2]
   else:
      raise TypeError("Please check the number of columns in flag_names list. Total should be equal to three")
   if len(flag_values) == 2:
      include_flag = flag_values[0]
      exclude_flag = flag_values[1]
   else:
      raise TypeError("Please check the number of columns in flag_values list. Total should be equal to two")
   # generating insert flag
   df_flag = df_flag.withColumn(insert_flag, F.when(df_flag[tgt_key_column].isNull(),include_flag).otherwise(exclude_flag))
   # generating update flag
   df_flag = df_flag.withColumn(update_flag, F.when((df_flag[src_key_column].isNotNull() & df_flag[tgt_key_column].isNotNull()) & (~(df_flag[src_checksum_name] == df_flag[tgt_checksum_name]) | (df_flag[tgt_change_flag] == 'D')),include_flag).otherwise(exclude_flag))
   # generating delete flag
   df_flag = df_flag.withColumn(delete_flag, F.when((df_flag[src_key_column].isNull() & df_flag[tgt_key_column].isNotNull()) & (df_flag[tgt_change_flag] != 'D'),include_flag).otherwise(exclude_flag))
   return df_flag

###################################################################################################################################
# Function to generate insert, update and delete dataframes based on filter conditions
###################################################################################################################################
def generate_insert_update_delete_dataframes(df, dataframe_dictionary):   
   """Function to generate insert, update and delete dataframes based on filter conditions    
       Parameters:
       :df (dataframe): Input dataframe which contains required source columns, Insert, Update, and Delete flag columns,
       :dataframe_dictionary (dictionary): Contains the set of column values in the form of below string and list dictionary keys,
       :src_columns (list) : Contains the source column names which are required to Insert and Update data to destination table,
       :src_key_column (string): Source data key column name
       :tgt_key_column (string): Destination data key column name
       :flag_name (list) : list contains the flag colmun names to be added to input dataframe after generating flag logic's.
                          Column names always in order of (Insert, Update, Delete) flag
       :flag_values (list) : list contains the include and exclude flag values ex: [1,0] or ['Y','N']       
       Returns:
       :df_list (list): Returning value is dataframe lists with Insert, Update and Delete dataframes.
   """
   src_columns = dataframe_dictionary["src_columns"]
   src_key_column = dataframe_dictionary["src_key_column"]
   tgt_key_column = dataframe_dictionary["tgt_key_column"]
   flag_names = dataframe_dictionary["flag_names"]
   flag_values = dataframe_dictionary["flag_values"]

   if len(flag_names) == 3:
      insert_flag = flag_names[0]
      update_flag = flag_names[1]
      delete_flag = flag_names[2]
   else:
      raise TypeError("Please check the number of columns in flag_names list. Total should be equal to three")
   include_flag = flag_values[0]
   # generating insert dataframe
   df_insert = df.filter(df[insert_flag] == include_flag).select(*src_columns)
   # generating update dataframe
   df_update = df.filter(df[update_flag] == include_flag).select(*src_columns)
   # generating delete dataframe
   df_delete = df.filter(df[delete_flag] == include_flag).select(tgt_key_column)
   df_delete = df_delete.withColumnRenamed(tgt_key_column, src_key_column)
   df_list = [df_insert, df_update, df_delete]   
   return df_list

###################################################################################################################################
# Dataframe Lookup function 
###################################################################################################################################
def lookup_dataframe(df, df_lkp, src_key_columns, lkp_key_columns, lkp_return_columns, on_match = 'anyone_row'):
    """Lookup Dataframe function
        Prerequisite(Important): The src_key_columns and lkp_key_columns names should not be the having same colummn names to compare.
        Parameters:
        :df (dataframe): Source dataframe which has the key columns to look up on another dataframe
        :df_lkp (dataframe): Lookup dataframe which contains the look up data
        :src_key_columns (list): List of columns which are required for join condition from source dataframe
        :lkp_key_columns (list): List of columns which are required for join condition from lookup dataframe
        :lkp_return_columns (list): List of column names of lookup dataframe only and these columns will be added to all source dataframe columns
        :on_match (string): String value specifies whether anyone row or all rows needs to be included on match condition.
                            Default value is 'anyone_row', if all rows needs to be included need to pass 'all_rows' value
        Return:
        :df_join (dataframe): Return dataframe contains the all the columns of source dataframe and required return columns from the lookup dataframe.
    """    
    if len(src_key_columns) == len(lkp_key_columns):        
        row_num = 'lkp_row_num'
        join_type = 'left'
        total_return_columns = (df.columns)+lkp_return_columns
        condition_list = [col(src) == col(lkp) for (src,lkp) in zip(src_key_columns, lkp_key_columns)]        
        if (on_match.lower() == 'anyone_row'):
            # generating row number to joined dataframe
            window_condition  = Window.partitionBy(lkp_key_columns).orderBy(lkp_key_columns)
            df_lkp = df_lkp.withColumn(row_num, row_number().over(window_condition))
            df_lkp = df_lkp.filter(col(row_num) == 1).drop(row_num)
            # Left joining the two dataframes            
            df_join = df.join(df_lkp, condition_list, join_type).select(*total_return_columns)

        elif (on_match.lower() == 'all_rows'):
            # Left joining the two dataframe
            df_join = df.join(df_lkp, condition_list, join_type).select(*total_return_columns)
        else:
            raise TypeError("Please enter valid on_match value")
    else:
        raise TypeError("Source and Lookup condition columns count did not match!")
    return df_join

# COMMAND ----------

# MAGIC %md
# MAGIC #### Regression Testing
# MAGIC
# MAGIC def compare_schemas(df1, df2, df1_name = 'df1', df2_name = 'df2'):
# MAGIC
# MAGIC def hash_comp(df1,df2):
# MAGIC
# MAGIC def compare_dataframes(df1, df2):
# MAGIC
# MAGIC def check_for_special_characters(df):

# COMMAND ----------

# DBTITLE 1,REGRESSION TESTING
def compare_schemas(df1, df2, df1_name = 'df1', df2_name = 'df2'):
    """
    Compares schemas of 2 PySpark DataFrames.

    Parameters:
        df1 (pyspark.sql.DataFrame): Input DataFrame1 
        df2 (pyspark.sql.DataFrame): Input DataFrame2 

    Returns:
        Schema differences as the output
    """
    schema1 = df1.schema
    schema2 = df2.schema
    schemas_are_equal = True

    if schema1 == schema2:
        print("Schemas are equal")
    else:
        fields1 = set((f.name, f.dataType) for f in schema1.fields)
        fields2 = set((f.name, f.dataType) for f in schema2.fields)
        
        schema1_diff = fields1 - fields2
        schema2_diff = fields2 - fields1

        if schema1_diff:
            print(f"Columns in {df1_name} NOT in {df2_name}:")
            for name, dataType in schema1_diff:
                print(f"{name}: {dataType}")
            schemas_are_equal = False

        if schema2_diff:
            print(f"Columns in {df2_name} NOT in {df1_name}:")
            for name, dataType in schema2_diff:
                print(f"{name}: {dataType}")
            schemas_are_equal = False
        
        if schemas_are_equal:
            print("Schemas are equal")
    return

def hash_comp(df1,df2):
    """
    Hashes the 2 dataframes and compares the hashed value to determine if the 2 dataframes are identical or not. This method is suitable for dataframes with records less than half a million.

    Parameters:
        df1 (pyspark.sql.DataFrame): Input DataFrame1 
        df2 (pyspark.sql.DataFrame): Input DataFrame2 

    Returns:
        Schema is identical or not
    """
    df1_hash = hashlib.sha256(df1.toPandas().to_string().encode()).hexdigest()
    df2_hash = hashlib.sha256(df2.toPandas().to_string().encode()).hexdigest()
    if df1_hash == df2_hash:
        print("The dataframes are identical.")
    else:
        print("The dataframes are different.")
    return df1_hash == df2_hash

def compare_dataframes(df1, df2):
    """
    Compare two PySpark DataFrames and print out any differences in DATA found.
    """
    # Check if the schemas of the two dataframes match
    if df1.schema != df2.schema:
        print("Schemas of the two dataframes do not match.")
        
    
    # Check if the number of rows in the two dataframes match
    count1 = df1.count()
    count2 = df2.count()
    if count1 != count2:
        print(f"Number of rows in the two dataframes do not match: {count1} != {count2}")
        
    
    # Compare the data in each row of the two dataframes
    rows1 = df1.collect()
    rows2 = df2.collect()
    differences = []
    for i in range(count1):
        row1 = rows1[i]
        row2 = rows2[i]
        if row1 != row2:
            row_differences = {}
            for field in df1.schema:
                if row1[field.name] != row2[field.name]:
                    row_differences[field.name] = (row1[field.name], row2[field.name])
            differences.append((i, row_differences))
    
    # Compare the data in each column of the two dataframes
    column_differences = []
    for col in df1.columns:
        if col not in df2.columns:
            column_differences.append(col)
        elif df1.select(col).exceptAll(df2.select(col)).count() != 0:
            column_differences.append(col)
    
    # Print out any differences found
    if differences or column_differences:
        print("Differences found:")
        for i, row_diffs in differences:
            print(f"  Row {i}:")
            for col, (val1, val2) in row_diffs.items():
                print(f"    {col}: {val1} != {val2}")
        if column_differences:
            print(f"  Columns: {column_differences}")
    else:
        print("Dataframes are identical.")


def check_for_special_characters(df):
    """
    Checks for special characters in a dataframe and returns the list of rows that have special characters

    Parameters:
        df (pyspark.sql.DataFrame): Input DataFrame

    Returns:
        result with the rows that contain special characters
    """
    results = []
    string_cols = [col_name for col_name, data_type in df.dtypes if data_type == "string"]
    for col_name in string_cols:
        contains_special_chars = df.select(count(regexp_extract(col(col_name), r"[^A-Za-z0-9\s]+", 0)).alias("count")).collect()[0]["count"]
        if contains_special_chars > 0:
            # Collect all rows that have special characters
            rows_with_special_chars = df.where(regexp_extract(col(col_name), r"[^A-Za-z0-9\s]+", 0) != '').select(col(col_name)).collect()
            # Find all special characters in those rows and add them to a set
            special_chars = set()
            for row in rows_with_special_chars:
                special_chars.update(re.findall(r"[^A-Za-z0-9\s]+", row[0]))
            results.append(f"Column '{col_name}' contains special characters: {special_chars}")
    return results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Secrets

# COMMAND ----------

# DBTITLE 1,Get Secrets
# =============================================================================
# SECRETS BLOCK — requires Databricks dbutils.  Commented out for local use.
#
# HOW TO RE-ENABLE:
#   1. In Databricks, create a secret scope (e.g. "secret-scope") and add the
#      keys listed below via the Databricks CLI or Secrets API.
#   2. Uncomment this block before running in Databricks.
#
# PLACEHOLDER MAPPING (replace with your actual secret keys):
#   cur_env              → key: "current-env"  values: env_dev | env_uat | env_prd
#   okta_client_id       → key: "okta-client-id"
#   okta_client_secret   → key: "okta-client-secret"
#   okta_subscription_key→ key: "okta-subscription-key"
#   s3_access_key_id     → key: "s3-access-key-id"
#   s3_access_key_secret → key: "s3-access-key-secret"
#   splunk_access_token  → key: "splunk-access-token"
#   ems_token            → key: "ems-token"
#   api_token_primary    → key: "api-token-primary"
#   api_sub_key          → key: "api-sub-key"
#   api_client_secret    → key: "api-client-secret"
#   api_client_id        → key: "api-client-id"
# =============================================================================

# try:
#     cur_env = dbutils.secrets.get("secret-scope", "current-env")
# except Exception as e:
#     print(f"Error retrieving secret for current-env: {e}")

# try:
#     okta_client_id = dbutils.secrets.get("secret-scope", "okta-client-id")
# except Exception as e:
#     print(f"Error retrieving secret for okta-client-id: {e}")
# try:
#     okta_client_secret = dbutils.secrets.get("secret-scope", "okta-client-secret")
# except Exception as e:
#     print(f"Error retrieving secret for okta-client-secret: {e}")
# try:
#     okta_subscription_key = dbutils.secrets.get("secret-scope", "okta-subscription-key")
# except Exception as e:
#     print(f"Error retrieving secret for okta-subscription-key: {e}")

# try:
#     s3_access_key_id = dbutils.secrets.get("secret-scope", "s3-access-key-id")
# except Exception as e:
#     print(f"Error retrieving secret for s3-access-key-id: {e}")
# try:
#     s3_access_key_secret = dbutils.secrets.get("secret-scope", "s3-access-key-secret")
# except Exception as e:
#     print(f"Error retrieving secret for s3-access-key-secret: {e}")

# try:
#     splunk_access_token = dbutils.secrets.get("secret-scope", "splunk-access-token")
# except Exception as e:
#     print(f"Error retrieving secret for splunk-access-token: {e}")

# try:
#     ems_token = dbutils.secrets.get("secret-scope", "ems-token")
# except Exception as e:
#     print(f"Error retrieving secret for ems-token: {e}")

# try:
#     api_token_primary = dbutils.secrets.get("secret-scope", "api-token-primary")
# except Exception as e:
#     print(f"Error retrieving secret for api-token-primary: {e}")
# try:
#     api_sub_key = dbutils.secrets.get("secret-scope", "api-sub-key")
# except Exception as e:
#     print(f"Error retrieving secret for api-sub-key: {e}")

# try:
#     api_client_secret = dbutils.secrets.get("secret-scope", "api-client-secret")
# except Exception as e:
#     print(f"Error retrieving secret for api-client-secret: {e}")
# try:
#     api_client_id = dbutils.secrets.get("secret-scope", "api-client-id")
# except Exception as e:
#     print(f"Error retrieving secret for api-client-id: {e}")

# COMMAND ----------

# DBTITLE 1,Environment Set
# =============================================================================
# ENVIRONMENT VARIABLES BLOCK — requires cur_env to be set from secrets.
# Commented out for local use.  Replace placeholder URLs with your own
# API / service endpoints before re-enabling in Databricks.
#
# Environment values: env_dev | env_uat | env_prd
# =============================================================================

# if (cur_env == "env_dev"):
#     okta_token_url        = 'https://<YOUR_OKTA_DEV_DOMAIN>/oauth2/default/v1/token'
#     s3_bucket_name        = '<YOUR_S3_DEV_BUCKET>'
#     api_users_url         = 'https://<YOUR_API_DEV_HOST>/v2/roles/export?select=Users.UserId,Id,Name'
#     api_roles_url         = 'https://<YOUR_API_DEV_HOST>/v2/roles/export?select=Id,Name,RoleType'
#     api_users_search_url  = 'https://<YOUR_API_DEV_HOST>/v2/users/search'
#     vendor_api_property_url = 'https://<YOUR_VENDOR_STAGE_API>/InboundPropertyService'
#     vendor_api_payment_url  = 'https://<YOUR_VENDOR_STAGE_API>/InboundPaymentService'
#     external_api_base_url   = 'https://<YOUR_EXTERNAL_API_DEV>/'
#     external_api_auth_url   = 'https://<YOUR_EXTERNAL_API_DEV>/'
#     partner_api_base_url    = "https://<YOUR_PARTNER_API_TRAINING>/api/"
#     partner_api_token_name  = "<PARTNER_TOKEN_NAME_DEV>"
#
# elif (cur_env == "env_uat"):
#     okta_token_url        = 'https://<YOUR_OKTA_UAT_DOMAIN>/oauth2/default/v1/token'
#     s3_bucket_name        = '<YOUR_S3_UAT_BUCKET>'
#     api_users_url         = 'https://<YOUR_API_UAT_HOST>/v2/roles/export?select=Users.UserId,Id,Name'
#     api_roles_url         = 'https://<YOUR_API_UAT_HOST>/v2/roles/export?select=Id,Name,RoleType'
#     api_users_search_url  = 'https://<YOUR_API_UAT_HOST>/v2/users/search'
#     vendor_api_property_url = 'https://<YOUR_VENDOR_STAGE_API>/InboundPropertyService'
#     vendor_api_payment_url  = 'https://<YOUR_VENDOR_STAGE_API>/InboundPaymentService'
#     external_api_base_url   = 'https://<YOUR_EXTERNAL_API_PROD>/'
#     external_api_auth_url   = 'https://<YOUR_EXTERNAL_API_PROD>/'
#     partner_api_base_url    = "https://<YOUR_PARTNER_API_TRAINING>/api/"
#     partner_api_token_name  = "<PARTNER_TOKEN_NAME_UAT>"
#
# elif (cur_env == "env_prd"):
#     okta_token_url        = 'https://<YOUR_OKTA_PRD_DOMAIN>/oauth2/default/v1/token'
#     s3_bucket_name        = '<YOUR_S3_PRD_BUCKET>'
#     api_users_url         = 'https://<YOUR_API_PRD_HOST>/v2/roles/export?select=Users.UserId,Id,Name'
#     api_roles_url         = 'https://<YOUR_API_PRD_HOST>/v2/roles/export?select=Id,Name,RoleType'
#     api_users_search_url  = 'https://<YOUR_API_PRD_HOST>/v2/users/search'
#     vendor_api_property_url = 'https://<YOUR_VENDOR_PRD_API>/InboundPropertyService'
#     vendor_api_payment_url  = 'https://<YOUR_VENDOR_PRD_API>/InboundPaymentService'
#     external_api_base_url   = 'https://<YOUR_EXTERNAL_API_PRD>/'
#     external_api_auth_url   = 'https://<YOUR_EXTERNAL_API_PRD>/'
#     partner_api_base_url    = "https://<YOUR_PARTNER_API_PRD>/api/"
#     partner_api_token_name  = "<PARTNER_TOKEN_NAME_PRD>"
#
# else:
#     raise TypeError("Please enter a valid environment: env_dev, env_uat, or env_prd")


# COMMAND ----------

# MAGIC %md
# MAGIC ## RED Data Warehouse
# MAGIC
# MAGIC #### Connecting from Databricks to RED using JDBC Driver

# COMMAND ----------

# MAGIC %md
# MAGIC def red_connection(database, environment= cur_env):
# MAGIC
# MAGIC def red_read(SrcTable, jdbcUrl, SrcSchema = "dbo"):
# MAGIC
# MAGIC def red_write(SrcTable, DesTable, jdbcUrl, DesSchema = "dbo", mode = "overwrite", truncate = "true"):
# MAGIC
# MAGIC def red_stored_proc(connection_info, query):

# COMMAND ----------

# DBTITLE 1,Connection Properties, Read and Write functions
###################################################################################################################################
# Function to set the environment
# database is the database in RED e.g. Stage_Yardi, CDW_Bank_of_America
# environment which will be automatically set
###################################################################################################################################
def red_connection(database, environment = None):
    """
    Function to set the environment
    :database: the database on the reporting server e.g. Stage_HR, DW_Finance.
    :environment: ('DEV', 'UAT', 'PRD') Anything other than these 3 will raise an error.
    :return: JDBC URL.
    """
    set_spark_configs()
    connectioninfo = onprem_connection('REPORTING_DB', database)
    return connectioninfo

###################################################################################################################################
# Read from RED function
# SrcTable is the table in RED that we want to read
# jdbcUrl is the environment setting variable that you will set after calling the red_DB_Connection function
# SrcSchema is the RED scehma where are SrcTable is present. The default value is dbo.
###################################################################################################################################
def red_read(SrcTable, jdbcUrl, SrcSchema = "dbo"):
    """
    Read from RED function
    :SrcTable: the table in RED that we want to read
    :jdbcUrl: the environment setting variable that you will set after calling the red_connection function
    :SrcSchema: the RED scehma where are SrcTable is present. The default value is dbo.
    :return: A dataframe with data.
    """
    tempDF = onprem_read(SrcTable, jdbcUrl, SrcSchema)
    return tempDF
  
###################################################################################################################################
# Write to Red Function 
# SrcTable is the dataframe in databricks we will write to RED
# DesTable is the RED table we are writing too
# jdbcUrl is the environment setting variable that you will set after calling the red_DB_Connection function
# DesSchema is the RED scehma where are DesTable is present.  The default value is dbo.
# mode = (append, overwrite, error or errorifexists, ignore) For full loads we are using overwrite
# truncate = (true, false) Please set this truncate option to true. Otherwise it will drop and recreate the table. 
# Truncate option helps retain datatypes from the source (RED) schema. When it is false the datatypes will change to the ones in the dataframe. 
###################################################################################################################################
def red_write(SrcTable, DesTable, jdbcUrl, DesSchema = "dbo", mode = "overwrite", truncate = "true"):
    """
    Write to Red Function 
    :SrcTable: the dataframe in databricks we will write to RED
    :DesTable: the RED table we are writing too
    :jdbcUrl: the environment setting variable that you will set after calling the red_connection function
    :DesSchema: the RED scehma where are SrcTable is present. The default value is dbo.
    :mode: (append, overwrite, error or errorifexists, ignore) For full loads we are using overwrite
    :truncate: (true, false) Please set this truncate option to true. Otherwise it will drop and recreate the table.
    :return: NULL.
    """
    onprem_write(SrcTable, DesTable, jdbcUrl, DesSchema , mode , truncate )
    return


def red_stored_proc(connection_info, query):
    """
    Function to run a stored procedure on an on prem server
    :jdbc_url: The JDBC URL that has all the server information where the stored proc needs to run
    :query: The query which contains the SQL statement that will run on the server
    :return: If the stored proc returns a result then that is returned as a dataframe
    """
    return onprem_stored_proc(connection_info, query)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ON-PREM Data Warehouse
# MAGIC
# MAGIC #### Connecting from Databricks to On-Prem Whitelisted Servers using JDBC Driver
# MAGIC
# MAGIC def onprem_connection(on_prem_name, jdbcDatabase):
# MAGIC
# MAGIC def onprem_read(SrcTable, connection_info, SrcSchema = "dbo"):
# MAGIC
# MAGIC def onprem_write(SrcTable, DesTable, connection_info, DesSchema = "dbo", mode = "overwrite", truncate = "true"):
# MAGIC
# MAGIC def onprem_stored_proc(connection_info, query):
# MAGIC
# MAGIC def onprem_query_read(connection_info, query):
# MAGIC
# MAGIC def onprem_query_execute(connection_info, query):
# MAGIC
# MAGIC def get_cursor_connection(connection_info):
# MAGIC
# MAGIC def onprem_update(connection_info, SrcTable, DesTable, pk_columns, columnsToUpdate, DesSchema="dbo", batch_size=1000, max_retries=3):

# COMMAND ----------

###################################################################################################################################
# Function to set the environment based on which server profile to use
###################################################################################################################################
def onprem_connection(on_prem_name, jdbcDatabase):
    """
    Function to set the environment
    :on_prem_name: Connection profile name (e.g. REPORTING_DB, INTEGRATION_DB)
    :jdbcDatabase: the database on the on-prem server e.g. Stage_HR, DW_Finance.
    :return: JDBC URL and connection properties
    """
    set_spark_configs()
    environment = environment if environment is not None else "env_dev"
    if(on_prem_name == 'DB_SERVER_1'):
        #Secrets for CLIENT_HUB on-prem connection
        try:
            clientHub_account = dbutils.secrets.get("secret-scope", "clienthubaccount")
        except Exception as e:
            print(f"Error retrieving secret for clienthubaccount: {e}")
        try:
            clientHub_key = dbutils.secrets.get("secret-scope", "clienthubkey")
        except Exception as e:
            print(f"Error retrieving secret for clienthubkey: {e}")
        connectionProperties = {
        "user" : clientHub_account,
        "password" : clientHub_key,
        "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "batchsize" : "1000"
        }
        if (environment == "env_dev"):
            print("not whitelisted in DEV")
        elif (environment == "env_uat"):
            jdbcHostname = '10.245.202.81'
            jdbcPort = "50002"
        elif (environment == "env_prd"):
            jdbcHostname = '10.245.202.81'
            jdbcPort = "50002"
        else:
            raise TypeError("Please Enter a valid Environment.")
    elif(on_prem_name == 'REPORTING_DB'):
        #Secrets for RED on-prem connection
        try:
            red_account = dbutils.secrets.get("secret-scope", "red-account")
        except Exception as e:
            print(f"Error retrieving secret for red-account: {e}")
        try:
            red_key = dbutils.secrets.get("secret-scope", "red-key")
        except Exception as e:
            print(f"Error retrieving secret for red-key: {e}")
        connectionProperties = {
        "user" : red_account,
        "password" : red_key,
        "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "batchsize" : "1000"
        }
        if (environment == "env_dev"):
            jdbcHostname = 'USDEVVREDSQL01.your-domain.com'
            jdbcPort = "1433"
        elif (environment == "env_uat"):
            jdbcHostname = 'USLILVREDUAT01.your-domain.com'
            jdbcPort = "1433"
        elif (environment == "env_prd"):
            jdbcHostname = 'USLILVMDWPRD1.your-domain.com'
            jdbcPort = "1433"
        else:
            raise TypeError("Please Enter a valid Environment.")
    elif(on_prem_name == 'MDM_DB'):
        #Secrets for MDM_ODS on-prem connection
        try:
            mdm_account = dbutils.secrets.get("secret-scope", "MDM-ODS-USR")
        except Exception as e:
            print(f"Error retrieving secret for mdm-user: {e}")
        try:
            mdm_key = dbutils.secrets.get("secret-scope", "MDM-ODS-PWD")
        except Exception as e:
            print(f"Error retrieving secret for mdm-password: {e}")
        connectionProperties = {
        "user" : mdm_account,
        "password" : mdm_key,
        "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "batchsize" : "1000"
        }
        if (environment == "env_dev"):
            jdbcHostname = 'USAZDAPMDMDBD01.your-domain.com'
            jdbcPort = "1433"
        elif (environment == "env_uat"):
            jdbcHostname = 'USAZDAPMDMDBQ01.your-domain.com'
            jdbcPort = "1433"
        elif (environment == "env_prd"):
            jdbcHostname = 'USAZPAPMDMDBP01.your-domain.com'
            jdbcPort = "1433"
        else:
            raise TypeError("Please Enter a valid Environment.")
    elif(on_prem_name == 'UTILITY_DB'):
        #Secrets for PEERS_UTIL on-prem connection
        try:
            peers_user = dbutils.secrets.get("secret-scope", "PEERS-USR")
        except Exception as e:
            print(f"Error retrieving secret for peers-user: {e}")
        try:
            peers_password = dbutils.secrets.get("secret-scope", "PEERS-PWD")
        except Exception as e:
            print(f"Error retrieving secret for peers-password: {e}")
        connectionProperties = {
        "user" : peers_user,
        "password" : peers_password,
        "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "batchsize" : "1000"
        }
        if (environment == "env_dev"):
            jdbcHostname = 'USDEVVSQLDEV06.your-domain.com'
            jdbcPort = "1433"
        elif (environment == "env_uat"):
            jdbcHostname = 'USDEVVSQLDEV06.your-domain.com'
            jdbcPort = "1433"
        elif (environment == "env_prd"):
            jdbcHostname = 'USAZPAPMDMDBP01.your-domain.com'
            jdbcPort = "1433"
        else:
            raise TypeError("Please Enter a valid Environment.")
    elif(on_prem_name == 'LEASE_DB'):
        #Secrets for OVLA_CREDOM on-prem connection
        try:
            ovla_account = dbutils.secrets.get("secret-scope", "ovla-cremdom-account")
        except Exception as e:
            print(f"Error retrieving secret for ovla-cremdom-account: {e}")
        try:
            ovla_key = dbutils.secrets.get("secret-scope", "ovla-cremdom-key")
        except Exception as e:
            print(f"Error retrieving secret for ovla-cremdom-key: {e}")
        connectionProperties = {
        "user" : ovla_account,
        "password" : ovla_key,
        "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "batchsize" : "1000"
        }
        if (environment == "env_dev"):
            jdbcHostname = 'usdevvovladb01.your-domain.com'
            jdbcPort = "1433"
        elif (environment == "env_uat"):
            jdbcHostname = 'USLILVLEASEDB.your-domain.com'
            jdbcPort = "1433"
        elif (environment == "env_prd"):
            jdbcHostname = 'USLILVLEASEDB.your-domain.com'
            jdbcPort = "1433"
        else:
            raise TypeError("Please Enter a valid Environment.")
    elif(on_prem_name == 'ORACLE_DB'):
        #Secrets for ORACLE_CLOUD on-prem connection
        try:
            oracle_account = dbutils.secrets.get("secret-scope", "oracle-account")
        except Exception as e:
            print(f"Error retrieving secret for oracle-account: {e}")
        try:
            oracle_key = dbutils.secrets.get("secret-scope", "oracle-key")
        except Exception as e:
            print(f"Error retrieving secret for oracle-key: {e}")
        connectionProperties = {
        "user" : oracle_account,
        "password" : oracle_key,
        "driver" : "oracle.jdbc.driver.OracleDriver",
        "batchsize" : "1000"
        }
        if (environment == "env_dev"):
            jdbcHostname = 'usocashe1db01-vmpd-nwd51-scan.your-oracle-domain.net'
            jdbcPort = "1521"
        elif (environment == "env_uat"):
            jdbcHostname = 'usocashe1db01-vmpd-nwd51-scan.your-oracle-domain.net'
            jdbcPort = "1521"
        elif (environment == "env_prd"):
            jdbcHostname = 'usocashe1db01-vmpd-nwd51-scan.your-oracle-domain.net'
            jdbcPort = "1521"
        else:
            raise TypeError("Please Enter a valid Environment.")
    elif(on_prem_name == 'STAGE_DB'):
        #Secrets for DIH_STAGE on-prem connection
        try:
            dih_stage_account = dbutils.secrets.get("secret-scope", "dih-stage-account")
        except Exception as e:
            print(f"Error retrieving secret for dih-stage-account: {e}")
        try:
            dih_stage_key = dbutils.secrets.get("secret-scope", "dih-stage-key")
        except Exception as e:
            print(f"Error retrieving secret for dih-stage-key: {e}")
        connectionProperties = {
        "user" : dih_stage_account,
        "password" : dih_stage_key,
        "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "batchsize" : "1000"
        }
        if (environment == "env_dev"):
            jdbcHostname = 'USDEVVINFDBD01.your-domain.com'
            jdbcPort = "1433"
        elif (environment == "env_uat"):
            jdbcHostname = 'USLILVINFDBP01.your-domain.com'
            jdbcPort = "1433"
        elif (environment == "env_prd"):
            jdbcHostname = 'USLILVINFDBP01.your-domain.com'
            jdbcPort = "1433"
        else:
            raise TypeError("Please Enter a valid Environment.")
    elif(on_prem_name == 'TRACKING_DB'):
        #Secrets for OVCP_ONE on-prem connection
        try:
            ovcp_account = dbutils.secrets.get("secret-scope", "ovcp-one-account")
        except Exception as e:
            print(f"Error retrieving secret for ovcp-one-account: {e}")
        try:
            ovcp_key = dbutils.secrets.get("secret-scope", "ovcp-one-key")
        except Exception as e:
            print(f"Error retrieving secret for ovcp-one-key: {e}")
        connectionProperties = {
        "user" : ovcp_account,
        "password" : ovcp_key,
        "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "batchsize" : "1000"
        }
        if (environment == "env_dev"):
            jdbcHostname = 'USDEVVSQLTRK.your-domain.com'
            jdbcPort = "1433"
        elif (environment == "env_uat"):
            jdbcHostname = 'uslildprdsql15.your-domain.com'
            jdbcPort = "1433"
        elif (environment == "env_prd"):
            jdbcHostname = 'uslildprdsql15.your-domain.com'
            jdbcPort = "1433"
        else:
            raise TypeError("Please Enter a valid Environment.")
    elif(on_prem_name == 'WC1'):
        #Secrets for WC1 on-prem connection
        try:
            wc1_accelus_username = dbutils.secrets.get("secret-scope", "wc1-accelus-username")
        except Exception as e:
            print(f"Error retrieving secret for wc1-accelus-username: {e}")
        try:
            wc1_accelus_password = dbutils.secrets.get("secret-scope", "wc1-accelus-password")
        except Exception as e:
            print(f"Error retrieving secret for wc1-accelus-password: {e}")
        connectionProperties = {
        "user" : wc1_accelus_username,
        "password" : wc1_accelus_password,
        "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        }
        if (environment == "env_dev"):
            jdbcHostname = 'your-azure-sql-dev.database.windows.net'
            jdbcPort = "1433"
        elif (environment == "env_uat"):
            jdbcHostname = 'your-azure-sql-uat.database.windows.net'
            jdbcPort = "1433"
        elif (environment == "env_prd"):
            jdbcHostname = 'your-azure-sql-prd.database.windows.net'
            jdbcPort = "1433"
        else:
            raise TypeError("Please Enter a valid Environment.")
    elif(on_prem_name == 'INTEGRATION_DB'):
        #Secrets for STG_INT on-prem connection
        try:
            stg_int_acct = dbutils.secrets.get("secret-scope", "stg-integration-user")
        except Exception as e:
            print(f"Error retrieving secret for stg-integration-user: {e}")
        try:
            stg_int_key = dbutils.secrets.get("secret-scope", "stg-integration-key")
        except Exception as e:
            print(f"Error retrieving secret for stg-integration-key: {e}")
        connectionProperties = {
        "user" : stg_int_acct,
        "password" : stg_int_key,
        "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "batchsize" : "1000"
        }
        if (environment == "env_dev"):
            jdbcHostname = 'USDEVVSQLDEV04.your-domain.com'
            jdbcPort = "1433"
        elif (environment == "env_uat"):
            jdbcHostname = 'USDEVVSQLDEV04.your-domain.com'
            jdbcPort = "1433"
        elif (environment == "env_prd"):
            jdbcHostname = 'USLILVMPRDSQL18.your-domain.com'
            jdbcPort = "1433"
        else:
            raise TypeError("Please Enter a valid Environment.")
    elif on_prem_name == 'PERMIT_DB':
        try:
            eWorkPermit_USR = dbutils.secrets.get("secret-scope", "eWorkPermit-USR")
        except Exception as e:
            print(f"Error retrieving secret for Peers-account: {e}")
        try:
            eWorkPermit_PWD = dbutils.secrets.get("secret-scope", "eWorkPermit-PWD")
        except Exception as e:
            print(f"Error retrieving secret for Peers-key: {e}")
        connectionProperties = {
        "user" : eWorkPermit_USR,
        "password" : eWorkPermit_PWD,
        "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "batchsize" : "1000"
        }
        if (environment == "env_dev"):
            jdbcHostname = 'USDEVVSQLDEV06.your-domain.com'
            jdbcPort = "1433"
        elif (environment == "env_uat"):
            jdbcHostname = 'USDEVVSQLDEV06.your-domain.com'
            jdbcPort = "1433"
        elif (environment == "env_prd"):
            jdbcHostname = 'USLILVPRDSQL16.your-domain.com'
            jdbcPort = "63551"
        else:
            raise TypeError("Please Enter a valid Environment.") 
    else:
        raise TypeError("Please Enter a valid connection name.")   
    print("Connection properties set to "+ on_prem_name )
    if(on_prem_name == 'ORACLE_DB'):
        jdbcUrl = f"jdbc:oracle:thin:@//{jdbcHostname}:{jdbcPort}/{jdbcDatabase}"
    else:
        jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2};encrypt=true;trustServerCertificate=true;authentication=SqlPassword".format(jdbcHostname, jdbcPort, jdbcDatabase)
    print("You are now connecting to " +jdbcDatabase+ " on server " + jdbcHostname )
    #print(jdbcUrl)
    return [jdbcUrl, connectionProperties]

###################################################################################################################################
# Read from any onprem function
# SrcTable is the table in RED that we want to read
# jdbcUrl is the environment setting variable that you will set after calling the red_DB_Connection function
# SrcSchema is the RED scehma where are SrcTable is present. The default value is dbo.
###################################################################################################################################
def onprem_read(SrcTable, connection_info, SrcSchema = "dbo"):
    """
    Read from on prem function
    :SrcTable: the table on the on prem server that we want to read
    :connection_info: the environment connections that you will set after calling the onprem_connection function
    :SrcSchema: the on prem scehma where are SrcTable is present. The default value is dbo.
    :return: A dataframe with data.
    """
    jdbcUrl = connection_info[0]
    connectionProperties = connection_info[1]
    TempDF = spark.read.jdbc(jdbcUrl, table=(SrcSchema + "." + SrcTable), properties=connectionProperties)
    df_count = TempDF.cache().count()
    print("{}.{} has {} rows.".format(SrcSchema, SrcTable, df_count))
    return TempDF
  
###################################################################################################################################
# Write to Red Function 
# SrcTable is the dataframe in databricks we will write to RED
# DesTable is the RED table we are writing too
# jdbcUrl is the environment setting variable that you will set after calling the red_DB_Connection function
# DesSchema is the RED scehma where are DesTable is present.  The default value is dbo.
# mode = (append, overwrite, error or errorifexists, ignore) For full loads we are using overwrite
# truncate = (true, false) Please set this truncate option to true. Otherwise it will drop and recreate the table. 
# Truncate option helps retain datatypes from the source (RED) schema. When it is false the datatypes will change to the ones in the dataframe. 
###################################################################################################################################
def onprem_write(SrcTable, DesTable, connection_info, DesSchema = "dbo", mode = "overwrite", truncate = "true"):
    """
    Write to Red Function 
    :SrcTable: the dataframe in databricks we will write to the on prem server
    :DesTable: the on prem table we are writing too
    :connection_info: the environment connections that you will set after calling the onprem_connection function
    :DesSchema: the RED scehma where are SrcTable is present. The default value is dbo.
    :mode: (append, overwrite, error or errorifexists, ignore) For full loads we are using overwrite
    :truncate: (true, false) Please set this truncate option to true. Otherwise it will drop and recreate the table.
    :return: NULL.
    """
    jdbcUrl = connection_info[0]
    connectionProperties = connection_info[1]
    df_count = SrcTable.cache().count()
    current_time = GetTime()
    print("Started writing to {}.{} which has {} rows at {}.".format(DesSchema, DesTable, df_count, current_time))
    SrcTable.write.mode(mode).option("truncate", truncate).jdbc(jdbcUrl, table=(DesSchema + "." + DesTable), properties=connectionProperties)
    current_time = GetTime()
    print("Finished writing to {}.{} with {} rows at {}.".format(DesSchema, DesTable, df_count, current_time))
    return

def onprem_stored_proc(connection_info, query):
    """
    Function to run a stored procedure on an on prem server
    :connection_info: the environment connections that you will set after calling the onprem_connection function
    :query: The query which contains the SQL statement that will run on the server
    :return: If the stored proc returns a result then that is returned as a dataframe
    """
    jdbc_url = connection_info[0]
    connectionProperties = connection_info[1]
    username = connectionProperties['user']
    password  =  connectionProperties['password']
    driver_manager = SparkContext._gateway.jvm.java.sql.DriverManager
    con = driver_manager.getConnection(jdbc_url, username, password)
    try:
        stmt = con.createStatement()
        #setting the timeout to 5 min 
        #stmt.setQueryTimeout(300) 
        rs = stmt.executeQuery (query)
         #Fetch the metadata of the result set
        meta_data = rs.getMetaData()

        # Get the number of columns in the result set
        num_columns = meta_data.getColumnCount()

        # Define the schema for the DataFrame
        schema = StructType([
            StructField(meta_data.getColumnName(i), StringType(), True)
            for i in range(1, num_columns + 1)
        ])

        # List to store the rows of data
        data = []

        # Extracting results
        while rs.next():
            row = [rs.getString(i) for i in range(1, num_columns + 1)]
            data.append(row)

        # Create a DataFrame from the extracted data and schema
        df = spark.createDataFrame(data, schema)
        return df

    except Exception as e:
        # Handle other exceptions
        print("\n EXCEPTION CAUGHT DURING EXECUTION OF STORED PROC!")

    con.close()

###################################################################################################################################
# Function to read from on prem databases using query
###################################################################################################################################
def onprem_query_read(connection_info, query):
    """Function to read from on prem databases using query
       Parameters:
       :query: the query will be executed on the on prem server database,
       :connection_info: the environment connections that you will set after calling the onprem_connection function       
       Returns:
       :tempDF (dataframe): Returning value is dataframe with query results
    """
    jdbcUrl = connection_info[0]
    connectionProperties = connection_info[1]
    tempDF = spark.read.format("jdbc") \
                 .option("url", jdbcUrl) \
                 .option("query", query) \
                 .option("fetchsize", 50000)\
                 .option("user", connectionProperties['user']) \
                 .option("password", connectionProperties['password']) \
                 .option("driver", connectionProperties['driver']) \
                 .load()
    df_count = tempDF.cache().count()
    print("Query has {} rows.".format(df_count))
    return tempDF

###################################################################################################################################
# Function to run a sql statements on an on-prem server databases
###################################################################################################################################
def onprem_query_execute(connection_info, query):
    """Function to run a sql statements on an on-prem server databases
    :connection_info (list): the environment connections that you will set after calling the onprem_connection function
    :query (string): The query which contains the SQL statement that will run on the server
    :return: Null
    """
    jdbc_url = connection_info[0]
    connectionProperties = connection_info[1]
    username = connectionProperties['user']
    password  =  connectionProperties['password']
    driver_manager = SparkContext._gateway.jvm.java.sql.DriverManager
    con = driver_manager.getConnection(jdbc_url, username, password)
    try:
        stmt = con.createStatement()
        stmt.execute(query)
    except Exception as e:
        print(e.TypeError())
        print("\n EXCEPTION CAUGHT DURING EXECUTION OF SQL STATEMENT!")
    finally:
        con.close()
    return


def get_cursor_connection(connection_info):
    """
    Creates a database cursor object from the connection_info list.
    Parameters:
    -----------
    connection_info : an instance of onprem_connection()
    Returns:
    --------
    cursor : JDBCCursor
        A cursor object for executing SQL statements that also handles connection closing
    """

    jdbc_url = connection_info[0]
    connection_properties = connection_info[1]
    username = connection_properties['user']
    password = connection_properties['password'] 
    driver_manager = SparkContext._gateway.jvm.java.sql.DriverManager
    conn = driver_manager.getConnection(jdbc_url, username, password)
    conn.setAutoCommit(True)  # auto-commit ON
 
    java_sql = SparkContext._gateway.jvm.java.sql
    sql_types = java_sql.Types
 
    class JDBCCursor:
        def __init__(self, conn):
            self.conn = conn
            self.stmt = None
        def executemany(self, sql, params_list):
            try:
                self.stmt = self.conn.prepareStatement(sql) 
                # build the prepared statement batch
                for params in params_list:
                    self.stmt.clearParameters()
                    # typecast to onprem compatible data types
                    for i, param in enumerate(params, 1):   
                        if param is None:
                            self.stmt.setNull(i, sql_types.VARCHAR)
                        elif isinstance(param, bool):
                            self.stmt.setBoolean(i, param)
                        elif isinstance(param, int):
                            self.stmt.setInt(i, param)
                        elif isinstance(param, float):
                            self.stmt.setDouble(i, param)
                        elif isinstance(param, datetime):  
                            java_date = java_sql.Timestamp(int(param.timestamp() * 1000))
                            self.stmt.setTimestamp(i, java_date)
                        else:
                            self.stmt.setString(i, str(param))
                    self.stmt.addBatch()
                return self.stmt.executeBatch()
            except Exception as e:
                print(f"Error in executemany: {str(e)}")
                return None
        def close(self):
            try:
                if self.stmt and not self.stmt.isClosed():
                    self.stmt.close()
            finally:
                if self.conn and not self.conn.isClosed():
                    self.conn.close()
    return JDBCCursor(conn)
 
def onprem_update(connection_info, SrcTable, DesTable, pk_columns, columnsToUpdate, DesSchema="dbo", batch_size=1000, max_retries=3):
    """
    :SrcTable: the dataframe in databricks we will update to the on prem server
    :DesTable: the on prem table we are writing too
    :connection_info: the environment connections that you will set after calling the onprem_connection function
    :DesSchema: the schema where our SrcTable is present. The default value is dbo.
    :batch_size: the number of rows to be updated in each batch. The default value is 1000. Should not be greater than 1000.
    :pk_columns: the primary key columns of the table to be put in the where column. pk_column values should be present in SrcTable.
    :columnsToUpdate: the columns to be updated in the on prem table.
    :max_retries: maximum number of retry attempts for failed updates (default: 3)
    :return: Number of records successfully processed. 
    :Record update limit: Upto 500,000 records can be updated but it is recommended to have max 300,000 to be updated.
    """

    cursor = None
    processed_count = 0

    try:
        user_data = SrcTable.collect()  # get the dataframe values to a list (row objects)

        if not user_data:
            print("No data to update.")
            return 0
 
        set_clause = ", ".join([f"[{col}]=?" for col in columnsToUpdate])
        where_clause = " AND ".join([f"[{pk_col}]=?" for pk_col in pk_columns])
        update_sql = f"UPDATE [{DesSchema}].[{DesTable}] SET {set_clause} WHERE {where_clause}"
 
        # finalise batch details
        total_rows = len(user_data)
        total_batches = (total_rows + batch_size - 1) // batch_size
 
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = (start_idx + batch_size) if (start_idx + batch_size) < total_rows else total_rows # min of both
            batch = user_data[start_idx:end_idx]
            batch_size_actual = end_idx - start_idx
 
            # Prepare parameters: set values then pk values (order matters)
            params_list = []
            for row in batch:
                set_values = [getattr(row, col) for col in columnsToUpdate]
                pk_values = [getattr(row, col) for col in pk_columns]
                params_list.append(tuple(set_values + pk_values))

            print(f"Processing batch {batch_num + 1}/{total_batches} ({start_idx + 1}-{end_idx})...")
 
            # retry logic
            retry_count = 0
            success = False
            last_exception = None

            while not success and retry_count < max_retries:
                try:
                    if retry_count > 0:
                        print(f"Retry attempt {retry_count} for batch {batch_num + 1}...")
                        if cursor:
                            cursor.close()
                        cursor = get_cursor_connection(connection_info) # Create a new cursor for each retry
                    else:
                        cursor = get_cursor_connection(connection_info)    # First attempt

                    update_counts = cursor.executemany(update_sql, params_list)

                    # Check if execution was successful
                    if update_counts is None:
                        print(f"Execution failed for batch {batch_num + 1} - check previous error messages")
                        last_exception = "Execution failed - check previous error messages"
                        retry_count += 1
                        if retry_count < max_retries:
                            wait_time = 2 ** retry_count  # 2, 4, 8 seconds
                            print(f"Waiting {wait_time} seconds before retry...")
                            sleep(wait_time)
                    else:
                        # Only increment processed count if execution was successful
                        processed_count += batch_size_actual
                        success = True
                        print(f"Batch {batch_num + 1} complete: {batch_size_actual} records processed.")

                except Exception as e:
                    last_exception = e
                    retry_count += 1
                    wait_time = 2 ** retry_count  # 2, 4, 8 seconds
                    print(f"Failed attempt {retry_count} for batch {batch_num + 1}: {e}")
                    if retry_count < max_retries:
                        print(f"Waiting {wait_time} seconds before retry...")
                        sleep(wait_time)  

            # If all retries failed, log the error and stop the flow
            if not success:
                print(f"ERROR: Failed to update batch {batch_num + 1} after {max_retries} attempts.")
                print(f"ERROR: Last error message: {last_exception}")
                print(f"ERROR: Stopping the update process. {processed_count} records were successfully updated before failure.")
                print(f"ERROR: Failed at records {start_idx + 1} to {end_idx} out of {total_rows} total records.")
                print(f"ERROR: Check connection, permissions, and data validity.")
                return processed_count  # Return the count of records processed before the failure
 
        print(f"Update complete: {processed_count} records of {DesTable} table processed.")
        return processed_count

    except Exception as e:
        print(f"ERROR: Critical error updating records: {str(e)}")
        print(f"ERROR: {processed_count} records were successfully updated before failure.")
        return processed_count  # Return the count of records processed before the exception
    finally:
        if cursor:
            try: 
                cursor.close()  
            except Exception as e: 
                print(f"Error closing cursor: {str(e)}")
 


# COMMAND ----------

# MAGIC %md
# MAGIC ## Blob Storage 
# MAGIC
# MAGIC #### Reading and writing to Blob Containers from databricks

# COMMAND ----------

# MAGIC %md
# MAGIC def blob_acct(acct = "primary_storage", storage_container = "dataintegration"):
# MAGIC
# MAGIC def blob_connection(acct = 'primary_storage', container = "dataintegration"):
# MAGIC
# MAGIC def blob_read(connection_info, folder="", FileName="Sample.csv",filetype = "csv", table ='p_ihub_landlord', delimiter ="," ,header = "false", row_to_drop = 1, tail ="false", multiLine = 'false'):
# MAGIC
# MAGIC def blob_write(df,FileName, folder, connection_info, filetype = "csv", delimiter =",", header = "false", quoteQualifier = "false", nullvalue = None, quote= '"', quote_dtypes = ['string','timestamp'], quote_header = "false", fillNull = '', new_line = "\n"):
# MAGIC
# MAGIC delete_all_files_in_folder(connection_info, folder_path):
# MAGIC
# MAGIC def blob_cleanup(connection_info):
# MAGIC
# MAGIC get_blob_file_paths(connection_info, folder_path):
# MAGIC
# MAGIC blob_delete(connection_info, blob_file_path):
# MAGIC
# MAGIC def blob_get_file_size(connection_info, blob_name):
# MAGIC
# MAGIC def myConcat(*cols, delimiter=""):
# MAGIC
# MAGIC

# COMMAND ----------

###################################################################################################################################
# Returns the blob storage account
# environment: the current storage account we are using
# return: the storage account name 
###################################################################################################################################
def blob_acct(acct = "primary_storage", storage_container = "dataintegration"):
    """
    Returns the blob storage account name, storage account access key and url. The url can be the wasb url using access key or a SAS URL
    :acct: points to the connection profile
    :storage_container: the blob container that hosts the data
    :return: the storage account name, storage account access key and url
    """
    acct = acct.lower()
    if (acct == "primary_storage"):
        #Secrets for CDI blob connection
        try:
            blob_key = dbutils.secrets.get("secret-scope", "bloblandkey")
        except Exception as e:
            print(f"Error retrieving secret for bloblandkey: {e}")
        try:
            cdi_acct = dbutils.secrets.get("secret-scope", "cdi-stor-acct")
        except Exception as e:
            print(f"Error retrieving secret for cdi-stor-acct: {e}")
        storage_account_name = cdi_acct
        storage_account_access_key = blob_key
        url = "wasbs://"+storage_container+"@"+storage_account_name+".blob.core.windows.net/"
        spark.conf.set("fs.azure.account.key."+storage_account_name+".blob.core.windows.net",storage_account_access_key)
    elif (acct == "client_storage_a"):
        #Secrets for client_storage_a blob connection
        try:
            client_a_acct = dbutils.secrets.get("secret-scope", "client-a-stor-acct")
        except Exception as e:
            print(f"Error retrieving secret for client-a-stor-acct: {e}")
        try:
            client_a_sas_url = dbutils.secrets.get("secret-scope", "client-a-SASURL")
        except Exception as e:
            print(f"Error retrieving secret for client-a-SASURL: {e}")
        storage_account_name = client_a_acct
        storage_account_access_key = client_a_sas_url
        url = "wasbs://"+storage_container+"@"+storage_account_name+".blob.core.windows.net/" 
        spark.conf.set("fs.azure.sas."+storage_container+"."+storage_account_name+".blob.core.windows.net",storage_account_access_key)
    elif (acct == "client_storage_b"):
        #Secrets for client_storage_b blob connection
        try:
            client_b_acct = dbutils.secrets.get("secret-scope", "client-b-stor-acct")
        except Exception as e:
            print(f"Error retrieving secret for client-b-stor-acct: {e}")
        try:
            client_b_sas_url = dbutils.secrets.get("secret-scope", "client-b-SASURL")
        except Exception as e:
            print(f"Error retrieving secret for client-b-SASURL: {e}")
        storage_account_name = client_b_acct
        storage_account_access_key = client_b_sas_url
        url = "wasbs://"+storage_container+"@"+storage_account_name+".blob.core.windows.net/" 
        spark.conf.set("fs.azure.sas."+storage_container+"."+storage_account_name+".blob.core.windows.net",storage_account_access_key) 
    else:
        raise TypeError("Please Enter a valid Account." + acct) 
    return [storage_account_name,storage_account_access_key, url]


###################################################################################################################################
# Connection to the  storage account and container 
# environment is the current  storage account we are using
# storage_account_access_key is the blob key stored as secret in the secret-scope scope
###################################################################################################################################
def blob_connection(acct = 'primary_storage', container = "dataintegration"):
    """
    Returns the wasb, blob storage account name, container storage account access key. The url can be the wasb url using access key or a SAS URL 
    :acct: points to the connection profile
    :container: the blob container that hosts the data
    :return: wasb, blob storage account name, container storage account access key
    """
    
    set_spark_configs()
    stor_acct = blob_acct(acct, container)
    storage_account_name = stor_acct[0]
    storage_account_access_key = stor_acct[1]
    wasb = stor_acct[2]
    storage_container = container
    print("You are now connecting to " + storage_account_name )
    return [wasb, storage_account_name, storage_container, storage_account_access_key]

###################################################################################################################################
# Read from a blob storage
# folder is the folder where the files are present within the container. e.g. test, SrcFiles
# filename is the name of the file in the blob container e.g. Sample.csv DO NOT FORGET THE EXTENSION
# filetype is the type of file we want to read. e.g. csv,json or text. Default is csv. DO NOT READ XML FILES using this function
# delimiter is how the records are separated. The default is ","
# header is an option to specify if a file has header or not. The default for it is false.
# row_to_drop is the row number that you want to drop in a "text" file. It is defaulted to 1. This to remove the first row.
# tail is the row after the last record which is not required. If set to true it will delete the last row of a text file. The default value is false
###################################################################################################################################
def blob_read(connection_info, folder="", FileName="Sample.csv", filetype="csv", table='my_table', delimiter=",", header="false", row_to_drop=1, tail="false", multiLine='false', quote_qualifier="\"", infer_schema="false", encoding=None, escape=None):
    """
    Read from a blob storage 
    :connection_info: the wasb returned from the blob_connection function
    :folder: the folder where the files are present within the container. e.g. test, SrcFiles
    :FileName: the name of the file in the blob container e.g. Sample.csv DO NOT FORGET THE EXTENSION
    :filetype: the type of file we want to read. e.g. csv,json or text. Default is csv. DO NOT READ XML FILES using this function
    :table: Table to be read using the SAS URL
    :delimiter: how the records are separated. The default is ","
    :header: an option to specify if a file has header or not. The default for it is false.
    :row_to_drop: the row number that you want to drop in a "text" file. It is defaulted to 1. This to remove the first row.
    :tail: the row after the last record which is not required. If set to true it will delete the last row of a text file. The default value is false
    :multiLine: the default value is false. If set to true it will read the file as a text file.
    :quote_qualifier: the default value is null
    :infer_schema: the default value is false. If set to true it will infer the schema of the file.
    :encoding: file encoding. Default is None (uses system default). Examples: 'utf-8', 'latin1', 'windows-1252'
    :escape: escape character for CSV files. Default is None. Example: '\\' or '"'
    :return: DF
    """
    
    wasbURL = connection_info[0]
    storage_account = connection_info[1]
    container = connection_info[2]
    access_key = connection_info[3]
    filetype = filetype.lower()
    header = header.lower()
    multiLine = multiLine.lower()
    tail = tail.lower()
    infer_schema = infer_schema.lower()
    
    def simple_csv_parse(line, delimiter, quote_qualifier, escape_char=None):
        """Simple CSV parser for quoted fields with basic escape support"""
        if not quote_qualifier or quote_qualifier not in line:
            return line.split(delimiter)
        
        result = []
        current_field = ""
        in_quotes = False
        i = 0
        
        while i < len(line):
            char = line[i]
            
            # Handle escape character
            if escape_char and char == escape_char and i + 1 < len(line):
                next_char = line[i + 1]
                if next_char == quote_qualifier:
                    # Escaped quote
                    current_field += quote_qualifier
                    i += 2
                    continue
                elif next_char == escape_char:
                    # Escaped escape char
                    current_field += escape_char
                    i += 2
                    continue
            
            if char == quote_qualifier:
                in_quotes = not in_quotes
            elif char == delimiter and not in_quotes:
                result.append(current_field)
                current_field = ""
            else:
                current_field += char
            
            i += 1
        
        result.append(current_field)
        return result
    
    if filetype == "text":
        file_path = wasbURL + folder + "/" + FileName
        
        # Handle encoding if specified
        if encoding:
            text_df = spark.read.option("encoding", encoding).text(file_path)
        else:
            text_df = spark.read.text(file_path)
        
        # Add row numbers
        window_spec = Window.orderBy(F.lit(1))
        text_df = text_df.withColumn("row_number", F.row_number().over(window_spec))
        
        # Get all rows first
        all_rows = text_df.orderBy("row_number").collect()
        
        if not all_rows:
            return spark.createDataFrame([], schema=StructType([]))
        
        # Handle different header scenarios
        if header == "true":
            # When header="true": drop specified row first, then use next row as header
            
            # Remove the row to drop (usually metadata row)
            filtered_rows = [row for row in all_rows if row["row_number"] != row_to_drop]
            
            # Remove tail if specified
            if tail == "true" and len(filtered_rows) > 1:
                filtered_rows = filtered_rows[:-1]
            
            if len(filtered_rows) < 2:  # Need at least header + 1 data row
                return spark.createDataFrame([], schema=StructType([]))
            
            # First remaining row is header, rest are data
            header_row = filtered_rows[0]["value"]
            data_rows = [row["value"] for row in filtered_rows[1:]]
            
            # Parse header to get column names
            header_columns = simple_csv_parse(header_row, delimiter, quote_qualifier, escape)
            header_columns = [col.strip() for col in header_columns]
            
        else:
            # When header="false": first row is header, then handle row dropping
            
            if len(all_rows) < 2:  # Need at least header + 1 data row
                return spark.createDataFrame([], schema=StructType([]))
            
            # First row is always header
            header_row = all_rows[0]["value"]
            header_columns = simple_csv_parse(header_row, delimiter, quote_qualifier, escape)
            header_columns = [col.strip() for col in header_columns]
            
            # Get remaining rows (excluding header)
            remaining_rows = all_rows[1:]
            
            # Handle row dropping (adjust index since header is already removed)
            if row_to_drop > 1:
                # row_to_drop is 1-indexed, but we need to adjust since header is removed
                adjusted_drop_index = row_to_drop - 1  # Convert to 0-indexed for remaining_rows
                if adjusted_drop_index > 0 and adjusted_drop_index <= len(remaining_rows):
                    remaining_rows = [row for i, row in enumerate(remaining_rows) if i != (adjusted_drop_index - 1)]
            
            # Handle tail removal
            if tail == "true" and len(remaining_rows) > 0:
                remaining_rows = remaining_rows[:-1]
            
            # Extract data
            data_rows = [row["value"] for row in remaining_rows]
        
        # Parse data rows
        if data_rows and header_columns:
            parsed_data = []
            for row_text in data_rows:
                # Handle empty values properly with escape support
                row_values = simple_csv_parse(row_text, delimiter, quote_qualifier, escape)
                # Pad with None if row has fewer columns than header
                while len(row_values) < len(header_columns):
                    row_values.append(None)
                # Truncate if row has more columns than header
                row_values = row_values[:len(header_columns)]
                
                # Convert empty strings to None for better handling
                row_values = [val if val and val.strip() else None for val in row_values]
                parsed_data.append(row_values)
            
            # Create schema
            fields = [StructField(col_name, StringType(), True) for col_name in header_columns]
            schema = StructType(fields)
            
            # Create DataFrame
            df = spark.createDataFrame(parsed_data, schema)
        else:
            # Empty DataFrame
            df = spark.createDataFrame([], schema=StructType([]))
        
    elif filetype == "delta":
        delta_path = wasbURL + folder + "/" + table
        df = spark.read.format(filetype).load(delta_path)
        
    elif filetype == "csv":
        options = {
            "header": header,
            "inferSchema": infer_schema,
            "quote": quote_qualifier,
            "delimiter": delimiter,
            "multiLine": multiLine
        }
        
        # Add encoding option if specified
        if encoding:
            options["encoding"] = encoding
        
        # Add escape option if specified
        if escape:
            options["escape"] = escape
            
        df = spark.read.format(filetype).options(**options).load(wasbURL + folder + "/" + FileName)
            
    elif filetype == "json":
        options = {
            "header": header,
            "quote": quote_qualifier,
            "delimiter": delimiter,
            "multiLine": multiLine
        }
        
        # Add encoding option if specified
        if encoding:
            options["encoding"] = encoding
            
        # Add escape option if specified
        if escape:
            options["escape"] = escape
            
        df = spark.read.format(filetype).options(**options).load(wasbURL + folder + "/" + FileName)
    
    return df

################################################################################################################################### 
# Function to write to a blob storage (new function)
# Write to a blob storage 
# df-Pass the dataframe that you want to write to the blob container.
# FileName: the name that we want to give to the file. e.g. Sample.csv DO NOT FORGET THE EXTENSION
# folder: the folder where we want to write the file within the container. e.g. SrcFiles. Default is test.
# connection_info: the wasb returned from the blob_connection function
# filetype: the type of file we want to read. e.g. csv,json or text. Default is csv. DO NOT WRITE XML FILES using this function
# delimiter: how the records are separated. The default is ","
# header: an option to specify if a file has header or not. The default for it is false.
# quoteQualifier: indicating whether all values should always be enclosed in quotes.
# nullvalue: sets the string representation of a null value. If None is set, it uses the default value, empty string.
# quote: (Other than text files):sets a single character used for escaping quoted values where the separator can be part of the value
#        (text files): quote is string value, which is used for prefix and suffix to values
# quote_dtypes: list contains the datatypes, which are required to have prefix and suffix values(quote parameter)
# quote_header: indicating whether all headers should be enclosed in quotes.
###################################################################################################################################
def blob_write(df, FileName, folder, connection_info, filetype="csv", delimiter=",", header="false", 
               quoteQualifier="false", nullvalue=None, quote='"', quote_dtypes=['string','timestamp'], 
               quote_header="false", fillNull='', new_line="\n", escape=None, multiLine="false"):
    """
    Write to a blob storage 
    :df: Pass the dataframe that you want to write to the blob container.
    :FileName: the name that we want to give to the file. e.g. Sample.csv DO NOT FORGET THE EXTENSION
    :folder: the folder where we want to write the file within the container. e.g. SrcFiles. Default is test.
    :connection_info: the wasb returned from the blob_connection function
    :filetype: the type of file we want to read. e.g. csv,json or text. Default is csv. DO NOT WRITE XML FILES using this function
    :delimiter: how the records are separated. The default is ","
    :header: an option to specify if a file has header or not. The default for it is false.
    :quoteQualifier: indicating whether all values should always be enclosed in quotes.
    :nullvalue: sets the string representation of a null value. If None is set, it uses the default value, empty string.
    :quote: (Other than text files):sets a single character used for escaping quoted values where the separator can be part of the value
            (text files): quote is string value, which is used for prefix and suffix to values
    :quote_dtypes: list contains the datatypes, which are required to have prefix and suffix values(quote parameter)
    :quote_header: indicating whether all headers should be enclosed in quotes.
    :fillNull: fill nulls with any input charcaters 
    :new_line: defines the new line character as carriage return and/or line feed. Requires double quotes eg. "\n", "\r\n"
    :escape: escape character for special characters in CSV files. Default is None. Example: '\\' or '"'
    :multiLine: whether or not a record can span multiple lines. The default value is false.
    Return: NULL
    """
    
    wasbURL = connection_info[0]
    filetype = filetype.lower()
    header = header.lower()
    quoteQualifier = quoteQualifier.lower()
    quote_header = quote_header.lower()
    multiLine = multiLine.lower()
    folder_path = f'''log_folder/{get_notebook_name()}'''
    output_blob_folder = wasbURL+folder_path

    if (filetype == "text"):
        # text file Environment
        if quoteQualifier == "true":
            df = add_prefix_suffix(df, quote_dtypes, quote, quote_header)
        df = to_string_datatype(df)
        df = df.na.fill(fillNull)  
        if (header.lower() == "true") or (quote_header == "true"):
            # Collect header and rows
            header_row = delimiter.join(df.columns)
            
            # Use collect() to get all rows as arrays
            data_rows = []
            for row in df.collect():
                data_row = delimiter.join(str(item) for item in row)
                data_rows.append(data_row)
                
            # Join with new_line character
            updated_data = new_line.join([header_row] + data_rows)
            df_text = spark.createDataFrame([(updated_data,)], ["combined"])            
        else:
            # Use concat_ws for joining columns with delimiter
            df_text = df.withColumn("combined", concat_ws(delimiter, *[col(c) for c in df.columns])).select("combined")
        
        df_text.coalesce(1).write.mode("overwrite").format(filetype).save(output_blob_folder)
    else:
        row_count = df.count()
        if row_count != 0:
            # Estimate dataframe size
            try:
                # Use the simplified calculation function
                df_size = calculate_df_size(df)
            except Exception as e:
                # Simple fallback estimation if calculation fails
                df_size = row_count * len(df.columns) * 50
                print(f"Warning: Using simplified size estimation due to error: {str(e)}")

            # Calculate number of partitions based on size
            partition_size = 128 * 1024 * 1024  # 128MB
            if df_size > partition_size:
                num_partitions = df_size // partition_size
                if num_partitions < 1:
                    num_partitions = 1
                # Cap at a reasonable number to avoid excessive partitioning
                if num_partitions > 200:
                    num_partitions = 200 
            else:
                num_partitions = 1   
                
            df = df.repartition(num_partitions)
        
        # Write options
        write_options = {
            "delimiter": delimiter,
            "header": header,
            "quoteAll": quoteQualifier,
            "quote": quote,
            "multiLine": multiLine
        }
        
        # Add nullValue option if specified
        if nullvalue is not None:
            write_options["nullValue"] = nullvalue
            
        # Add escape option if specified
        if escape is not None:
            write_options["escape"] = escape
        
        # Write the dataframe to blob storage
        writer = df.coalesce(1).write.mode("overwrite")
        for key, value in write_options.items():
            writer = writer.option(key, value)
        
        writer.format(filetype).save(output_blob_folder)
    
    # Rename output file to desired filename
    files = dbutils.fs.ls(output_blob_folder)
    output_file = [x for x in files if x.name.startswith("part-")]
    
    if output_file:
        dbutils.fs.mv(output_file[0].path, wasbURL+folder+"/"+FileName)
    else:
        print(f"Warning: No output file found in {output_blob_folder}")
    
    # Clean up temporary files
    delete_all_files_in_folder(connection_info, folder_path)
    blob_cleanup(connection_info)
    return

# Delete all files in a specific folder
def delete_all_files_in_folder(connection_info, folder_path):
    # Get the list of blob file paths
    blob_file_paths = get_blob_file_paths(connection_info, folder_path)
    # Delete each blob using blob_delete function
    for blob_file_path in blob_file_paths:
      blob_delete(connection_info, blob_file_path)

def blob_cleanup(connection_info):
    # Azure Blob Storage account details
    blob_container = connection_info[2]
    blob_storage_account = connection_info[1]
    blob_access_key = connection_info[3]

    # Create BlobServiceClient
    blob_service_client = BlobServiceClient.from_connection_string(f"DefaultEndpointsProtocol=https;AccountName={blob_storage_account};AccountKey={blob_access_key};EndpointSuffix=core.windows.net")

    # Get a reference to the container
    container_client = blob_service_client.get_container_client(blob_container)

    # List all files in the container
    blob_list = container_client.list_blobs()

    # Delete 0B files
    deleted_count = 0
    for blob in blob_list:
        if blob.size == 0:
            try:
                container_client.delete_blob(blob.name)
            except Exception as e:
                print('WARNING: Failed to delete blob:', blob.name)
            deleted_count += 1

    print(f"Deleted {deleted_count} 0B files from the container.")
    return

def get_blob_file_paths(connection_info, folder_path):
    # Azure Blob Storage account details
    blob_container = connection_info[2]
    blob_storage_account = connection_info[1]
    blob_access_key = connection_info[3]
    
    # Create BlobServiceClient
    blob_service_client = BlobServiceClient.from_connection_string(f"DefaultEndpointsProtocol=https;AccountName={blob_storage_account};AccountKey={blob_access_key};EndpointSuffix=core.windows.net")
    
    # Get a reference to the container
    container_client = blob_service_client.get_container_client(blob_container)
    
    # List all blobs within the specified folder
    blob_paths = []
    blobs_list = container_client.list_blobs(name_starts_with=folder_path)
    
    for blob in blobs_list:
        blob_paths.append(blob.name)
    
    return blob_paths

def blob_delete(connection_info, blob_file_path):
    # Azure Blob Storage account details
    blob_container = connection_info[2]
    blob_storage_account = connection_info[1]
    blob_access_key = connection_info[3]

    # Create BlobServiceClient
    blob_service_client = BlobServiceClient.from_connection_string(f"DefaultEndpointsProtocol=https;AccountName={blob_storage_account};AccountKey={blob_access_key};EndpointSuffix=core.windows.net")

    # Get a reference to the container
    container_client = blob_service_client.get_container_client(blob_container)

    # Delete specific blob
    try:
        container_client.delete_blob(blob_file_path)
        #print('Successfully deleted blob:', blob_file_path)
    except Exception as e:
        print('WARNING: Failed to delete blob:', blob_file_path, 'with error:', str(e))

def blob_get_file_size(connection_info, blob_name):
    """
    :param connection_info: The connection_details for the blob storage
    :param blob_name: The name of the file (blob) we want to get size of
    :return: File size of the blob
    """
    blob_storage_account = connection_info[1]
    blob_container = connection_info[2]
    blob_access_key = connection_info[3]
    url = f"DefaultEndpointsProtocol=https;AccountName={blob_storage_account};AccountKey={blob_access_key};EndpointSuffix=core.windows.net"
    # Create a Blob service client
    retry_index = 5
    while(retry_index > 0):
        try:
            blob_service_client = BlobServiceClient.from_connection_string(url)
            blob_client = blob_service_client.get_blob_client(blob_container, blob_name)
            blob_properties = blob_client.get_blob_properties()
            retry_index = 0
            return blob_properties.size
        except:
            print("WARNING: Could not get file size. Retrying")
            retry_index = retry_index-1
            time_delay(10, 'sec')
    print("ERROR: Failed to get file size after 5 tries")

def myConcat(*cols, delimiter=""):
    """
    Function called within the write to help create a text (.txt) file
    :cols: Pass the dataframe that you want to write to the blob container.
    :return: All the data concatenated into a single column.
    """
    concat_columns = []
    for c in cols[:-1]:
        concat_columns.append(F.coalesce(c, F.lit("")))
        concat_columns.append(F.lit(delimiter))
    concat_columns.append(F.coalesce(cols[-1], F.lit("")))
    return F.concat(*concat_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Snowflake
# MAGIC
# MAGIC #### Connecting from Databricks to Snowflake

# COMMAND ----------

# MAGIC %md
# MAGIC def snow_conection(sfWarehouse='MY_WAREHOUSE', sfRole='MY_ROLE', sfSchema=None, snowdb='MY_DATABASE'):
# MAGIC
# MAGIC def snow_read(swtablename,sfOptions, SNOWFLAKE_SOURCE_NAME="snowflake"):
# MAGIC
# MAGIC def snow_write(sw_df,swtablename,sfOptions, SNOWFLAKE_SOURCE_NAME="snowflake"):
# MAGIC
# MAGIC def snow_stored_proc(sfOptions, procedure_name)
# MAGIC
# MAGIC def snow_write_insert(sw_df, swtablename, sfOptions, SNOWFLAKE_SOURCE_NAME="snowflake"):
# MAGIC
# MAGIC def snow_write_update_query(sw_df, swtablename, sfOptions, update_query_keys): --> Old
# MAGIC
# MAGIC def load_insert_update_delete_dataframes(df_list, tgt_table, sfOptions, update_query_keys):
# MAGIC
# MAGIC def snowflake_python_connector(sfOptions):
# MAGIC
# MAGIC def snow_write_update_query(sw_df, swtablename, sfOptions, update_query_keys): --> New
# MAGIC
# MAGIC def snow_write_update_query_delta(sw_df, swtablename, sfOptions, update_query_keys): --> Using Delta lake
# MAGIC
# MAGIC def generate_delta_merge_conditions(column_list, update_query_keys):
# MAGIC
# MAGIC def snow_query_read(snow_query, sfOptions, SNOWFLAKE_SOURCE_NAME="snowflake"):

# COMMAND ----------

# DBTITLE 0,Snowflake Connection Properties and Functions
###################################################################################################################################
# Setting up the snowflake connection
# sfWarehouse is the compute resource within snowflake that we are using. Replace MY_WAREHOUSE with your warehouse name.
# sfRole is the role within snowflake used to implement our code.  Replace MY_ROLE with your role name.
# sfSchema is the schema within the the database refering to the table. Defaults to env_dev if None.
# snowdb is the database which has the schema and table we want to connect with. Replace MY_DATABASE with your DB name.
# column_mapping is used to map columns as per the names in snowflake. DOES NOT WORK WITH OVERWRITE METHOD
# column_mismatch_behavior raises an error if the number of columns within the dataframe do not match with the number of columns in snowflake 
###################################################################################################################################
def snow_conection(sfWarehouse ='MY_WAREHOUSE', sfRole = 'MY_ROLE', sfSchema = None, snowdb = 'MY_DATABASE', sfProfile= ''):
    """
    Function to set snowflake connection
    :sfWarehouse: the compute resource within snowflake that we are using.
    :sfRole: the role within snowflake used to implement our code.
    :sfSchema: the schema within the the database refering to the table.
    :snowdb: the database which has the schema and table we want to connect with.
    :return: returns sfOptions.
    """
    set_spark_configs()
    if sfSchema is None:
        sfSchema = "env_dev"  # default to dev if no environment provided
    if sfRole == 'MY_SHARE_ROLE' and sfWarehouse == 'MY_SHARE_WAREHOUSE':
        print("You are now connecting to " +snowdb+ " on "+sfSchema+ " server "+ sfWarehouse )

        # Setting Secrets info for Snowflake Share
        try:
            snowserv = dbutils.secrets.get("secret-scope", "snowflake-server-share")
        except Exception as e:
            print(f"Error retrieving secret for : {e}")
        try:
            snowuser = dbutils.secrets.get("secret-scope", "snowflake-user-share")
        except Exception as e:
            print(f"Error retrieving secret for : {e}")
        try:
            snowpemprivatekey = dbutils.secrets.get("secret-scope", "snowflake-pem-key-share")
        except Exception as e:
            print(f"Error retrieving secret for snowpemprivatekey: {e}")         
    else:
        if (sfSchema == "env_dev"):
            sfSchema = "DEV" 
            if sfProfile == "INT":
                sfSchema = "INT"
            print("You are now connecting to " +snowdb+ " on "+sfSchema+ " server " + sfWarehouse )
        elif (sfSchema == "env_uat"):
            sfSchema = "PRE_PROD"
            if sfProfile == "INT":
                sfSchema = "INT"
            print("You are now connecting to " +snowdb+ " on "+sfSchema+ " server "+ sfWarehouse )
        elif (sfSchema == "env_prd"):
            sfSchema ="PROD"
            if sfProfile == "INT":
                sfSchema = "INT"
            print("You are now connecting to " +snowdb+ " on "+sfSchema+ " server "+ sfWarehouse )
        else:
            raise TypeError("Please Enter a valid Environment.")
        
        # Setting Secrets info for Snowflake
        try:
            snowserv = dbutils.secrets.get("secret-scope", "snowflake-server")
        except Exception as e:
            print(f"Error retrieving secret for snowserv: {e}")
        try:
            snowuser = dbutils.secrets.get("secret-scope", "snowflake-user")
        except Exception as e:
            print(f"Error retrieving secret for snowuser: {e}")    
        try:
            snowpemprivatekey = dbutils.secrets.get("secret-scope", "snowflake-pem-key")
        except Exception as e:
            print(f"Error retrieving secret for snowpemprivatekey: {e}")

    sfOptions = {
        "sfURL" : snowserv,
        "sfUser" : snowuser,
        "pem_private_key" : snowpemprivatekey,
        "sfDatabase" : snowdb,
        "sfSchema" : sfSchema,
        "sfWarehouse" : sfWarehouse,
        "sfRole" : sfRole,
        "column_mapping" : 'name',
        "column_mismatch_behavior" : 'error'  
    }
    return  sfOptions

###################################################################################################################################
# Function to read from a snowflake table into a dataframe
# swtablename is the table name that we want to read from snowflake 
# sfOptions is the output of the snow_conection function that has to be run before this.
# SNOWFLAKE_SOURCE_NAME="snowflake" is the format that lets spark know that we are interacting with snowflake
###################################################################################################################################
def snow_read(swtablename,sfOptions, SNOWFLAKE_SOURCE_NAME="snowflake"):
    """
    Function to read from Snowflake
    :swtablename: the table name that we want to read from snowflake 
    :sfOptions: the output of the snow_conection function that has to be run before this.
    :SNOWFLAKE_SOURCE_NAME: the format that lets spark know that we are interacting with snowflake
    :return: returns sw_df with the data we want to read.
    """
    sw_df = spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable",swtablename).load()
    df_count = sw_df.cache().count()
    print("{} has {} rows.".format(swtablename, df_count))
    return sw_df

###############################################################################################################################
# Function to write a dataframe to a snowflake table 
# sw_df is the dataframe which has the data that we are going to write to snowflake
# swtablename is the table name that we want to wite to in snowflake 
# sfOptions is the output of the snow_conection function that has to be run before this.
# SNOWFLAKE_SOURCE_NAME="snowflake" is the format that lets spark know that we are interacting with snowflake
###############################################################################################################################
def snow_write(sw_df,swtablename,sfOptions, SNOWFLAKE_SOURCE_NAME="snowflake"):
    """
    Function to write to Snowflake
    :sw_df: the dataframe we want to write from 
    :swtablename: the table name that we want to write to in snowflake 
    :sfOptions: the output of the snow_conection function that has to be run before this.
    :SNOWFLAKE_SOURCE_NAME: the format that lets spark know that we are interacting with snowflake
    :return: null
    """
    df_count = sw_df.cache().count()
    print('Writing to table {} rows to table {}'.format(df_count,swtablename))
    SparkContext._jvm.net.snowflake.spark.snowflake.Utils.runQuery(sfOptions, "DELETE FROM "'{targettable} '.format(targettable=swtablename))
    sw_df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable",swtablename).mode('append').save()
    return 
    
###############################################################################################################################
#Function to Run a stored procedure in Snowflake
#DOES NOT RETURN ANYTHING. ASSUME IT HAS RUN. SOURCE: TRUST ME BRO.
###############################################################################################################################
def snow_stored_proc(sfOptions, procedure_name) -> DataFrame:
    """
    Function to execute a stored procedure in Snowflake and return the result.
    :sfOptions: the snowflake connection options.
    :stored_proc_name: the name of the stored procedure to execute.
    :return: returns the result of the stored procedure.
    """
    # Execute stored procedure
    result_df = SparkContext._jvm.net.snowflake.spark.snowflake.Utils.runQuery(
        sfOptions,
        f"CALL {procedure_name}",
    )
    return result_df

###################################################################################################################################
# Function to Insert rows to Snowflake table
###################################################################################################################################
def snow_write_insert(sw_df, swtablename, sfOptions, SNOWFLAKE_SOURCE_NAME="snowflake"):
    """Function to Insert rows to Snowflake table       
       Parameters:
       :sw_df (dataframe): Dataframe contains the Insert rows data, 
       :swtablename (string): Target table name to which data get loaded,
       :sfOptions (dictionary) : Contains the connection properties of snowflake table, which is return value of snow_conection function,
       :SNOWFLAKE_SOURCE_NAME (string): the format that lets spark know that we are interacting with snowflake       
       Returns: null
    """
    df_count = sw_df.cache().count()
    print('Writing to table {} rows to table {}'.format(df_count,swtablename))    
    sw_df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable",swtablename).mode('append').save()
    return

###################################################################################################################################
# Function to load insert, update and delete records into target table
###################################################################################################################################
def load_insert_update_delete_dataframes(df_list, tgt_table, sfOptions, update_query_keys):
   """Function to load insert, update and delete records into target table    
       Parameters:
       :df_list (list): Input value is the dataframe lists with Insert, Update and Delete dataframes, 
                       Order must be Insert, Update, Delete dataframes 
       :tgt_table (string): Target table name to which data get loaded,
       :sfOptions (dictionary) : Contains the connection properties of snowflake table, which is return value of snow_conection function,
       :update_query_keys (list): Contains the list of key columns, which are required to SQL update where condition       
       Returns:
       :status (string): Successful status.
   """
   if len(df_list) == 3:
      df_tgt_insert = df_list[0]
      df_tgt_update = df_list[1]
      df_tgt_delete = df_list[2]
   else:
      raise TypeError("Please check the number of dataframes in df_list. Total should be equal to three")
   print("insert dataframe row count:{}".format(df_tgt_insert.count()))
   print("update dataframe row count:{}".format(df_tgt_update.count()))
   print("delete dataframe row count:{}".format(df_tgt_delete.count()))
   # inserting records
   snow_write_insert(df_tgt_insert, tgt_table, sfOptions)
   # updating records
   snow_write_update_query(df_tgt_update, tgt_table, sfOptions, update_query_keys)
   # deleting records: will be used update method
   print("Deleting records means updating existing records with flag D")
   snow_write_update_query(df_tgt_delete, tgt_table, sfOptions, update_query_keys)
   status = 'Insertion, Updation and Deletion of records completed successfully'
   return status

###################################################################################################################################
# Function to create a snowflake connection
###################################################################################################################################
def snowflake_python_connector(sfOptions):
    """Function to create a snowflake connection       
       Parameters:
       :sfOptions (dictionary) : Contains the connection properties of snowflake table, which is return value of snow_conection function,
       Returns:
       :con (object): snowflake connection object
    """
    con = snowflake.connector.connect(
        user = sfOptions['sfUser'],
        password = sfOptions['sfPassword'],
        account = '.'.join(sfOptions['sfURL'].split('.')[:3]),        
        database = sfOptions['sfPassword'],
        schema = sfOptions['sfDatabase'],
        role = sfOptions['sfRole'],
        warehouse = sfOptions['sfWarehouse']
    )
    return con


###################################################################################################################################
# Function to update rows in Snowflake table Using Delta Lake - V3.0
###################################################################################################################################

def snow_write_update_query(sw_df, swtablename, sfOptions, update_query_keys, catalog="my_catalog", schema="my_schema"):
    """Function to update rows in Snowflake table       
       Parameters:
       :sw_df (dataframe): Dataframe contains the update rows data, 
       :swtablename (string): Target table name to which data get loaded,
       :sfOptions (dictionary) : Contains the connection properties of snowflake table, which is return value of snow_conection function,
       :update_query_keys (list): Contains the list of key columns, which are required to SQL update where condition,
       :catalog (string): Unity Catalog catalog name (default: "work_dynamics"),
       :schema (string): Unity Catalog schema name (default: "udpcdi")
       Returns: null
    """
    sw_df = sw_df.dropDuplicates()
    update_df_count = sw_df.cache().count()
    
    if update_df_count >= 1:        
        column_list = sw_df.columns        
        condition_set_strings = generate_delta_merge_conditions(column_list, update_query_keys)        
        on_condition = condition_set_strings[0]
        set_column_dict = condition_set_strings[1]
        
        notebook_name = get_notebook_name()
        temp_table_name = f'{notebook_name}_{swtablename}'.lower()
        full_table_name = f'{catalog}.{schema}.{temp_table_name}'
        
        # Drop the table if it already exists
        spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
        
        # Query source data from Snowflake
        source_table_query = f'select * from {swtablename}'
        source_df = snow_query_read(source_table_query, sfOptions)
        
        print(f'Updating {update_df_count} rows to table {swtablename}')
        
        # Write source data to Unity Catalog
        source_df.write.mode('overwrite').format('delta').saveAsTable(full_table_name)
        
        # Use forName instead of forPath for Unity Catalog tables
        deltaTableSource = DeltaTable.forName(spark, full_table_name)
        
        # Execute merge operation
        deltaTableSource.alias('source').merge(sw_df.alias('updates'), on_condition) \
            .whenMatchedUpdate(set=set_column_dict).execute()    
        
        # Read the merged data
        final_df = spark.table(full_table_name)
        
        try:
            # Write merged data back to Snowflake
            snow_write(final_df, swtablename, sfOptions)
            print(f"Successfully updated {update_df_count} rows in {swtablename}")
        except Exception as e:
            # Rollback to original data if update fails
            snow_write(source_df, swtablename, sfOptions)
            print(f"Issue encountered while loading the updated data: {str(e)}")
            raise TypeError("Issue encountered while loading the updated data")
        finally:
            # Clean up temporary table regardless of success or failure
            spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
            print(f"Temporary table {full_table_name} has been dropped")
    else:
        print("Zero records present in dataframe")           
    
    return


###################################################################################################################################
# Function to generate delta merge conditions
###################################################################################################################################
def generate_delta_merge_conditions(column_list, update_query_keys):
    """Function to generate delta merge conditions      
       Parameters:
       :column_list (list): Contains the list of columns, which are required to SQL update set and where condition,
       :update_query_keys (list): Contains the list of key columns, which are required to SQL update where condition,       
       Returns:
       :condition_string (string): string contains set of key comparison statements(ex: 'source.key = target.key')
       :set_dict (dictionary): its dictionary of column assignments on condition match (ex: target.lastSeen": "source.timestamp)
    """
    condition_string, condition_string_list, set_dict = '', [], {}
    for key_column in update_query_keys:        
        if key_column in column_list:
            column_list.remove(key_column)
        else:
            raise TypeError("Key column not present in the column_list")
    if (len(column_list) >= 1) and (len(update_query_keys) >= 1):
        for key_column in update_query_keys:
            condition_string = f"source.{key_column} = updates.{key_column}"
            condition_string_list.append(condition_string)
        for set_column in column_list:
            set_dict[set_column] = f'updates.{set_column}'
        condition_string = ' AND '.join(condition_string_list)
    else:
        raise TypeError("Dataframe should contains atleast two columns to proceed with update command. One or more columns will be used in where condition and rest will be in set condition")
    return [condition_string, set_dict]
###################################################################################################################################
#Function to read from Snowflake based on query
###################################################################################################################################
def snow_query_read(snow_query, sfOptions, SNOWFLAKE_SOURCE_NAME="snowflake"):
    """
    Function to read from Snowflake based on query
    :snow_query: the query that we want to read from snowflake 
    :sfOptions: the output of the snow_conection function that has to be run before this.
    :SNOWFLAKE_SOURCE_NAME: the format that lets spark know that we are interacting with snowflake
    :return: returns sw_df with the data we want to read.
    """
    sw_query_df = spark.read.format('snowflake').options(**sfOptions).option("query", snow_query).load()
    df_count = sw_query_df.count()
    print("{} has {} rows.".format(snow_query, df_count))
    return sw_query_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDP LAKE
# MAGIC
# MAGIC def lake_read(path,container):

# COMMAND ----------

def lake_read(path,container):
    """
    Function to read data from the EDP lake.

    Parameters:
        path: The path provided by the EDP team. 
        container: Container where the file exisits (snapshot or processed)

    Returns:
        df_snapshot: Return a dataframe with data from the lake.
    """
    # Setting configuration info for lake.
    try:
        lake_account = dbutils.secrets.get("secret-scope", "datalakeaccount")
    except Exception as e:
        print(f"Error retrieving secret for datalakeaccount: {e}")
    try:
        lake_key = dbutils.secrets.get("secret-scope", "datalakekey")
    except Exception as e:
        print(f"Error retrieving secret for datalakekey: {e}")
    spark.conf.set("fs.azure.account.key.{}.dfs.core.windows.net".format(lake_account), lake_key)
    hlevel_path = "abfss://{}@{}.dfs.core.windows.net/".format(container,lake_account)
    snapshot_path = hlevel_path + path
    df_snapshot = spark.read.format('delta').option('header', 'true').option("escape", '"').load(snapshot_path)
    df_count = df_snapshot.cache().count()
    print("load {} of {} has {} rows.".format(container,snapshot_path, df_count))
    
    return(df_snapshot)

# COMMAND ----------

# MAGIC %md
# MAGIC ##SALESFORCE Integration

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC def salesforce_connection(resource = 'sf_org_primary', environment = None):
# MAGIC
# MAGIC def salesforce_read(connectionProperties, salesf_query, schema):
# MAGIC
# MAGIC def salesforce_write(connectionProperties, df, sfObject, operation, load_type='bulk', object_pk = 'Id', batch_size="auto", set_to_null = 'False', use_serial = False):
# MAGIC
# MAGIC def generate_failed_records_file(df_FR, operation):

# COMMAND ----------

############################################################################################################################################################ 
# Function to generate salesforce connnection properties
###########################################################################################################################################################
def salesforce_connection(resource = 'sf_org_primary', environment = None):
    """ Function to generate salesforce connnection properties
        Parameters:
            :resource (string): represents the type of account (sf_org_primary, sf_org_secondary, sf_org_tertiary),
        Returns:
            :(list): It returns the list with salesforce url, connection properties, salesforce session instance.
    """
    # generating connection properties based on resouce
    if(resource.lower() == 'sf_org_primary'):
        #Secrets for sf_org_primary salesforce connection
        try:
            sales_username = dbutils.secrets.get("secret-scope", "sales-username")
        except Exception as e:
            print(f"Error retrieving secret for sales-username: {e}")
        try:
            sales_password = dbutils.secrets.get("secret-scope", "sales-password")
        except Exception as e:
            print(f"Error retrieving secret for sales-password: {e}")
        connectionProperties = {
        "username" : sales_username,
        "password" : sales_password,
        }
    elif(resource.lower() == 'sf_org_secondary'):
        #Secrets for sf_org_secondary salesforce connection
        try:
            sf_secondary_username = dbutils.secrets.get("secret-scope", "sf-secondary-username")
        except Exception as e:
            print(f"Error retrieving secret for sf-secondary-username: {e}")
        try:
            sf_secondary_password = dbutils.secrets.get("secret-scope", "sf-secondary-password")
        except Exception as e:
            print(f"Error retrieving secret for sf-secondary-password: {e}")
        connectionProperties = {
        "username" : sf_secondary_username,
        "password" : sf_secondary_password,
        }
    elif(resource.lower() == 'sf_org_tertiary'):
        #Secrets for worldcheck salesforce connection
        try:
            wc1_username = dbutils.secrets.get("secret-scope", "wc1-username")
        except Exception as e:
            print(f"Error retrieving secret for wc1-username: {e}")
        try:
            wc1_password = dbutils.secrets.get("secret-scope", "wc1-password")
        except Exception as e:
            print(f"Error retrieving secret for wc1-password: {e}")
        connectionProperties = {
        "username" : wc1_username,
        "password" : wc1_password,
        }
    else:
        raise TypeError("Please enter a valid resource name")
    #Setting environment URL
    if (environment == "env_dev"):
        url ="https://test.salesforce.com/"
    elif (environment == "env_uat"):
        url ="https://test.salesforce.com/"
    elif (environment == "env_prd"):
        url ="https://login.salesforce.com/"
    else:
        raise TypeError("Please Enter a valid Environment.")

    # Creating the session object        
    salesf_domain = url.split('//')[1].split('.')[0]
    session_id, instance = SalesforceLogin(username=connectionProperties['username'], password= connectionProperties['password'], domain=salesf_domain)
    salesfWrite = Salesforce(instance=instance, session_id=session_id)
    print(f'[{GetTime(strform = "%Y-%m-%d %H:%M:%S")}] You are now connecting to {resource} using the url {url}')
    return [url, connectionProperties, salesfWrite]

############################################################################################################################################################ 
# Function to read the object data from salesforce
###########################################################################################################################################################
def salesforce_read(connectionProperties, salesf_query, schema):
    """Function to read the object data from salesforce
       Parameters:
            :connectionProperties (list): contains the list of salesforce url, connection properties, salesforce session instance,
            :salesf_query (string): SOQL query which needs to execute on salesforce object,
            :schema (StructType) : Contains the schema of the query results, will be used for generating dataframe from query result,
        Returns:
            :sparkDF (dataframe): dataframe contains the SOQL results.
    """
    salesfQuery = connectionProperties[2]
    query_result = salesfQuery.query_all(salesf_query)
    
    if (query_result['totalSize'] != 0):
        # Create pandas DataFrame from query results, dropping the attributes column
        data = pd.DataFrame(query_result['records']).drop(columns='attributes')
        # Convert pandas DataFrame to Spark DataFrame using the provided schema
        sparkDF = spark.createDataFrame(data=data, schema=schema)
    else:
        # Create an empty DataFrame with the provided schema instead of using emptyRDD
        sparkDF = spark.createDataFrame([], schema)
    
    # Cache and count the DataFrame
    sparkDF = sparkDF.cache()
    df_count = sparkDF.count()
    
    print(f'[{GetTime(strform = "%Y-%m-%d %H:%M:%S")}] {df_count} rows have been read')
    return sparkDF


############################################################################################################################################################ 
# ---->  ((((  With Exception Handling and Error Logs Generation   ))))
###########################################################################################################################################################

def salesforce_write(connectionProperties, df, sfObject, operation, load_type='bulk', object_pk='Id', batch_size="auto", set_to_null='False', use_serial=False):
    """ Function to insert, update and upsert salesforce objects
        Parameters:
            :connectionProperties (list): contains the list of salesforce url, connection properties, salesforce session instance,
            :df (dataframe): dataframe contains the records to be loaded to salesforce object,
            :sfObject (string): name of the object, on which required operation will be executed,
            :load_type (string): Contains the name of the load type(bulk or standard type). Default value is 'bulk',
            :object_pk (string) : contains the name of the object primary key column name. Mainly used for upsert operation. Default value is 'Id',
            :batch_size (string or integer): Accepts either string or integer. By default batch size set to auto
                                             If any specific batch size needed, can be passed as integer value(Ex: 10000),
            :set_to_null (string): To specify whether null values to be loaded to salesforce object or not
                                   Default value is false, which will not load the null values to salesforce object.
            :use_serial (boolean): Whether to use serial processing for bulk operations. Default is False.
    """    
    
    total_records = df.count()
    # If number of records zero, will return without any action    
    if (total_records == 0):
        print(f'[{GetTime(strform = "%Y-%m-%d %H:%M:%S")}] Zero records present in the dataframe, {operation} operation will not processed')
        return
    # if total number of records equal to one, setting load type as standard as bulk will not work for single record.
    if (total_records == 1):
        load_type='standard'

    print(f'[{GetTime(strform = "%Y-%m-%d %H:%M:%S")}] Total number of records considered for {operation} {load_type} operation on {sfObject} object: {total_records}')
    
    
    rows = df.collect()
    columns = df.columns
    
    # Convert rows to dictionaries manually
    data = []
    for row in rows:
        record = {}
        for i, column in enumerate(columns):
            record[column] = row[i]
        data.append(record)
    
    if (set_to_null.lower() == 'false'):
        data = remove_null_from_dictionary(data)
        
    salesfWrite = connectionProperties[2]
    failed_records = []
                                          
    Error_Msg_Column = 'Error_Message'
    
    if (load_type.lower() == 'standard'):
        salesfWriteObject = getattr(salesfWrite, sfObject)
        if(operation.lower() == 'insert'):
            for record in data:
                try:
                    result = salesfWriteObject.create(record)
                except Exception as err:
                    record[Error_Msg_Column] = f'{err =}'
                    failed_records.append(record)
        elif(operation.lower() == 'update'):
            for record in data:
                try:
                    incoming_record = record.copy()
                    primaryKey=record.pop(object_pk)
                    result = salesfWriteObject.update(primaryKey, record)
                except Exception as err:
                    incoming_record[Error_Msg_Column] = f'{err =}'
                    failed_records.append(incoming_record)        
        elif(operation.lower() == 'upsert'):
            for record in data:
                try:
                    incoming_record = record.copy()
                    external_id = format_external_id(object_pk, record.pop(object_pk))
                    result = salesfWriteObject.upsert(external_id, record)
                except Exception as err:
                    incoming_record[Error_Msg_Column] = f'{err =}'
                    failed_records.append(incoming_record)        
        elif(operation.lower() == 'delete'):
            pass
            # for record in data:
            #     result = salesfWriteObject.delete(record[object_pk])
        else:
            raise TypeError("Please Enter a valid write operation")
    
    elif (load_type.lower() == 'bulk'):
        salesfWriteObject=getattr(salesfWrite.bulk, sfObject)
        if(operation.lower() == 'insert'):
            results = salesfWriteObject.insert(data, batch_size, use_serial)
        elif(operation.lower() == 'update'):
            results = salesfWriteObject.update(data, batch_size, use_serial)
        elif(operation.lower() == 'upsert'):
            # Filter records with null and non-null primary key separately
            df_id_null = df.filter(col(object_pk).isNull())
            df_id_not_null = df.filter(col(object_pk).isNotNull())
            
            # Convert null ID rows to dictionaries
            null_rows = df_id_null.drop(object_pk).collect()
            null_columns = df_id_null.drop(object_pk).columns
            data_id_null = []
            for row in null_rows:
                record = {}
                for i, column in enumerate(null_columns):
                    record[column] = row[i]
                data_id_null.append(record)
            
            # Convert non-null ID rows to dictionaries
            non_null_rows = df_id_not_null.collect()
            non_null_columns = df_id_not_null.columns
            data = []
            for row in non_null_rows:
                record = {}
                for i, column in enumerate(non_null_columns):
                    record[column] = row[i]
                data.append(record)
            
            # Combine both sets of records
            data += data_id_null            
            
            results = salesfWriteObject.upsert(data, object_pk, batch_size, use_serial)
        elif(operation.lower() == 'delete'):
            pass
            #results = salesfWriteObject.delete(data, batch_size, use_serial)
        else:
            raise TypeError("Please Enter a valid write operation")        
        
        for result, record in zip(results, data):
            if (result['success'] == False):
                record[Error_Msg_Column] = result['errors']
                failed_records.append(record)
    else:
        raise TypeError("Please Enter a valid load type")
    
    # Create failed records DataFrame
    if len(failed_records) > 0:
        # Convert failed records to rows
        failed_rows = []
        for record in failed_records:
            row = []
            # Add all columns from original schema
            for column in df.columns:
                if column in record:
                    row.append(record[column])
                else:
                    row.append(None)
            # Add error message column
            row.append(record.get(Error_Msg_Column, None))
            failed_rows.append(tuple(row))
        
        # Create schema for failed records
        failed_schema = df.schema.add(Error_Msg_Column, StringType(), True)
        
        # Create DataFrame with failed records
        df_failed_records = spark.createDataFrame(data=failed_rows, schema=failed_schema)
    else:
        # Create an empty DataFrame with the schema
        failed_schema = df.schema.add(Error_Msg_Column, StringType(), True)
        df_failed_records = spark.createDataFrame(data=[], schema=failed_schema)
    
    generate_failed_records_file(df_failed_records, operation)
    print(f'[{GetTime(strform = "%Y-%m-%d %H:%M:%S")}] Operation successfully executed.')
    return

###########################################################################################################################################################
# Function to generate bad records log file
###########################################################################################################################################################
def generate_failed_records_file(df_FR, operation):
    """Function to generate bad records log file
        Parameters:
            :df_FR (dataframe): Dataframe contains the failed reocrds,
            :operation (string): Indicates the type of operation performed,
        Returns:
            :filename (string): Contains the filename of the failed reocrds.
    """
    failed_records_count = df_FR.count()
    if (failed_records_count == 0):
        print(f'[{GetTime(strform = "%Y-%m-%d %H:%M:%S")}] {failed_records_count} records failed for {operation} operation')
        return
    blob_conn = blob_connection()
    notebook_name = get_notebook_name()
    chicago_time = GetTime(strform = "%Y_%m_%d_%H_%M_%S")
    run_date = GetTime(strform= '%Y_%m_%d')
    filename = f'{operation.upper()}_ERROR_RECORDS_{notebook_name}_{chicago_time}.csv'
    folder_path = f'/Job_Logs/Error_Logs/{notebook_name}/{run_date}'
    blob_write(df_FR, filename, folder_path, blob_conn, header = 'true')
    print(f'[{GetTime(strform = "%Y-%m-%d %H:%M:%S")}] {failed_records_count} records failed for {operation} operation. Please refer <{filename}> file in blob location <{folder_path}>')
    return filename

# COMMAND ----------

# MAGIC %md
# MAGIC ##Databricks Catalog

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC def catalog_connection(conn_profile, environment = None):
# MAGIC
# MAGIC def catalog_read(connection_info, catalog_name, schema_name, table_name, conditional_query = None):
# MAGIC
# MAGIC def catalog_query(connection_info, mode, catalog_name, db_schema, table_name, col_list = '', custom_query = '', spark_df=''):
# MAGIC
# MAGIC def catalog_swap_tables(connection_info, catalog_name='', db_schema='', Main_table='',Stage_table=''):
# MAGIC
# MAGIC

# COMMAND ----------

def catalog_connection(conn_profile, environment = None):
    conn_profile = conn_profile.lower()
    environment = environment.lower()
 
    if conn_profile == "none":
        return None
   
    if environment == "env_dev":
        table_name = "[int_audit].[catalog_connection_config_dev]"
    else:
        table_name = "[int_audit].[catalog_connection_config]"
   
    # get the details of connection profile
    conn_info = onprem_connection('INTEGRATION_DB', "integration_stage")
    query = f"SELECT * FROM {table_name} WHERE ConnectionProfile = '{conn_profile}'"
 
    try:
        config_df = onprem_query_read(connection_info=conn_info, query=query)
        server_hostname = config_df.select('ServerHostname').first()[0]
        http_path = config_df.select('HttpPath').first()[0]
        scope_key = config_df.select('ScopeKey').first()[0]
    except Exception as e:
        print(f"Error while fetching the config table. Please check if the connection profile is correct.")
   
    # connect to catalog
    try:
        connection = sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=dbutils.secrets.get("secret-scope", scope_key)
        )
        print(f'Connected to {conn_profile} profile')
        return connection
    except Exception as e:
        print(f"Error while establishing connection: {str(e)}")
 
 
def catalog_read(connection_info = None, catalog_name = None, schema_name = None, table_name = None, conditional_query = None, conditional_filter = None):
    """
    Reads a table from Unity Catalog using either Spark's DataFrame API (preferred), or through the legacy catalog connection.
 
    Parameters:
        connection_info (object, optional): Legacy database connection object. Required only for legacy jobs. Not required for Spark jobs.
        catalog_name (str): The name of the catalog.
        schema_name (str): The name of the schema.
        table_name (str): The name of the table.
        conditional_query (str, optional): SQL WHERE clause for legacy backend (e.g., "WHERE amount > 1000").
        conditional_filter (str, optional): Spark DataFrame filter expression (e.g., "amount > 1000").
 
    Returns:
        DataFrame: Spark DataFrame containing the queried data.    
    """
   
    if not all([catalog_name, schema_name, table_name]):
        print("WARNING: catalog_name, schema_name, and table_name cannot be None or empty.")
        return
    if connection_info is None and conditional_query is not None and conditional_filter is not None:
        print("WARNING: Provide either conditional_query or conditional_filter.")
        return
    if connection_info is not None and conditional_query is None and conditional_filter is not None:
        print("WARNING: Provide either conditional_query or do not provide connection_info.")
        return
 
    df = None
    view_name = f'{catalog_name}.{schema_name}.{table_name}'
 
    if connection_info is not None:
        # (legacy method)
        cursor = None
        try:
            cursor = connection_info.cursor()   # Connect to the database
            cursor.columns(catalog_name, schema_name, table_name) # Get the metadata of the source table
            metadata = cursor.fetchall()
            schema = StructType()   # Define the schema using the metadata
 
            for row in metadata:
                column_name = row.COLUMN_NAME
                column_type = get_spark_data_type(row.TYPE_NAME)
                is_nullable = row.IS_NULLABLE == 'YES'
                schema.add(StructField(column_name, column_type, is_nullable))
 
            # Execute the query
            if conditional_query:
                sql_query = f'SELECT * FROM {view_name} {conditional_query}'
            else:
                sql_query = f'SELECT * FROM {view_name}'
 
            print(f"Starting to read from {view_name}")
            cursor.execute(sql_query)
            query_data = cursor.fetchall()
   
            # Create DataFrame
            df = spark.createDataFrame(query_data, schema)
        except Exception as e:
            print(f"ERROR: {e}")
            return
        finally:
            # Clean up resources
            if cursor:
                try:
                    cursor.close()
                except Exception as e:
                    print(f"Error closing cursor: {e}")
                    return
       
        print(f"Data was read successfully from {view_name}")
    else:
        # new method        
        # Validate table exists
        if not spark.catalog.tableExists(view_name):
            print(f"ERROR: Table {view_name} does not exist.")
            return
        print(f"Starting to read from {view_name}")
        try:
            if conditional_filter:
                df = spark.read.table(view_name).filter(conditional_filter)  # Apply filter if provided
            elif conditional_query:
                df = spark.sql(f"SELECT * FROM {view_name} {conditional_query}")  # Apply query if provided
            else:
                df = spark.read.table(view_name)  # Read table
        except Exception as e:
            print(f"ERROR: {e}")
            return
        print(f"Data was read successfully from {view_name}")
 
    return df


def get_spark_data_type(type_name):
    # Regular expression to match DECIMAL(precision, scale)
    decimal_regex = r'DECIMAL\((\d+),(\d+)\)'

    # Check for matching decimal pattern
    decimal_match = re.match(decimal_regex, type_name)
    if decimal_match:
        # Extract precision and scale from DECIMAL type
        precision, scale = map(int, decimal_match.groups())
        return DecimalType(precision, scale)

    data_type_mapping = {
        'INT': IntegerType(),
        'STRING': StringType(),
        'TIMESTAMP': TimestampType(),
        'DATE': DateType()
    }

    # Return the corresponding data type from the mapping
    return data_type_mapping.get(type_name, StringType())  # default to StringType if type is unknown

def catalog_query(connection_info, mode, catalog_name, db_schema, table_name, spark_df='', custom_query = '', batch_size = 1000):
    try:
        cursor = connection_info.cursor()
        print("Executing: " + mode)
        if(mode.lower() == "insert"):
            columns = spark_df.columns
            for column in spark_df.columns:
                spark_df = spark_df.withColumn(column,regexp_replace(col(column), "--", "-"))
            insert_table_name = f'{catalog_name}.{db_schema}.{table_name}'
            rows = spark_df.collect()
            for i in range(0, len(rows), batch_size):
                ##Loop Begins
                batch = rows[i:i + batch_size]

                # Create a single INSERT statement for the batch
                values = [f"({','.join(['%s' for _ in columns])})" for _ in range(len(batch))]
                sql = f"INSERT INTO {insert_table_name} ({','.join(columns)}) VALUES {','.join(values)}"

                # Flatten the data for this batch
                data = [value for row in batch for value in (row[col] for col in columns)]

                cursor.execute(sql, data)
                # print(f":{len(batch)}: records Inserted in this batch!")
                ##Loop Ends
            print(f":{len(rows)}: records Inserted in {insert_table_name} table!!")
        if(mode.lower() == "update"):
            #UPDATE
            print("UPDATE under development")
        if(mode.lower() == "upsert"):
            #UPSERT
            print("UPSERT under development")
        if(mode.lower() == "delete"):
            #DELETE
            # REQUIRES: catalog_name, db_schema, table_name, spark_df, custom_query
            pandas_df = spark_df.toPandas()
            # Add the rows to the table
            for _, row in pandas_df.iterrows():
                values = row.values.tolist()
                values_str = ", ".join([f"'{value}'" for value in values])
                query = f" DELETE FROM {catalog_name}.{db_schema}.{table_name} WHERE {custom_query} in ({values_str})"
                cursor.execute(query)
        if(mode.lower() == "custom"):
            #CUSTOM
            cursor.execute(custom_query)
            return cursor.fetchall()
        if(mode.lower() == "truncate_and_load"):
            # TRUNCATE AND LOAD
            truncate_query = f'TRUNCATE TABLE {catalog_name}.{db_schema}.{table_name}'
            catalog_query(connection_info, mode='custom', catalog_name=catalog_name, db_schema=db_schema, table_name=table_name, custom_query=truncate_query)
            catalog_query(connection_info, mode='insert', catalog_name=catalog_name, db_schema=db_schema, table_name=table_name, spark_df=spark_df, batch_size=batch_size)
    except Exception as e:
        print(f"ERROR: {e}")
        raise TypeError(f'error occured while doing {mode} operation')
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception as e:
                print(f"Error closing cursor: {e}")

def catalog_swap_tables(connection_info, catalog_name='', db_schema='', Main_table='',Stage_table=''):
    """
    Swap two tables in a database catalog.

    This function renames the Main table to a temporary name, then renames the Stage table
    to the Main table name, and finally renames the temporary table to the Stage table name.
    This effectively swaps the two tables.

    Args:
        connection_info (object): A database connection object with a cursor() method.
        catalog_name (str): The name of the database catalog.
        db_schema (str): The name of the database schema.
        Main_table (str): The name of the main table to be swapped.
        Stage_table (str): The name of the stage table to be swapped.

    Returns:
        None

    Raises:
        Exception: If any error occurs during the table swapping process.

    Note:
        This function assumes that the connection_info object has a cursor() method
        that returns a cursor object capable of executing SQL commands.
    """
    cursor = connection_info.cursor()
    final_main_table=f"{catalog_name}.{db_schema}.{Main_table}"
    final_Stage_table=f"{catalog_name}.{db_schema}.{Stage_table}"
    final_temp_table=f"{catalog_name}.{db_schema}.swap_{Stage_table}"

    try:
        # Rename table1 to a temporary name
        print(f"Renaming {final_main_table} to {final_temp_table}...")
        cursor.execute(f"ALTER TABLE {final_main_table} RENAME TO {final_temp_table}")
        print(f"Rename comlpeted successfully {final_main_table} to {final_temp_table}!!")

        print(f"Renaming {final_Stage_table} to {final_main_table}...")
        # Rename table2 to table1
        cursor.execute(f"ALTER TABLE {final_Stage_table} RENAME TO {final_main_table}")
        print(f"Renamed {final_Stage_table} to temp_{final_main_table}!!")

        print(f"Renaming {final_temp_table} to {final_Stage_table}...")
        # Rename the temporary table to table2
        cursor.execute(f"ALTER TABLE {final_temp_table} RENAME TO {final_Stage_table}")
        print(f"Renamed {final_temp_table} to {final_Stage_table}!!")

        
        print(f"Tables {Main_table} and {Stage_table} swapped successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")
        raise 

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Extraction

# COMMAND ----------

# MAGIC %md
# MAGIC def view_extraction(jobName="",batchProcess = 'Y')
# MAGIC
# MAGIC def view_extraction_audit_check(jobName="", batchProcess='Y')

# COMMAND ----------

def view_extraction(jobName="", batchProcess = 'Y'):
   
    # Establish connection and read job_run_config for the job
    env_set = onprem_connection('INTEGRATION_DB', "integration_stage")
    dt_config_tv = onprem_query_read(connection_info = env_set, query = f"Select * from [integration_stage].[int_audit].[job_run_config] where IsActive = 'Y' and JobName = '{jobName}'")

    # Fail previous run if it's a source error before rerunning the job.
    cust_q = f"UPDATE [integration_stage].[int_audit].[file_log] set JobStatus = 'FAILED' where JobName = '{jobName}' and FileTransferStatus = 'Pending' and JobStatus ='FINISHED' and StatusMessage like '%Source Error%' "
    onprem_query_execute(env_set, cust_q)

    # Discard any previous runs before rerunning the job on same day.
    cust_q = f"UPDATE [integration_stage].[int_audit].[file_log] set JobStatus = 'DISCARDED' where JobName = '{jobName}' and FileTransferStatus = 'Pending' and JobStatus IN ('RUNNING', 'FINISHED')"
    onprem_query_execute(env_set, cust_q)

    
    # Process each job step based on the configuration
    for row in dt_config_tv.collect():
        # Initialize variables for each job step
        df_data = None
        column_names = None
        new_column_names = None
        delimiter= None
        header= None
        quote_qualifier = None
        incremental_column = None
        onprem_query = None
        fileIncludeFlag = 'Y'
        df_data_count = 0
        fileSize = '0'
        status = "Success"
        status_msg = ''
        full_data_count = 0
        escape_character = None

        # Extract job step details from the configuration
        jobName = row.JobName
        jobStep = row.JobStep
        jobSubstep = row.JobSubstep
        stepType = row.StepType
        connectionProfile = row.ConnectionProfile
        databaseName = row.DatabaseName
        schemaName = row.SchemaName
        tableName = row.TableName
        folderPath = row.FolderPath
        fileName = row.FileName
        columnRename = row.ColumnRename.upper()
        incrementalExtract = row.Incremental.upper()
        thresholdExtract = row.Threshold.upper()
        nullcheck = row.NullCheck.upper()
    

        # Initialize job status and other variables
        jobstatus = 'RUNNING'
        ZeroRecordFlag = ''
        maxRecordDateTime = ''

        # Read additional parameters for the job step
        dt_config_params_tv = onprem_query_read(connection_info = env_set, query =f"Select * from [integration_stage].[int_audit].[job_run_config_params] where JobName = '{jobName}' and JobStep = '{jobStep}' and StepType = '{stepType}'")
    

        # Store parameter configurations in a dictionary for easy access
        parameters_dict = {param_row['ParameterName']: param_row['ParameterValue'] for param_row in dt_config_params_tv.collect()}
       
        # Extract parameters from the dictionary
        if 'ColumnNames' in parameters_dict:
            column_names = parameters_dict['ColumnNames']
            
        if 'NewColumnNames' in parameters_dict:
            new_column_names = parameters_dict['NewColumnNames']
                       
        if 'Delimiter' in parameters_dict:
            delimiter = parameters_dict['Delimiter']
            
        if 'Header' in parameters_dict:
            header = parameters_dict['Header']
            
        if 'QuoteQualifier' in parameters_dict:
            quote_qualifier = parameters_dict['QuoteQualifier']
            
        if 'IncrementalColumn' in parameters_dict:
            incremental_column = parameters_dict['IncrementalColumn']
            
        if 'FileExtension' in parameters_dict:
            FileExtension = parameters_dict['FileExtension']
            
        if 'OnpremQuery' in parameters_dict:
            onprem_query = parameters_dict['OnpremQuery']
        
        if 'EscapeCharacter' in parameters_dict:
            escape_character = parameters_dict['EscapeCharacter']
        
        # Process data based on the step type (catalog or on-premises)
        if (stepType.lower() == "catalog"): 
            # FULL FILE GENERATION when batch process ='Y'
            if incrementalExtract == "N" or incremental_column == "":
                try:
                    env_set_catalog = catalog_connection(connectionProfile)
                    df_data = catalog_read(env_set_catalog,databaseName,schemaName,tableName)
                    df_data.cache()
                    full_data_count = df_data.count()
                    df_data.unpersist()
                except Exception as e:
                    status_msg = f"Source Error: No Profile Found: {e}"
                    print(status_msg)
                    status = 'Failed'
                    ZeroRecordFlag = 'NA'
                    jobstatus = 'FAILED'

            # INCREMENTAL FILE GENERATION when batch process ='N'
            elif incrementalExtract == "Y" and incremental_column != None:
                print("incremental load started")
                # Read MaxRecordDateTime from LOG
                last_run = onprem_query_read(connection_info = env_set, query =f"Select TOP 1 MaxRecordDateTime from [integration_stage].[int_audit].[file_log] where JobName = '{jobName}' and JobStep = '{jobStep}' and FileGenerationStatus = 'Success' and FileTransferStatus = 'Success' order by MaxRecordDateTime desc")

                last_run_row = last_run.collect()[0] if last_run.count()>0  else ''
                # Get the GenerationTS value and format it properly
                maxRecordDateTime = last_run_row['MaxRecordDateTime'].strftime('%Y-%m-%d %H:%M:%S') if last_run_row != '' else ''

                # Prepare the custom query suffix
                if (maxRecordDateTime == ''):
                    try:
                        env_set_catalog = catalog_connection(connectionProfile)
                        df_data = catalog_read(env_set_catalog,databaseName,schemaName,tableName)
                        df_data.cache()
                        full_data_count = df_data.count()
                        df_data.unpersist()
                    except Exception as e:
                        status_msg = f"Source Error: Error while reading full catalog: {e}"
                        print(status_msg)
                        status = 'Failed'
                        ZeroRecordFlag = 'NA'
                        jobstatus = 'FAILED'
                else:
                    incremental_cols = incremental_column.split(',')
                    newtimestamp = datetime.strptime(maxRecordDateTime, '%Y-%m-%d %H:%M:%S') + timedelta(seconds=1)
                    incremental_filter_conditions = '(' + ' or '.join(
                            [f'({incremental_col} > "{newtimestamp}")' for incremental_col in incremental_cols]
                                                        ) + ')'
                    full_cus_query = f"where {incremental_filter_conditions};"
                    print(f"Custom query: {full_cus_query}")

                    source_count_query = f"select count(*) from {databaseName}.{schemaName}.{tableName}"
                    print(f"Source count query: {source_count_query}")
                    
                    try:
                        env_set_catalog = catalog_connection(connectionProfile)
                        df_data = catalog_read(env_set_catalog, databaseName, schemaName, tableName, full_cus_query)                        

                    except Exception as e:
                        status_msg = f"Source Error: Error while reading incremental catalog: {e}"
                        print(status_msg)
                        status = 'Failed'
                        ZeroRecordFlag = 'NA'
                        jobstatus = 'FAILED'

                    try:
                        source_data_count = catalog_query(env_set_catalog, mode='custom', catalog_name  = databaseName, db_schema = schemaName, table_name = tableName, custom_query = source_count_query )
                        full_data_count = source_data_count[0][0]
                        print(f"Source full data count: {full_data_count}")
                    except Exception as e:
                        status_msg = f"Source Error: Error while getting source record count from catalog: {e}"
                        print(status_msg)
                        status = 'Failed'
                        ZeroRecordFlag = 'NA'
                        jobstatus = 'FAILED'
                        
            else:
                status_msg = "Invalid value for Incremental in Config Table OR incremental_column in Param Table"
                print(status_msg)
                status = 'Failed'
                ZeroRecordFlag = 'NA'
                jobstatus = 'FAILED'
        
        # Process data based on the step type (catalog or on-premises)      
        if (stepType.lower() == "onprem") :
            if onprem_query is None:
                try:
                    env_set_onprem = onprem_connection(connectionProfile, databaseName)
                    df_data = onprem_read(tableName, env_set_onprem, schemaName)
                except Exception as e:
                    status_msg = f"Error while reading onprem : {e}"
                    print(status_msg)
                    status = 'Failed'
                    ZeroRecordFlag = 'NA'
                    jobstatus = 'FAILED'

            elif onprem_query!= None:
                try:
                    env_set_onprem = onprem_connection(connectionProfile, databaseName)
                    df_data = onprem_query_read(env_set_onprem,onprem_query)
                except Exception as e:
                    status_msg = f"Error while reading custom query : {e}"
                    print(status_msg)
                    status = 'Failed'
                    ZeroRecordFlag = 'NA'
                    jobstatus = 'FAILED'

            else:
                status_msg = f"Error: No Profile found: {e}"
                print(status_msg)
                status = 'Failed'
                ZeroRecordFlag = 'NA'
                jobstatus = 'FAILED'
        
        # Snowflake
        if (stepType.lower() == "snowflake"): 
            # FULL FILE GENERATION when batch process ='Y'
            if incrementalExtract == "N" or incremental_column == "":
                try:
                    sfOptions = snow_conection( sfSchema = cur_env, snowdb = databaseName, sfProfile= schemaName )
                    df_data =  snow_read(tableName,sfOptions, SNOWFLAKE_SOURCE_NAME="snowflake")
                    df_data.cache()
                    full_data_count = df_data.count()
                    df_data.unpersist()
                except Exception as e:
                    status_msg = f"Source Error: No Profile Found: {e}"
                    print(status_msg)
                    status = 'Failed'
                    ZeroRecordFlag = 'NA'
                    jobstatus = 'FAILED'

            # INCREMENTAL FILE GENERATION when batch process ='N'
            elif incrementalExtract == "Y" and incremental_column != None:
                print("incremental load started")
                # Read MaxRecordDateTime from LOG
                last_run = onprem_query_read(connection_info = env_set, query =f"Select TOP 1 MaxRecordDateTime from [integration_stage].[int_audit].[file_log] where JobName = '{jobName}' and JobStep = '{jobStep}' and FileGenerationStatus = 'Success' and FileTransferStatus = 'Success' order by MaxRecordDateTime desc")

                last_run_row = last_run.collect()[0] if last_run.count()>0  else ''
                # Get the GenerationTS value and format it properly
                maxRecordDateTime = last_run_row['MaxRecordDateTime'].strftime('%Y-%m-%d %H:%M:%S') if last_run_row != '' else ''

                # Prepare the custom query suffix
                if (maxRecordDateTime == ''):
                    try:
                        sfOptions = snow_conection( sfSchema = cur_env, snowdb = databaseName, sfProfile= schemaName )
                        df_data =  snow_read(tableName,sfOptions, SNOWFLAKE_SOURCE_NAME="snowflake")
                        df_data.cache()
                        full_data_count = df_data.count()
                        df_data.unpersist()
                    except Exception as e:
                        status_msg = f"Source Error: Error while reading full catalog: {e}"
                        print(status_msg)
                        status = 'Failed'
                        ZeroRecordFlag = 'NA'
                        jobstatus = 'FAILED'
                else:
                    incremental_cols = incremental_column.split(',')
                    newtimestamp = datetime.strptime(maxRecordDateTime, '%Y-%m-%d %H:%M:%S') + timedelta(seconds=1)
                    incremental_filter_conditions = '(' + ' or '.join(
                            [f'({incremental_col} >= "{newtimestamp}")' for incremental_col in incremental_cols]
                                                        ) + ')'
                    full_cus_query = f"where {incremental_filter_conditions} order by {incremental_column} desc;"
                    print(f"Custom query: {full_cus_query}")

                    source_count_query = f"select count(*) from {databaseName}.{schemaName}.{tableName}"
                    print(f"Source count query: {source_count_query}")
                    
                    try:
                        sfOptions = snow_conection( sfSchema = cur_env, snowdb = databaseName, sfProfile= schemaName )
                        df_data =  snow_query_read(full_cus_query, sfOptions, SNOWFLAKE_SOURCE_NAME="snowflake")                 

                    except Exception as e:
                        status_msg = f"Source Error: Error while reading incremental catalog: {e}"
                        print(status_msg)
                        status = 'Failed'
                        ZeroRecordFlag = 'NA'
                        jobstatus = 'FAILED'

                    try:
                        source_data_count = snow_query_read(source_count_query, sfOptions,SNOWFLAKE_SOURCE_NAME="snowflake" )
                        full_data_count = source_data_count[0][0]
                        print(f"Source full data count: {full_data_count}")

                    except Exception as e:
                        status_msg = f"Source Error: Error while getting source record count from catalog: {e}"
                        print(status_msg)
                        status = 'Failed'
                        ZeroRecordFlag = 'NA'
                        jobstatus = 'FAILED'
                        
            else:
                status_msg = "Invalid value for Incremental in Config Table OR incremental_column in Param Table"
                print(status_msg)
                status = 'Failed'
                ZeroRecordFlag = 'NA'
                jobstatus = 'FAILED'

        # Data processing and file generation
        if df_data:
            df_data_count = df_data.count() 
            if df_data_count > 0 and incrementalExtract == "Y": 
                datetime_columns = incremental_column.split(',')
                # Initialize the first column as max_datetime
                df_data = df_data.withColumn("max_datetime", df_data[datetime_columns[0]])
                # Compare and replace the max_datetime with the greater datetime value
                for column in datetime_columns[1:]:
                    df_data = df_data.withColumn("max_datetime", F.greatest(df_data["max_datetime"], df_data[column]))
                # Calculate the overall max datetime from 'max_datetime' column
                maxRecordDateTime = df_data.agg(F.max("max_datetime")).collect()[0][0]
                try:
                    maxRecordDateTime = maxRecordDateTime.strftime('%Y-%m-%d %H:%M:%S') if maxRecordDateTime != '' else ''
                except:
                    print("Warning: Unable to convert strftime to the proper format")
            if df_data_count > 0 and incrementalExtract == "N":
                 maxRecordDateTime = ''

            if column_names:
                #Select only provided columns
                cols_list = column_names.split(',')
                try:
                    df_data = df_data.select(cols_list)
                    # Renaming columns
                    if columnRename == 'Y':
                        if new_column_names:
                            new_cols_list = new_column_names.split(',')
                            try:
                                df_data = df_column_rename(df_data, cols_list, new_cols_list)
                            except Exception as e:
                                status_msg = "Please check the number of columns in old and new list. Both should be having equal number in parameter table"
                                print(status_msg)
                                status = "Failed"
                                jobstatus = "FAILED"
                        else:
                            status_msg = "New column names not defined in the parameter table."
                            print(status_msg)
                            status = "Failed"
                            jobstatus = "FAILED"
                except Exception as e:
                    status_msg = "Source Error:Column not found in the source table"
                    print(status_msg)
                    status = "Failed"
                    jobstatus = "FAILED"
                          

            # Writing data into blob storage containers  
            try: 
                env_set_blob = blob_connection()
                blob_write(df_data, fileName, folderPath, env_set_blob, filetype = FileExtension, delimiter = delimiter, header = header, quoteQualifier = quote_qualifier, escape = escape_character)
                print(f'''Finished writing {fileName} with {df_data_count} rows.''')
                # check here 
                if status != "Failed":
                    status_msg = "File generation successful"
                else:
                    status_msg = "Source Error: validate the file in blob"
                                     
            except Exception as e:
                status_msg = f"Error while writing file to Blob: {e}"
                print(status_msg)
                status = "Failed"
                jobstatus = 'FAILED'

                        
            # Zero record flag 
            if(df_data_count == 0):
                ZeroRecordFlag = 'Y'
            elif(df_data_count > 0):
                ZeroRecordFlag = 'N'
            
            # Insert the stats into the audit table if data is available
            cust_q = f"INSERT INTO [integration_stage].[int_audit].[file_log] (JobName, JobStep, FolderPath, FileName, FileSize, RecordCount, StatusMessage, FileIncludeFlag, ZeroRecordFlag, MaxRecordDateTime, FileGenerationDateTime, FileGenerationStatus,FileTransferStatus, JobStatus,SourceRecordCount) VALUES ('{jobName}', '{jobStep}', '{folderPath}', '{fileName}','{fileSize}','{df_data_count}','{status_msg}','{fileIncludeFlag}', '{ZeroRecordFlag}', '{maxRecordDateTime}','{GetTime(strform='%Y-%m-%d %H:%M:%S')}', '{status}', 'Pending', 'RUNNING', '{full_data_count}')"
            onprem_query_execute(env_set, cust_q)

            print(f"File generated time has been updated in the timestamp table for {tableName}.\n")
        else: 
            # insert the audit table with the status as "There is no data to process" if no data available    
            cust_q = f"INSERT INTO [integration_stage].[int_audit].[file_log] (JobName, JobStep, FolderPath, FileName, FileSize, RecordCount, StatusMessage, FileIncludeFlag, ZeroRecordFlag, MaxRecordDateTime, FileGenerationDateTime, FileGenerationStatus,FileTransferStatus, JobStatus) VALUES ('{jobName}', '{jobStep}', '{folderPath}', '{fileName}','{fileSize}','{df_data_count}','There is no data to process','{fileIncludeFlag}', '{ZeroRecordFlag}', '{maxRecordDateTime}','{GetTime(strform='%Y-%m-%d %H:%M:%S')}', '{status}', 'Pending', 'RUNNING')"
            onprem_query_execute(env_set, cust_q)

        # Update the jobstatus as finished when batch process =N at jobstep level 
        if batchProcess == 'N':
            cust_q = f"UPDATE [integration_stage].[int_audit].[file_log] set JobStatus = 'FINISHED' where JobName = '{jobName}' and JobStep = '{jobStep}' and FileTransferStatus = 'Pending' and JobStatus = 'RUNNING' and FileGenerationDateTime = (select max(FileGenerationDateTime) from [integration_stage].[int_audit].[file_log] where JobName = '{jobName}' and JobStep = '{jobStep}')"
            onprem_query_execute(env_set, cust_q)
  
    print(jobstatus)

 
    if(batchProcess == 'Y'):
        # Update job status as finished when batch process =Y
        cust_q = f"UPDATE [integration_stage].[int_audit].[file_log] set JobStatus = 'FINISHED' where JobName = '{jobName}' and FileTransferStatus = 'Pending' and JobStatus = 'RUNNING'"
        onprem_query_execute(env_set, cust_q)

        # Check if there is any failure in the Filegenerationstatus and if yes raise an exception
        cust_q = f"select count(*) as record_count from [integration_stage].[int_audit].[file_log] where JobName = '{jobName}' and FileGenerationDateTime = (select max(FileGenerationDateTime) from [integration_stage].[int_audit].[file_log] where JobName = '{jobName}' and Filegenerationstatus='Failed' and jobstatus='FINISHED' and FileTransferStatus !='Not Processed')"
        df_count=onprem_query_read(env_set, cust_q)
        count_value = df_count.collect()[0]['record_count']    
        print(count_value)    
        if count_value > 0:
            raise Exception("Job has failed in an error state. Please review logs.")

# COMMAND ----------

def view_extraction_audit_check(jobName="", batchProcess='Y'):
   
    df_data_count = 0
    # CONFIG READ ---------------
    env_set = onprem_connection('INTEGRATION_DB', "integration_stage")

    # checking if the view extraction job is still running
    dt_log_tv=onprem_query_read(
        connection_info=env_set,
        query=f"Select count(*) AS record_count from [integration_stage].[int_audit].[file_log] where JobName = '{jobName}' and FileTransferStatus = 'Pending' and JobStatus = 'RUNNING'"
    )

    count_value = dt_log_tv.collect()[0]['record_count']
    # If the view extraction is in running status, then fail the job else proceed with audit check.
    if count_value > 0:
        return -1
    else:
        dt_config_tv = onprem_query_read(
            connection_info=env_set,
            query=f"Select * from [integration_stage].[int_audit].[job_run_config] where IsActive = 'Y' and JobName = '{jobName}'"
        )

        # Extract config information and Process each job step
        for row in dt_config_tv.collect():

            # Set variables with default values
            df_data = None
            column_names = None
            new_column_names = None
            primary_key = None
            delimiter = None
            header = None
            no_of_days = None
            fileIncludeFlag = 'Y'
            ZeroRecordFlag= 'N'
            df_data_count = 0
            fileSize = '0'
            threshold_percent = '15'
            record_count_threshold = 'N'
            full_data_count = 0

            # Assign values from the configuration row
            jobName = row.JobName
            jobStep = row.JobStep
            jobSubstep = row.JobSubstep
            stepType = row.StepType
            connectionProfile = row.ConnectionProfile
            databaseName = row.DatabaseName
            schemaName = row.SchemaName
            tableName = row.TableName
            folderPath = row.FolderPath
            fileName = row.FileName
            duplicateCheck = row.DuplicateCheck.upper()
            incrementalExtract = row.Incremental.upper()
            thresholdExtract = row.Threshold.upper()
            nullcheck = row.NullCheck.upper()
            ZeroRecordAudit = row.ZeroRecordCheck.upper() 
            NoDataCheck = row.NoDataCheck.upper()

            # PARAM READ ---------------
            dt_config_params_tv = onprem_query_read(
                connection_info=env_set,
                query=f"Select * from [integration_stage].[int_audit].[job_run_config_params] where JobName = '{jobName}' and JobStep = '{jobStep}' and StepType = '{stepType}'"
            )
            
            # Create a dictionary to store parameter configurations for the current step
            parameters_dict = {param_row['ParameterName']: param_row['ParameterValue'] for param_row in dt_config_params_tv.collect()}
            
            # Accessing parameters from parameters_dict:
            if 'ColumnNames' in parameters_dict:
                column_names = parameters_dict['ColumnNames']
                
            if 'NewColumnNames' in parameters_dict:
                new_column_names = parameters_dict['NewColumnNames']
                
            if 'PrimaryKey' in parameters_dict:
                primary_key = parameters_dict['PrimaryKey']
                
            if 'Delimiter' in parameters_dict:
                delimiter = parameters_dict['Delimiter']
                
            if 'Header' in parameters_dict:
                header = parameters_dict['Header']
                
            if 'FileExtension' in parameters_dict:
                FileExtension = parameters_dict['FileExtension']

            if 'NoOfDays' in parameters_dict:
                no_of_days = parameters_dict['NoOfDays']
            
            if 'ThresholdPercentage' in parameters_dict:
                threshold_percent = parameters_dict['ThresholdPercentage']
            
            if 'RecordCountThreshold' in parameters_dict:
                record_count_threshold = parameters_dict['RecordCountThreshold']
            
            print("file read started")

            status_message = ""
            JobStatus = "Passed"

            # Read the file from blob storage
            file_flag = 'Y'
        
            retry_index = 5
            while(retry_index > 0):
                try:
                    env_set_blob = blob_connection()
                    df_data = blob_read(
                        connection_info=env_set_blob,
                        folder=folderPath,
                        FileName=fileName,
                        filetype=FileExtension,
                        delimiter=delimiter,
                        header=header,
                        multiLine='true'
                    )
                    retry_index = 0
                    print(f'''File {fileName} read successfully''')
                except Exception as e:
                    retry_index = retry_index-1
                    time_delay(10, 'sec')
                    print("WARNING: Error while reading file from Blob. Retrying")
                    file_flag = 'N'
                    status_msg = f"Error while reading file from Blob: {e}"


            if file_flag == 'N':
                status_message = "Audit Failed - Error while reading file from Blob after 5 re-tries"
                JobStatus = 'Failed'   
                # Update the log table with the audit results
                cust_q = f"""update [integration_stage].[int_audit].[file_log] set FileSize = '0', StatusMessage = '{status_message}' where  JobName = '{jobName}' and FileTransferStatus = 'Pending' and JobStatus = 'FINISHED' and JobStep = {jobStep}"""
                onprem_query_execute(env_set, cust_q)                        
            
            path_for_size = f'''{folderPath}/{fileName}'''
            print('')
            print(path_for_size)

            if df_data:

                # Get File Size
                try:        
                    fileSize = blob_get_file_size(env_set_blob, path_for_size)
                    status_message = "Audit Passed"
                except Exception as e:
                    status_msg = f"Failed to get file size after 5 tries: {e}"
                    filesize = '0'
                    JobStatus = 'Failed'
                    status_message = "Audit Failed - Failed to get file size"
            

                # AUDIT 

                # No Data Check
                cust_q = f"""select SourceRecordCount as SourceRecordCount from [integration_stage].[int_audit].[file_log] where JobName = '{jobName}' and FileTransferStatus = 'Pending' and JobStatus = 'FINISHED' and JobStep = {jobStep} and FileGenerationDateTime = (select max(FileGenerationDateTime) from [integration_stage].[int_audit].[file_log] where JobName = '{jobName}' and FileTransferStatus = 'Pending' and JobStatus = 'FINISHED' and JobStep = {jobStep})"""

                full_data_count=onprem_query_read(env_set, cust_q)
                print(full_data_count.collect()[0][0])
                # No Data Check
                if NoDataCheck.upper() == 'Y':
                    if int(full_data_count.collect()[0][0]) == 0 :
                        status = "NoDataInSource"
                        if "Audit Failed" not in status_message:
                            status_message = "Audit Failed - " + status
                        else:
                            status_message += '|' + status
                        JobStatus = 'Failed'
                        print(status_message)
                    else:
                        status_msg = "No Data check passed."
                        if "Audit Failed" not in status_message:
                            status_message = "Audit Passed"
                        print(status_msg)

                # Duplicate Check
                if duplicateCheck == 'Y':
                    if primary_key:
                        primary_key_columns = primary_key.split(',')
                        duplicates = df_data.groupBy(primary_key_columns).count().filter(col('count') > 1)
                        has_duplicates = duplicates.count() > 0
                        if has_duplicates:
                            status_msg = "There are duplicates based on the primary key columns"
                            print(status_msg)
                            status = "DuplicatesFound"
                            if "Audit Failed" not in status_message:
                                status_message = "Audit Failed - " + status
                            else:
                                status_message += '|' + status
                            JobStatus = 'Failed'
                        else:
                            status_msg = "No duplicates found based on the primary key. Duplicate check passed."
                            if "Audit Failed" not in status_message:
                                status_message = "Audit Passed"
                            print(status_msg)
                    else:
                        status_msg = "Primary Key columns not defined in the parameter table."
                        print(status_msg)
                        status = "ConfigError"
                        if "Audit Failed" not in status_message:
                            status_message = "Audit Failed - " + status
                        else:
                            status_message += '|' + status
                        JobStatus = 'Failed'

                # Null Check
                if nullcheck == 'Y':
                    if primary_key:
                        primary_key_columns = primary_key.split(',')
                        null_counts = df_data.select([count(when(col(c).isNull(), c)).alias(c) for c in primary_key_columns]).collect()[0].asDict()
                        null_cols = [k for k, v in null_counts.items() if v > 0]
                        if null_cols:
                            status_msg = f"Null values detected in the primary key columns {', '.join(null_cols)}"
                            print(status_msg)
                            status = "NullsFound"
                            if "Audit Failed" not in status_message:
                                status_message = "Audit Failed - " + status
                            else:
                                status_message += '|' + status
                            JobStatus = 'Failed'
                        else:
                            status_msg = "No null values found based on the primary key. Nullcheck passed."
                            if "Audit Failed" not in status_message:
                                status_message = "Audit Passed"
                            print(status_msg)
                    else:
                        status_msg = "Primary Key columns not defined in the parameter table."
                        print(status_msg)
                        status = "ConfigError"
                        if "Audit Failed" not in status_message:
                            status_message = "Audit Failed - " + status
                        else:
                            status_message += '|' + status
                        JobStatus = 'Failed'

        
                # Zero Record Audit check for incremental files
                if incrementalExtract == "Y" and ZeroRecordAudit == "Y":
                    current_date = datetime.today()
                    no_of_days = int(no_of_days)
                    past_date = current_date - timedelta(days=no_of_days)
                    
                    log_df = onprem_query_read(connection_info = env_set, query = f"Select * from [integration_stage].[int_audit].[file_log] where JobName = '{jobName}' and FileTransferStatus = 'Success' and JobStatus = 'FINISHED' and JobStep = {jobStep} and FileGenerationDateTime >= '{past_date.strftime('%Y-%m-%d %H:%M:%S')}' and FileGenerationDateTime <= '{current_date.strftime('%Y-%m-%d %H:%M:%S')}' and FileIncludeFlag = 'Y'")                 
                    avg_file_size = log_df.select(col("RecordCount").cast("float")).agg({"RecordCount": "avg"}).collect()[0][0]
                    if avg_file_size == 0.0:
                        status_msg = f"For the current file {jobName}-{jobStep} there is no data from source from past {no_of_days} days."
                        print(status_msg)
                        status = "ZeroRecordAuditFailed"
                        if "Audit Failed" not in status_message:
                            status_message = "Audit Failed - " + status
                        else:
                            status_message += '|' + status
                        JobStatus = 'Failed'
                    else:
                        print(f"Audit check for zero record audit passed for the incremental load file {jobName}-{jobStep}")
                        if "Audit Failed" not in status_message:
                            status_message = "Audit Passed"

                # File size threshold check for full files
                if incrementalExtract == "N" and thresholdExtract == "Y":
                    current_date = datetime.today().date()
                    no_of_days = int(no_of_days)
                    past_date = current_date - timedelta(days=no_of_days)
                    log_df = onprem_query_read(connection_info = env_set, query = f"Select * from [integration_stage].[int_audit].[file_log] where JobName = '{jobName}' and FileTransferStatus = 'Success' and JobStatus = 'FINISHED' and JobStep = {jobStep} and FileGenerationDateTime >= '{past_date.strftime('%Y-%m-%d %H:%M:%S')}' and FileGenerationDateTime <= '{current_date.strftime('%Y-%m-%d %H:%M:%S')}' and FileIncludeFlag = 'Y'")
                    avg_file_size = log_df.select(col("FileSize").cast("float")).agg({"FileSize": "avg"}).collect()[0][0]
                    if avg_file_size is None:
                        status_msg = "First run no threshold check required."
                        print(status_msg)
                    else:
                        threshold_percent = int(threshold_percent)
                        lower_bound = (1 - threshold_percent / 100.0) * avg_file_size
                        upper_bound = (1 + threshold_percent / 100.0) * avg_file_size
                        if not lower_bound <= fileSize <= upper_bound:
                            status_msg = f"Current file size ({fileSize}) is not within the range of {threshold_percent}% from the average of the last {no_of_days} days."
                            print(status_msg)
                            status = "FileSizeThresholdFailed"
                            if "Audit Failed" not in status_message:
                                status_message = "Audit Failed - " + status
                            else:
                                status_message += '|' + status
                            JobStatus = 'Failed'
                        else:
                            status_msg = f"Threshold check passed. Current file size ({fileSize}) is within the range of {threshold_percent}% from the average of the last {no_of_days} days."
                            if "Audit Failed" not in status_message:
                                status_message = "Audit Passed"
                            print(status_msg)

                # Record count threshold check
                if record_count_threshold == 'Y':
                    df_data_count = df_data.count()
                    current_date = datetime.today().date()
                    no_of_days = int(no_of_days)
                    past_date = current_date - timedelta(days=no_of_days)
                    log_df = onprem_query_read(connection_info = env_set, query = f"Select * from [integration_stage].[int_audit].[file_log] where JobName = '{jobName}' and FileTransferStatus = 'Success' and JobStatus = 'FINISHED' and JobStep = {jobStep} and FileGenerationDateTime >= '{past_date.strftime('%Y-%m-%d %H:%M:%S')}' and FileGenerationDateTime <= '{current_date.strftime('%Y-%m-%d %H:%M:%S')}' and FileIncludeFlag = 'Y'")
                    avg_record_count = log_df.select(col("RecordCount").cast("int")).agg({"RecordCount": "avg"}).collect()[0][0]
                    if avg_record_count is None:
                        status_msg = "First run, no record count threshold check required."
                        print(status_msg)
                    else:
                        threshold_percent = int(threshold_percent)
                        lower_bound = (1 - threshold_percent / 100.0) * avg_record_count
                        upper_bound = (1 + threshold_percent / 100.0) * avg_record_count
                        if not lower_bound <= df_data_count <= upper_bound:
                            status_msg = f"Current record count ({df_data_count}) is not within the range of {threshold_percent}% from the average of the last {no_of_days} days."
                            print(status_msg)
                            status = "RecordCountThreshold"
                            if "Audit Failed" not in status_message:
                                status_message = "Audit Failed - " + status
                            else:
                                status_message += '|' + status
                            JobStatus = 'Failed'
                        else:
                            status_msg = f"Record count threshold check passed. Current record count ({df_data_count}) is within the range of {threshold_percent}% from the average of the last {no_of_days} days."
                            if "Audit Failed" not in status_message:
                                status_message = "Audit Passed"
                            print(status_msg)

                       

                print(status_message)
                # Update the log table with the audit results
                cust_q = f"""update [integration_stage].[int_audit].[file_log] set FileSize = '{fileSize}', StatusMessage = '{status_message}' where  JobName = '{jobName}' and FileTransferStatus = 'Pending' and JobStatus = 'FINISHED' and JobStep = {jobStep}"""
                onprem_query_execute(env_set, cust_q)
                print(f"File generated time has been updated in the timestamp table for {tableName}.\n")
            

        # Update job status as finished when batch process =Y
        if batchProcess == 'Y':
            cust_q = f"select count(*) as record_count from [integration_stage].[int_audit].[file_log] where JobName = '{jobName}' and FileGenerationDateTime = (select max(FileGenerationDateTime) from [integration_stage].[int_audit].[file_log] where JobName = '{jobName}' and StatusMessage like 'Audit Failed -%' and jobstatus='FINISHED' and FileTransferStatus = 'Pending')"
            df_count = onprem_query_read(env_set, cust_q)
            record_count = df_count.collect()[0]['record_count']
           
            if record_count is None:
                record_count = 0
            elif record_count > 0:
                raise Exception("Job has failed in an error state. Please review logs.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Logging 

# COMMAND ----------

###########################################################################################################################################################
# Function for logging Inbound jobs
###########################################################################################################################################################
def db_log_update(statusMessage, jobStatusFlag, df=None, jobName=""):
    """
    Function to update db_file_log table based on statusflag
    :df: dataframe that needs to be passed
    :statusMessage: status message to know the failure details ("Short Description about error")
    :jobStatusFlag: job status flag indicates success or failure of the job ('Y' or 'N')
    :jobName: jobname to consider for update
    """

    # Establish connection to update into file_log for the job
    env_set = onprem_connection('INTEGRATION_DB', "integration_stage") 

    # Establish connection to blob
    env_set_blob = blob_connection()

    # Consider jobsteps when fileincludeflag ='Y' and jobstatus = 'RUNNING'
    dt_config_tv = onprem_query_read(
        connection_info=env_set,
        query=f"Select * from [integration_stage].[int_audit].[file_log] where JobName = '{jobName}' and FileIncludeFlag='Y' and FileTransferStatus = 'Success' and JobStatus = 'RUNNING'"
    )

    for row in dt_config_tv.collect():
        # set variable to default values
        fileSize = '0'
       
        # Assign values from log table row
        jobStep = row.JobStep
        folderPath = row.FolderPath
        fileName = row.FileName
       
        # Get record count and file size
        path_for_size = f'''{folderPath}/{fileName}'''

        try:
          recordCount = df.count()
        except Exception as e:
          statusMessage = f"Error getting record count: {e}"
          recordCount = 0  
        
        try:        
            fileSize = blob_get_file_size(env_set_blob, path_for_size)
        except Exception as e:
            statusMessage = f"Failed to get file size after 5 tries: {e}"
            fileSize = '0'

        # Update jobstatus, FileGenerationStatus, FileSize, RecordCount, StatusMessage, FileGenerationDateTime)
        if jobStatusFlag == 'Y':
            cust_q = f"UPDATE [integration_stage].[int_audit].[file_log] set JobStatus = 'FINISHED', FileSize='{fileSize}', RecordCount='{recordCount}',StatusMessage='{statusMessage}', FileGenerationStatus ='Success', FileGenerationDateTime ='{GetTime(strform='%Y-%m-%d %H:%M:%S')}' where JobName = '{jobName}' and JobStep='{jobStep}' and JobStatus = 'RUNNING'"
            print("Job status updated to FINISHED")
            onprem_query_execute(env_set, cust_q)
        elif jobStatusFlag == 'N':
            cust_q = f"UPDATE [integration_stage].[int_audit].[file_log] set JobStatus = 'FAILED', FileSize='{fileSize}', RecordCount='{recordCount}',StatusMessage='{statusMessage}', FileGenerationStatus ='Failed', FileGenerationDateTime ='{GetTime(strform='%Y-%m-%d %H:%M:%S')}' where JobName = '{jobName}' and JobStep='{jobStep}' and JobStatus = 'RUNNING'"
            print("Job status updated to FAILED")
            onprem_query_execute(env_set, cust_q)
        elif jobStatusFlag == 'NA':
            cust_q = f"UPDATE [integration_stage].[int_audit].[file_log] set JobStatus = 'DISCARDED', FileSize='{fileSize}', RecordCount='{recordCount}',StatusMessage='{statusMessage}', FileGenerationStatus ='Failed', FileGenerationDateTime ='{GetTime(strform='%Y-%m-%d %H:%M:%S')}' where JobName = '{jobName}' and JobStep='{jobStep}' and JobStatus = 'RUNNING'"
            print("Job status updated to DISCARDED")
            onprem_query_execute(env_set, cust_q)
        else: 
            print("jobStatusFlag is not defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **REST API Integration**
# MAGIC
# MAGIC Generic REST API functions: authentication, paginated data retrieval, upserts to catalog tables, and audit logging for integration pipelines.

# COMMAND ----------

# MAGIC %md
# MAGIC def get_api_access_token():
# MAGIC
# MAGIC def get_paginated_api_data(end_point_url, access_token, start_page=1, page_size=100, params=''):
# MAGIC
# MAGIC def get_paginated_api_response(end_point_url, access_token, start_page=1, page_size=100, params=''):
# MAGIC
# MAGIC def api_upsert_to_catalog(source_df,pk_columns, table_name, stage_table_name):
# MAGIC
# MAGIC def onprem_query_execute_returning(connection_info, query):
# MAGIC
# MAGIC def integration_log(jobName = get_notebook_name(),jobStep=None,folderPath=None,fileName=None,fileSize=None, df_data_count=None,status_msg=None,fileIncludeFlag=None,ZeroRecordFlag=None,maxRecordDateTime=None, status=None, fileTransferStatus=None,fileGenerationStatus=None,full_data_count = None,action_type="insert"):
# MAGIC
# MAGIC def log_lastrun(jobStep, time_col = "MaxRecordDateTime"):
# MAGIC

# COMMAND ----------

# =============================================================================
# REST API INTEGRATION — COMMENTED OUT (requires live API credentials)
# =============================================================================
# These functions handle OAuth2 token auth, paginated data retrieval, catalog
# upserts, on-prem audit logging, and last-run tracking for REST API pipelines.
#
# To enable:
#   1. Set partner_api_base_url, partner_api_token_name, api_client_id,
#      api_client_secret in the environment/secrets block at the top of the file.
#   2. Ensure catalog_connection, catalog_read, catalog_query, catalog_swap_tables
#      are available (Databricks Unity Catalog section above).
#   3. Ensure the INTEGRATION_DB on-prem connection profile is configured in
#      onprem_connection() and the [integration_stage].[int_audit].[file_log]
#      table exists.
#   4. Uncomment all functions in this section.
#
# Dependencies: requests, pandas, pyspark, simple_salesforce (for catalog funcs)
# =============================================================================

# DBTITLE 1,REST API authentication, data retrieval and upsert functions
# def get_api_access_token():
#     """
#     Fetches and returns a Bearer access token from a REST API using client credentials.
#     """
#     # NOTE: Replace partner_api_base_url, partner_api_token_name, api_client_secret,
#     #       and api_client_id with your configured environment variables.
#     url = partner_api_base_url + "v2/pub/oauth/token?Token Name=" + partner_api_token_name
#     print(url)
#     payload = 'grant_type=client_credentials&client_secret=' + api_client_secret + '&client_id=' + api_client_id
#     headers = {
#         'Content-Type': 'application/x-www-form-urlencoded',
#     }
#     response = requests.request("POST", url, headers=headers, data=payload)

#     try:    
#         access_token = 'Bearer '+response.json()['access_token']
#         print(f"Access Token generated Successfully.")
#     except JSONDecodeError as e:
#         print('Response content is not valid JSON for Access Token')
#         access_token = None
#         raise Exception("Error: Unable to fetch access token.") from e
#     except Exception as e:
#         print('Unexpected error while fetching access token')
#         raise Exception('Unexpected error while fetching access token')
#     return access_token

# def get_paginated_api_data(end_point_url, access_token, start_page=1, page_size=100, params=''):
#     """
#     Fetches paginated data from a REST API endpoint and returns a Spark DataFrame.
#     Handles pagination, collects all items, and returns the result as a Spark DataFrame.
#     """
#     headers = {
#         'Authorization': access_token,
#         'Content-Type': 'application/json',
#         'Accept': 'application/json'
#     }
#     all_data = []
#     page = start_page
#     try:
#         while True:
#             url = f"{partner_api_base_url}{end_point_url}?page={page}&per_page={page_size}&{params}"
#             print(f"Fetching page {page}... URL: {url}")
#             response = requests.get(url, headers=headers)
#             print(f"Response Status Code: {response.status_code}")
#             if response.status_code != 200:
#                 print(f"Error: {response.status_code}")
#                 break
#             data = response.json()
#             items = data.get("items", [])
#             if not items:
#                 break
#             all_data.extend(items)
#             if len(items) < page_size:
#                 break
#             page += 1

#         pdf = pd.json_normalize(all_data)
#         df = spark.createDataFrame(pdf)
#         return df
#     except Exception as e:
#         print(f"Exception occurred while fetching data from API: {e}")

# def get_paginated_api_response(end_point_url, access_token, start_page=1, page_size=100, params=''):
#     """
#     Fetches paginated data from a REST API endpoint, aggregates all items across pages, and returns a combined response dictionary.
#     """
#     headers = {
#         'Authorization': access_token,
#         'Content-Type': 'application/json',
#         'Accept': 'application/json'
#     }
#     page = start_page
#     total_items = []
#     last_response = None
#     try:
#         while True:
#             url = f"{partner_api_base_url}{end_point_url}?page={page}&per_page={page_size}&{params}"
#             print(url)
#             response = requests.get(url, headers=headers)
#             last_response = response
#             if response.status_code != 200:
#                 break
#             data = response.json()
#             items = data.get("items", [])
#             total_items.extend(items)
#             if not items or len(items) < page_size:
#                 break
#             page += 1
#         total_response = {"items": total_items}
#         if last_response is not None:
#             for k, v in last_response.json().items():
#                 if k != "items":
#                     total_response[k] = v
#         return total_response
#     except Exception as e:
#         print(f"Error fetching data from API: {e}")

# def api_upsert_to_catalog(source_df,pk_columns, table_name, stage_table_name):
#     """
#     This function performs an upsert (update and insert) operation from a source DataFrame to a target catalog table.
#     It identifies records to update, insert, and preserve, merges them, loads the result into a staging table, 
#     and swaps the staging table with the main table in the catalog.
#     """
#     v_current_datetime = current_timestamp()
#     # NOTE: Replace 'my_conn_profile', 'my_catalog', and 'my_schema' with your
#     #       actual catalog connection profile, catalog name, and schema name.
#     try:
#         conn_info = catalog_connection(conn_profile='my_conn_profile')
#     except Exception as e:
#         print("Error establishing catalog connection")
#         raise Exception("Error establishing catalog connection")

#     try:
#         target_df = catalog_read(
#             connection_info=conn_info,
#             catalog_name='my_catalog',
#             schema_name='my_schema',
#             table_name=table_name
#         )
#         target_df.cache()
#     except Exception as e:
#         print("Error reading target table from catalog")
#         raise Exception("Error reading target table from catalog")

#     try:
#         old_rec_df = target_df.join(source_df, on=pk_columns, how='left_anti')
#         print("Records present in target table but not in source file:", old_rec_df.count())
#     except Exception as e:
#         print("Error identifying records to preserve")
#         raise Exception("Error identifying records to preserve")

#     # 2) Update rows Operation
#     try:
#         print('Update Operation Begins')
#         update_df = (
#             source_df.alias("src")
#             .join(target_df.alias("tgt"), on=pk_columns, how="inner")
#             .select("src.*", col("tgt.ETL_Inserted_dt").alias("ETL_Inserted_dt"))
#             .withColumn("ETL_Updated_dt",v_current_datetime)
#         )

#         print('Total records to update:', update_df.count())
#         print('Update Operation Ends')
#     except Exception as e:
#         print("An error occurred during the update operation: " + str(e))
#         raise Exception("An error occurred during the update operation: " + str(e))

#     # 3) Insert records Operation
#     try:
#         print('Insert Operation Begins')
#         insert_df = source_df.alias("src").join(target_df.select(*pk_columns).alias("tgt"), on=pk_columns, how='left_anti')
#         print("Number of new records for insert:", insert_df.count())

#         insert_df_final = (
#             insert_df
#             .withColumn("ETL_Inserted_dt", lit(v_current_datetime).cast("timestamp"))
#             .withColumn("ETL_Updated_dt", lit(v_current_datetime).cast("timestamp"))
#         )
#         print('Insert Operation Ends')
#     except Exception as e:
#         print("An error occurred during the insert operation: " + str(e))
#         raise Exception("An error occurred during the insert operation: " + str(e))

#     # 4) Merge final DF (inserted + updated + preserved)
#     try:
#         print("--- Union Operation begins ---")
#         # Get the common columns from all DataFrames
#         final_write_target_df = insert_df_final.unionByName(update_df).unionByName(old_rec_df)

#         print("Number of records in final df:", final_write_target_df.count())
#         print("Data frames Merging completed successfully")
#     except Exception as e:
#         print("An error occurred while merging the DataFrames: " + str(e))
#         raise Exception("An error occurred while merging the DataFrames: " + str(e))

#     #Operation of truncate and load on wrk_tbl_wp_custom_attributes_uat
#     try:
#         catalog_query(conn_info, mode='truncate_and_load', catalog_name='my_catalog', db_schema='my_schema', table_name=stage_table_name, spark_df=final_write_target_df, batch_size=10000)
#     except Exception as e:
#         print("Error occurred while loading data to catalog table")
#         raise Exception("Error occurred while loading data to catalog table")

#     # Swapping the stage and final tables
#     try:
#         print("---Start of swap operation---")
#         catalog_swap_tables(
#             conn_info,
#             catalog_name='my_catalog',
#             db_schema='my_schema',
#             Main_table=table_name,
#             Stage_table=stage_table_name
#         )
#     except Exception as e:
#         print("Error occurred while swapping tables")
#         raise Exception("Error occurred while swapping tables")


# def onprem_query_execute_returning(connection_info, query):
#     """Function to run a sql statements on an on-prem server databases
#     :connection_info (list): the environment connections that you will set after calling the onprem_connection function
#     :query (string): The query which contains the SQL statement that will run on the server
#     :return: Number of rows affected by the query
#     """
#     jdbc_url = connection_info[0]
#     connectionProperties = connection_info[1]
#     username = connectionProperties['user']
#     password  =  connectionProperties['password']
#     driver_manager = SparkContext._gateway.jvm.java.sql.DriverManager
#     con = driver_manager.getConnection(jdbc_url, username, password)
#     rows_affected = 0
#     try:
#         stmt = con.createStatement()
#         rows_affected = stmt.executeUpdate(query)
#     except Exception as e:
#         print(e)
#         print("\n EXCEPTION CAUGHT DURING EXECUTION OF SQL STATEMENT!")
#         raise
#     finally:
#         con.close()
#     return rows_affected

# def integration_log(jobName = get_notebook_name(),
#     jobStep=None,
#     folderPath=None,
#     fileName=None,
#     fileSize=None, 
#     df_data_count=None,
#     status_msg=None,
#     fileIncludeFlag=None, 
#     ZeroRecordFlag=None,
#     maxRecordDateTime=None, 
#     status=None, 
#     fileTransferStatus=None,
#     fileGenerationStatus=None,
#     full_data_count = None,
#     action_type="insert"  # "insert" or "update"
#     ):
#     """
#     Logs job execution status and metadata to the integration audit table.
#     Supports insert, update-success, and update-failed actions to track job progress and outcomes.
#     """
#     try:
#         env_set = None
#         try:
#             env_set = onprem_connection('INTEGRATION_DB', "integration_stage")
#         except Exception as e:
#             print(f"Error establishing onprem connection: {e}")
#             raise

#         if action_type == "insert":
#             try:
#                 cust_q = f"""INSERT INTO [integration_stage].[int_audit].[file_log] 
#                 (JobName, JobStep, FolderPath, FileName, FileSize, RecordCount, StatusMessage, FileIncludeFlag, ZeroRecordFlag, MaxRecordDateTime, FileGenerationDateTime, FileGenerationStatus, FileTransferStatus, JobStatus, SourceRecordCount) 
#                 VALUES (
#                     '{jobName}', -- JobName
#                     '{jobStep}', -- JobStep
#                     '{folderPath}', -- FolderPath
#                     '{fileName}', -- FileName
#                     {('NULL' if fileSize is None else f"'{fileSize}'")}, -- FileSize
#                     {('NULL' if df_data_count is None else f"'{df_data_count}'")},-- RecordCount
#                     '{status_msg}',-- StatusMessage
#                     '{fileIncludeFlag}', -- FileIncludeFlag
#                     '{ZeroRecordFlag}', -- ZeroRecordFlag
#                     {('NULL' if maxRecordDateTime is None else f"'{maxRecordDateTime}'")},-- MaxRecordDateTime
#                     '{GetTime(strform='%Y-%m-%d %H:%M:%S')}',-- FileGenerationDateTime
#                     '{fileGenerationStatus}', -- FileGenerationStatus
#                     '{fileTransferStatus}', -- FileTransferStatus
#                     '{status}', -- JobStatus 
#                     {('NULL' if full_data_count is None else f"'{full_data_count}'")} -- SourceRecordCount
#                 )"""
#                 rows_affected = onprem_query_execute_returning(env_set, cust_q)
#                 print(f"Rows affected by insert: {rows_affected}")
#                 print("Inserted Running Status")
#             except Exception as e:
#                 print(f"Error during insert action in stageintegration_log: {e}")
#                 raise

#         elif action_type == "update-success":
#             try:
#                 update_q = f"""
#                 UPDATE [integration_stage].[int_audit].[file_log]
#                 SET FileTransferStatus = '{fileTransferStatus}',
#                 JobStatus = '{status}',
#                 StatusMessage = '{status_msg}',
#                 FileGenerationStatus = '{fileGenerationStatus}'
#                 WHERE JobName = '{jobName}' AND JobStep = '{jobStep}' AND
#                 FileGenerationStatus ='Running' and FileTransferStatus = 'Pending' and JobStatus='Running' and StatusMessage = 'Started writing to Azara';
#                 """
#                 rows_affected = onprem_query_execute_returning(env_set, update_q)
#                 print(f"Rows affected by update-success: {rows_affected}")
#                 print(f"Updated staging table for JobStep= {jobStep} with status= {status}")
#             except Exception as e:
#                 print(f"Error during update-success action in stageintegration_log: {e}")
#                 raise

#         elif action_type == "update-failed":
#             try:
#                 update_q = f"""
#                 UPDATE [integration_stage].[int_audit].[file_log]
#                 SET FileTransferDateTime = '{GetTime(strform='%Y-%m-%d %H:%M:%S')}', FileTransferStatus = '{fileTransferStatus}',
#                 JobStatus = '{status}',
#                 StatusMessage = '{status_msg}',
#                 FileGenerationStatus = '{fileGenerationStatus}'
#                 WHERE JobName = '{jobName}' AND JobStep = '{jobStep}' AND
#                 FileGenerationStatus ='Running' and FileTransferStatus = 'Pending' and JobStatus='Running' and StatusMessage = 'Started writing to Azara';
#                 """
#                 rows_affected = onprem_query_execute_returning(env_set, update_q)
#                 print(f"Rows affected by update-failed: {rows_affected}")
#                 print(f"Updated staging table for JobStep= {jobStep} with status= {status}")
#             except Exception as e:
#                 print(f"Error during update-failed action in stageintegration_log: {e}")
#                 raise Exception(f"Error during update-failed action in stageintegration_log: {e}")

#     except Exception as e:
#         print(f"Unexpected error in stageintegration_log for action_type={action_type}, JobStep={jobStep}: {e}")
#         raise Exception(f"Unexpected error in stageintegration_log for action_type={action_type}, JobStep={jobStep}: {e}")

# def log_lastrun(jobStep, time_col = "MaxRecordDateTime"):
#     """
#     Returns the most recent successful run datetime for a given job step from the audit log table.
#     """
#     try:
#         env_set = onprem_connection('INTEGRATION_DB', "integration_stage")
#         jobName = get_notebook_name()
#         last_run_df = onprem_query_read(connection_info = env_set, query =f"SELECT TOP 1 {time_col} FROM [integration_stage].[int_audit].[file_log] WHERE JobName = '{jobName}' AND JobStep = '{jobStep}' AND FileGenerationStatus = 'Success' AND FileTransferStatus = 'Success' AND {time_col} IS NOT NULL ORDER BY {time_col} DESC")
#         last_run = None
#         if last_run_df is not None and last_run_df.count() > 0:
#             row = last_run_df.first()
#             last_run = row[0] if row[0] not in [None, '', 'NULL'] else None
#         return last_run
#     except Exception as e:
#         print(f"Error in stgint_log_lastrun: {e}")
#         raise Exception(f"Error in stgint_log_lastrun: {e}")    
