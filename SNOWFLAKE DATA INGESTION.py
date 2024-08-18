import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
from snowflake.snowpark import Session
from datetime import datetime, timedelta
import pandas as pd
import json

#Function to create a session with Snowflake:
def create_session():
    #Read credentials
    with open(r'\directory\folder_path\file_name.json') as f:
        connection_parameters = json.load(f)

    # Connect to Snowflake
    session = Session.builder.configs(connection_parameters).create()
    return session

#Function to bulk load CSVs into Snowflake
def bulk_load(snowpark_session, local_directory, internal_stage, file_format_name, table_name, start_date, end_date):
    #Generate the list of file paths based on the date range
    file_paths = [f"{local_directory}/data_{date.strftime('%Y-%m-%d')}.csv"
                 for date in pd.date_range(start_date, end_date)]
    
    for file_path in file_paths:
        put_command = f"PUT 'file://{file_path} @{internal_stage}"
        snowpark_session.sql(put_command).collect()
        print(f"Uploaded {file_path} to {internal_stage}")

    #COPY INTO table from internal stage
    copy_command = f"""
    COPY INTO {table_name}
    FROM @{internal_stage}
    FILE_FORMAT = (FORMAT_NAME = '{file_format_name}')
    """
    snowpark_session.sql(copy_command).collect()
    print(f"Copied data from {internal_stage} into {table_name}")

#Main Function to handle the bulk load process
def main(session: snowpark.Session):
    #Use session passed in by Snowflake
    snowpark_session = session

    #Define a path to your local files
    local_directory = r"directory_path/folder_name"

    #Specify the internal stage
    internal_stage = "MY_STAGE"

    #File Format name for the COPY INTO command
    file_format_name = "my_format"

    #Table name to COPY INTO
    table_name = "TABLE_NAME"

    #Get the current date
    current_date = datetime.now().date()

    # Calculate the previous day's date
    previous_date = current_date - timedelta(days=1)

    #Assign the previous date to both the start_date and end_date
    start_date = previous_date
    end_date = previous_date

    #Perform the bulk load
    bulk_load(snowpark_session, local_directory, internal_stage, file_format_name, table_name, start_date, end_date)

if __name__=="__main__":
    session = create_session()
    main(session)
