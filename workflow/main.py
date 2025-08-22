from prefect import task, flow
from prefect.server.schemas.schedules import IntervalSchedule
import pandas as pd
import pyodbc
import os
import sys
import glob
import datetime
from datetime import timedelta
from pytz import timezone
import math

# read configurations from config.ini
from configparser import ConfigParser
# CREATE OBJECT
config = ConfigParser()
config.read('D:\HGInsights\Git\hg_datapipeline\config.ini')

# current datetime id in int
tz = 'Asia/Kolkata'
now = datetime.datetime.now(timezone(tz))
datetime_str = now.strftime("%Y%m%d%H%M")
datetime_int = int(now.strftime("%H%M"))

# Read configurations
v_root_folder = config['DEFAULT']['v_root_folder']

server = config['DATABASE']['server']
user = config['DATABASE']['user']
password = config['DATABASE']['password']

# initialize runid
runid = 0

# function for DB Connection
def fn_connection(dbname):
    conn = pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={server};"
        f"DATABASE={dbname};"
        f"UID={user};"
        f"PWD={password};"
    )
    # print(f"Connection successful to {dbname} DB !")
    # print('...')
    return conn


system_db_conn = fn_connection('system_db')
system_db_cursor = system_db_conn.cursor()

bronze_db_conn = fn_connection('bronze_db')
bronze_db_cursor = bronze_db_conn.cursor()

silver_db_conn = fn_connection('silver_db')
silver_db_cursor = silver_db_conn.cursor()

gold_db_conn = fn_connection('gold_db')
gold_db_cursor = gold_db_conn.cursor()

####### common functions definition starts #######

# to initialize all db connections
def fn_disconnct_dbs():
    try:
        system_db_conn.close()
        system_db_cursor.close()
        bronze_db_conn.close()
        bronze_db_cursor.close()
        silver_db_conn.close()
        silver_db_cursor.close()
        gold_db_conn.close()
        gold_db_cursor.close()
    except:
        print('Closing cursor..')

# to generate run id
def fn_get_runid():
    system_db_cursor.execute(
        'SELECT NEXT VALUE FOR system_db.dbo.sq_runid AS NewRunId;')
    runidrow = system_db_cursor.fetchone()
    runid = runidrow[0]
    print('------------------------------------')
    print('Current runid is:', runid)
    print('------------------------------------')
    print('')    
    return int(runid)

# to create range dimension for 5 and 10 intervals
def fn_create_range_of_5(x):
    lower = math.floor(x / 5) * 5
    upper = math.ceil(x / 5) * 5
    if x <=5:
        lower=0
        upper=5
    elif x % 5 == 0 and x>5:
        lower = x-4
        upper = x
    else:
        lower=lower+1
        upper=upper
    value_range = str(lower)+'-'+str(upper)
    return value_range

def fn_create_range_of_10(x):
    lower = math.floor(x / 10) * 10
    upper = math.ceil(x / 10) * 10
    if x <=10:
        lower=0
        upper=10
    elif x % 10 == 0 and x>10:
        lower = x-9
        upper = x
    else:
        lower=lower+1
        upper=upper
    value_range = str(lower)+'-'+str(upper)
    return value_range
####### common functions definition ends #######
# runid = fn_get_runid()

# Function to get sql server datetime id


def fn_getsql_datetime():
    system_db_cursor.execute('select getdate()')
    getdaterow = system_db_cursor.fetchone()
    curr_datetime = getdaterow[0]
    return curr_datetime

# Function to insert run status


def fn_run_status(runid, status):
    try:

        insertsql = """
        INSERT INTO dbo.ach_runs (runid, createdate, startdatetimeid, enddatetimeid, runstatus)
        VALUES (?, ?, ?, ?, ?)
        """
        updatesql = """
        UPDATE dbo.ach_runs set enddatetimeid = ?, runstatus = ?
        WHERE runid = ?
        """
        curr_datetime = fn_getsql_datetime()

        if status == 'Run Started':
            system_db_cursor.execute(
                insertsql, (runid, curr_datetime, curr_datetime, 0, status))
            system_db_cursor.connection.commit()
        elif status == 'Run Ended':
            system_db_cursor.execute(updatesql, (curr_datetime, status, runid))
            system_db_cursor.connection.commit()

        return 0
    except Exception as e:
        message_line = (f"Error inserting log: {e}")
        print(message_line)
        fn_log_message(runid, 'ERROR', message_line, 'fn_run_status')
        return 1

# Function to insert log messages


def fn_log_message(runid, loglevel, messageline, function_name):
    try:
        sql = """
        INSERT INTO dbo.ach_logs (runid, loglevel, message_line,function_name)
        VALUES (?, ?, ?, ?)
        """
        system_db_cursor.execute(
            sql, (runid, loglevel, messageline, function_name))
        system_db_cursor.connection.commit()

    except Exception as e:
        print(f"Error inserting log: {e}")


@task()
def fn_extract_load_data():
    print('')
    print('====================== Data Ingest process ======================')       
    try:
        # read source files
        v_source_folder = v_root_folder+'source/'
        # make a list of all raw input files
        raw_files = glob.glob(os.path.join(v_source_folder, "*.csv"))

        # initiaize an empty dataframe
        df_customers = pd.DataFrame(columns=['CustomerID', 'Age', 'Gender', 'Tenure', 'MonthlyCharges',
                                    'ContractType', 'InternetService', 'TotalCharges', 'TechSupport', 'Churn', 'runid'])

        if len(raw_files) > 0:
            for file in raw_files:
                print(file)
                file_name = os.path.splitext(os.path.basename(file))[0]
                df_curr_data = pd.read_csv(file, header=0)
                df_curr_data['runid'] = runid
                df_customers = pd.concat(
                    [df_curr_data, df_customers], ignore_index=True)

        # To fix longer decimal values for TotalCharges
        df_customers['TotalCharges'] = pd.to_numeric(
            df_customers['TotalCharges'], errors='coerce')

        # detect if invalid customerid is present if so move the rows in the bad data
        invalid_cusotmerids = df_customers["CustomerID"].apply(
            lambda x: str(x).isdigit())

        # Split into good and bad dataframes
        df_good_customers_data = df_customers[invalid_cusotmerids].copy()
        df_bad_customers_data = df_customers[~invalid_cusotmerids].copy()

        df_good_customers_data = df_good_customers_data.astype({
            "CustomerID": "int",
            "Age": "int",
            "Tenure": "int",
            "MonthlyCharges": "float",
            "TotalCharges": "float",  # to accect long float values
            "runid": "int"
        })
        # use case transformaiton - 01 InternetService missing values to missing
        df_good_customers_data['InternetService'] = df_good_customers_data['InternetService'].fillna(
            'missing')

        print('')
        if len(df_good_customers_data) > 0:
            # assuming df_good_customers_data has same column order as raw_customers
            insert_sql = """
            INSERT INTO raw_customers (
                CustomerID, Age, Gender, Tenure, MonthlyCharges,
                ContractType, InternetService, TotalCharges,
                TechSupport, Churn, runid
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            data = df_good_customers_data.to_records(index=False).tolist()

            # Insert all rows at once
            bronze_db_cursor.executemany(insert_sql, data)

            # Commit the transaction
            bronze_db_cursor.connection.commit()

            messageline = f"{len(data)} rows inserted into raw_customers.."
            print(messageline)
            fn_log_message(runid, 'INFO', messageline, 'fn_extract_data')

            # export good data of all files
            filegooddata = f'{v_root_folder}archive/runid_{runid}_allfiles_gooddata_{datetime_str}.csv'
            print(filegooddata)
            df_good_customers_data.to_csv(filegooddata, index=False)

        # export bad data of all files
        if len(df_bad_customers_data) > 0:
            filebaddata = f'{v_root_folder}archive/runid_{runid}_allfiles_baddata_{datetime_str}.csv'
            df_bad_customers_data.to_csv(filebaddata, index=False)

        return 0
    except Exception as err_load_data:
        messageline = str(err_load_data)
        print(messageline)
        fn_log_message(runid, 'ERROR', messageline, 'fn_extract_data')
        return 1

@task
def fn_tranform_data():
    try:
        # read raw_customers data for transformations
        query = f"""select customers.* from dbo.raw_customers customers,
                    (select max(runid) as max_runid , CustomerID from dbo.raw_customers group by CustomerID) as latest_customers
                    where customers.CustomerID = latest_customers.CustomerID 
                    and customers.runid = latest_customers.max_runid
                    and latest_customers.max_runid = {runid}"""
        bronze_db_cursor.execute(query)

        # Fetch all rows
        rows_customers = bronze_db_cursor.fetchall()

        # get column names for the dataframe
        columns = [column[0] for column in bronze_db_cursor.description]
        # create dataframe from the data rows
        df_latest_customers = pd.DataFrame.from_records(
                                            rows_customers, columns=columns)
        print('')
        print('====================== Transformations ======================')

        # use case transformaiton - 02
        df_latest_customers['TotalCharges'] = df_latest_customers['TotalCharges'].round(
            2)

        # use case transformaiton - 03 Define new dimension as Tenure_Range every 10 years
        df_latest_customers['Tenure_Range'] = df_latest_customers['Tenure'].apply(fn_create_range_of_10)
        
        # use case transformaiton - 04 Define Age_band dimension every 5 years
        df_latest_customers['Age_band'] = df_latest_customers['Age'].apply(fn_create_range_of_5)
        
        # use case transformaiton - 04A - Drop Age field to protect PII informaiton
        df_latest_customers.drop(columns=['Age'], inplace=True)

        # use case transformaiton - 05
        df_latest_customers['Category'] = df_latest_customers['MonthlyCharges'].apply(
                                            lambda x: 'High' if x > 100 else ('Medium' if x > 50 else 'Low'))
        
        # set the current rows as Y
        df_latest_customers['is_current'] = 'Y'

        df_latest_customers = df_latest_customers[['CustomerID', 'Age_band', 'Gender', 'Tenure', 'Tenure_Range', 'MonthlyCharges',
                                                    'ContractType', 'InternetService', 'TotalCharges',
                                                    'TechSupport', 'Churn', 'runid', 'Category','is_current']]
        # To soft delete previous data
        try:
            # get new incoming list of cusotmerids, customerid key is the key identifier to refresh data
            new_customer_ids = df_latest_customers['CustomerID'].tolist()
            if not new_customer_ids:
                new_customer_ids = [-99]   # dummy id that won't match

            placeholders = ",".join("?" for _ in new_customer_ids)

            update_sql = f"""
                            UPDATE dbo.customers set is_current='N' 
                            where runid < ? and customerid in ({placeholders})
                        """
            # log new custmoer ids
            new_string_of_customerids = ",".join(str(c) for c in new_customer_ids)
            fn_log_message(runid, 'DEBUG', f'CustomerIDs: {new_string_of_customerids}', 'fn_tranform_data')

            # make a list of current runids and all new customerids
            update_params = [runid] + new_customer_ids
            # execute the update sql
            silver_db_cursor.execute(update_sql, update_params)
            # to get the total rows updated
            rows_updated = silver_db_cursor.rowcount
            silver_db_cursor.commit()
            
            messageline = f'{rows_updated} rows are soft deleted while data refresh in the silver_db.'
            fn_log_message(runid, 'INFO', messageline, 'fn_tranform_data')
        except Exception as updaterr:
            messageline = f'Soft delete failed with error {updaterr}'
            fn_log_message(runid, 'ERROR', messageline, 'fn_tranform_data')
            return 1
        
        # To load latest transformed data
        try:
            # to insert all new rows as per SCD 2
            insert_sql = """
                            INSERT INTO dbo.customers (
                                CustomerID, Age_band, Gender, Tenure, Tenure_Range, MonthlyCharges,
                                ContractType, InternetService, TotalCharges,
                                TechSupport, Churn, runid, Category, is_current
                            )
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """
            data = df_latest_customers.to_records(index=False).tolist()

            # Insert all rows at once
            silver_db_cursor.executemany(insert_sql, data)

            # Commit the transaction
            silver_db_cursor.connection.commit()

            messageline = f"{len(data)} rows transformed {len(df_latest_customers)} rows inserted into silver_db.dbo.customers.."
            fn_log_message(runid, 'INFO', messageline, 'fn_tranform_data')
        except Exception as updaterr:
            messageline = f'Data refresh in silver_db failed with error {updaterr}'
            fn_log_message(runid, 'ERROR', messageline, 'fn_tranform_data')
            return 1

        return 0
    except Exception as errtransform:
        messageline = str(errtransform)
        fn_log_message(runid, 'ERROR', messageline, 'fn_tranform_data')
        return 1

@task
def fn_model_report_data():
    print('')
    print('====================== Reporting Data Aggregation ======================')    
    try:
        query = """ select * from dbo.customers where is_current = 'Y'
                """
        silver_db_cursor.execute(query)

        # Fetch all rows
        rows_customers = silver_db_cursor.fetchall()

        # get column names for the dataframe
        columns = [column[0] for column in silver_db_cursor.description]
        # create dataframe from the data rows
        df_customers = pd.DataFrame.from_records(
                                            rows_customers, columns=columns)
        # take columns in the dataframe
        df_customers = df_customers[['CustomerID', 'Age_band', 'Gender', 'Tenure', 'Tenure_Range', 'MonthlyCharges',
                                        'ContractType', 'InternetService', 'TotalCharges',
                                        'TechSupport', 'Churn', 'runid', 'Category', 'is_current']]

        try:
            # report use case 01 Count of customers by Categories (i.e. High/Medium/Low)
            df_revenues_by_category = df_customers.groupby('Category').agg(
                                                                            CustomerCount=('CustomerID', 'count'),
                                                                            TotalRevenue=('TotalCharges', 'sum')
                                                                            ).reset_index()
            
            df_revenues_by_category = df_revenues_by_category[['Category','CustomerCount','TotalRevenue']]
            
            # delete data before refresh
            delete_customers_by_category_sql = """ delete from dbo.customers_by_category"""

            gold_db_cursor.execute(delete_customers_by_category_sql)

            # insert report data
            insert_customers_by_category_sql = """
                            INSERT INTO dbo.customers_by_category (
                            Category,CustomerCount,TotalRevenue
                            )
                            VALUES (?, ?, ?)
                            """
            data = df_revenues_by_category.to_records(index=False).tolist()

            # Insert all rows at once
            gold_db_cursor.executemany(insert_customers_by_category_sql, data)

            # Commit the transaction
            gold_db_cursor.connection.commit()

            messageline = f"{len(data)} rows refreshed into gold_db.dbo.customers_by_category table."
            fn_log_message(runid, 'INFO', messageline, 'fn_model_report_data')
        except Exception as err:
            messageline = f"Failed refreshing table gold_db.dbo.customers_by_category table {err}."
            fn_log_message(runid, 'ERROR', messageline, 'fn_model_report_data')
            return 1

        try:
            # report use case 02 and 03 Aggregated revenue (TotalCharges) by Contract Types and InternetService
            df_aggrevenue_summary = df_customers.groupby(['ContractType', 'InternetService']).agg(
                                                                            CustomerCount=('CustomerID', 'count'),
                                                                            TotalRevenue=('TotalCharges', 'sum')
                                                                            ).reset_index()

            df_aggrevenue_summary = df_aggrevenue_summary[['ContractType', 'InternetService' ,'CustomerCount','TotalRevenue']]

            # delete data before refresh
            delete_customers_by_category_sql = """ delete from dbo.aggrevenue_summary"""

            gold_db_cursor.execute(delete_customers_by_category_sql)

            # insert report data            
            insert_aggrevenue_summary_sql = """
                            INSERT INTO dbo.aggrevenue_summary (
                            ContractType, InternetService ,CustomerCount,TotalRevenue
                            )
                            VALUES (?, ?, ?, ?)
                            """
            data = df_aggrevenue_summary.to_records(index=False).tolist()

            # Insert all rows at once
            gold_db_cursor.executemany(insert_aggrevenue_summary_sql, data)

            # Commit the transaction
            gold_db_cursor.connection.commit()

            messageline = f"{len(data)} rows refreshed into gold_db.dbo.df_aggrevenue_summary table."
            fn_log_message(runid, 'INFO', messageline, 'fn_model_report_data')
        except Exception as err:
            messageline = f"Failed refreshing table gold_db.dbo.df_aggrevenue_summary table {err}."
            fn_log_message(runid, 'ERROR', messageline, 'fn_model_report_data')
            return 1

        try:
            # report use case 04 Customer demographic Presentation
            df_customer_demographics = df_customers.groupby(['Age_band','Gender', 'Tenure_Range', 'TechSupport', 'Churn']).agg(
                                                                            CustomerCount=('CustomerID', 'count'),
                                                                            TotalRevenue=('TotalCharges', 'sum')
                                                                            ).reset_index()

            df_customer_demographics = df_customer_demographics[['Age_band', 'Gender' ,'Tenure_Range','TechSupport', 'Churn',
                                                        'CustomerCount', 'TotalRevenue']]

            # delete data before refresh
            delete_customers_by_category_sql = """ delete from dbo.customer_demographics"""

            gold_db_cursor.execute(delete_customers_by_category_sql)

            # insert report data            
            insert_customer_demographics_sql = """
                            INSERT INTO dbo.customer_demographics (
                            Age_band, Gender ,Tenure_Range,TechSupport, Churn,
                            CustomerCount, TotalRevenue
                            )
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                            """
            data = df_customer_demographics.to_records(index=False).tolist()

            # Insert all rows at once
            gold_db_cursor.executemany(insert_customer_demographics_sql, data)

            # Commit the transaction
            gold_db_cursor.connection.commit()

            messageline = f"{len(data)} rows refreshed into gold_db.dbo.customer_demographics table."
            fn_log_message(runid, 'INFO', messageline, 'fn_model_report_data')
        except Exception as err:
            messageline = f"Failed refreshing table gold_db.dbo.customer_demographics table {err}."
            fn_log_message(runid, 'ERROR', messageline, 'fn_model_report_data')
            return 1

    except Exception as errreportdata:
        messageline = str(errreportdata)
        fn_log_message(runid, 'ERROR', messageline, 'fn_model_report_data')
        return 1
            
    return 0


@flow (name='Customer Data Refresh')
def customer_bi():
    print('')
    print('-----------------------------------------------------------------')

    #customer_bi()
    runid = fn_get_runid()    
    start_status = fn_run_status(runid, 'Run Started')

    if start_status == 0:
        messageline = f'Runid {runid} Started.'
        fn_log_message(runid, 'INFO', messageline, 'customer_bi')

        out1 = fn_extract_load_data()
        if out1 == 0:
            messageline = f'Raw data has been inserted successfully for runid {runid}.'
            fn_log_message(runid, 'INFO', messageline, 'customer_bi')
        else:
            messageline = f'Raw data could not laod runid {runid}, check for errors.'
            # propose an email alert to admin here
            fn_log_message(runid, 'INFO', messageline, 'customer_bi')

        out2 = fn_tranform_data()
        if out2 == 0:
            messageline = f'Tranformed data has been inserted successfully for runid {runid}.'
            fn_log_message(runid, 'INFO', messageline, 'customer_bi')
        else:
            messageline = f'Tranformed data could not laod runid {runid}, check for errors.'
            # propose an email alert to admin here
            fn_log_message(runid, 'INFO', messageline, 'customer_bi')        

        out3 = fn_model_report_data()

        if out3 == 0:
            messageline = f'Reporting data has been inserted successfully for runid {runid}.'
            fn_log_message(runid, 'INFO', messageline, 'customer_bi')
        else:
            messageline = f'Reporting data could not laod runid {runid}, check for errors.'
            # propose an email alert to admin here
            fn_log_message(runid, 'INFO', messageline, 'customer_bi')   

        end_status = fn_run_status(runid, 'Run Ended')
    else:
        messageline = 'Failed Start'
        fn_log_message(runid, 'ERROR', messageline, 'customer_bi')
        start_status = 1
        end_status = 1

    if start_status == 0 and end_status == 0:
        messageline = f'All tasks for runid {runid} are completed successfully.'
        fn_log_message(runid, 'INFO', messageline, 'customer_bi')
    else:
        messageline = f'Some tasks have got errors please check system_db.dbo.ach_logs table data for runid {runid}.'
        fn_log_message(runid, 'ERROR', messageline, 'customer_bi')
    
    fn_disconnct_dbs()
    print('-----------------------------------------------------------------')
    print('')


if __name__ == "__main__":

#    Create a deployment with an hourly schedule
    customer_bi.serve(
        name="customer-bi-deploy",
        cron="0 * * * *"#,  # runs every hour
        #work_pool_name="chitwan-work-pool"
    )

