from prefect import task, flow
import pandas as pd
import pyodbc
import os, sys
import glob

# read configurations from config.ini
from configparser import ConfigParser
# CREATE OBJECT
config = ConfigParser()
config.read('D:\HGInsights\Git\hg_datapipeline\config.ini')

# Read configurations
v_root_folder=config['DEFAULT']['v_root_folder']

server=config['DATABASE']['server']
user=config['DATABASE']['user']
password=config['DATABASE']['password']

# function Connection string
def fn_connection (dbname):
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

def fn_disconnct_dbs():
    system_db_conn.close()
    system_db_cursor.close()
    bronze_db_conn.close()
    bronze_db_cursor.close()
    silver_db_conn.close()
    silver_db_cursor.close()
    gold_db_conn.close()
    gold_db_cursor.close()


def fn_get_runid():
    system_db_cursor.execute('SELECT NEXT VALUE FOR system_db.dbo.sq_runid AS NewRunId;')
    runidrow = system_db_cursor.fetchone()
    runid = runidrow[0]
    return int(runid)

runid = fn_get_runid()
print('------------------------------------')
print('Current runid is:', runid)
print('------------------------------------')
print('')

# Function to get sql server datetime id
def fn_getsql_datetime():
    system_db_cursor.execute('select getdate()')
    getdaterow = system_db_cursor.fetchone()
    curr_datetime = getdaterow[0]
    return curr_datetime

# Function to insert run status
def fn_run_status(runid,status):
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

        if status == 'started':
            system_db_cursor.execute(insertsql, (runid, curr_datetime, curr_datetime,0,status))
            system_db_cursor.connection.commit()
        elif status == 'ended':
            system_db_cursor.execute(updatesql, (curr_datetime, status, runid))
            system_db_cursor.connection.commit()

        return 0
    except Exception as e:
        message_line = (f"Error inserting log: {e}")
        print(message_line)
        fn_log_message(runid,'ERROR', message_line,'fn_run_status')
        return 1

# Function to insert log messages
def fn_log_message(runid,loglevel,messageline,function_name):
    try:
        sql = """
        INSERT INTO dbo.ach_logs (runid, loglevel, message_line,function_name)
        VALUES (?, ?, ?, ?)
        """
        system_db_cursor.execute(sql, (runid, loglevel, messageline,function_name))
        system_db_cursor.connection.commit()

    except Exception as e:
        print(f"Error inserting log: {e}")

@task()
def fn_extract_data():
    try:
        # read source files
        v_source_folder = v_root_folder+'source/'
        # make a list of all raw input files
        raw_files = glob.glob(os.path.join(v_source_folder, "*.csv"))
        
        # initiaize an empty dataframe
        df_customers = pd.DataFrame(columns=['CustomerID','Age','Gender','Tenure','MonthlyCharges','ContractType','InternetService','TotalCharges','TechSupport','Churn','runid'])

        if len(raw_files) > 0:
            for file in raw_files:
                print(file)
                df_curr_data = pd.read_csv(file, header=0)
                df_curr_data['runid'] = runid
                df_customers = pd.concat([df_curr_data,df_customers])

        # To fix longer decimal values for TotalCharges
        df_customers['TotalCharges'] = pd.to_numeric(df_customers['TotalCharges'], errors='coerce')
        df_customers = df_customers.astype({
                                            "CustomerID": "int",
                                            "Age": "int",
                                            "Tenure": "int",
                                            "MonthlyCharges": "float",
                                            "TotalCharges": "float", # to accect long float values 
                                            "runid": "int"
                                        })
        df_customers['InternetService'] = df_customers['InternetService'].fillna('missing')
        
        print('')
        if len(df_customers) > 0:
            # assuming df_customers has same column order as raw_customers
            insert_sql = """
            INSERT INTO raw_customers (
                CustomerID, Age, Gender, Tenure, MonthlyCharges,
                ContractType, InternetService, TotalCharges,
                TechSupport, Churn, runid
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            data = df_customers.to_records(index=False).tolist()

            # Insert all rows at once
            bronze_db_cursor.executemany(insert_sql, data)

            # Commit the transaction
            bronze_db_cursor.connection.commit()

            messageline = f"{len(data)} rows inserted into raw_customers.."
            print(messageline)
            fn_log_message(runid,'INFO',messageline,'fn_extract_data')

        return 0
    except Exception as err_load_data:
        messageline = str(err_load_data)
        print(messageline)
        fn_log_message(runid,'ERROR',messageline,'fn_extract_data')        
        return 1

@task 
def fn_load_data():
    print('Loading data into sqlserverdata base...')    
    return 0

@task 
def fn_tranform_data():
    print('Transfor data for reports...')
    return 0

@task 
def fn_model_data():
    print('Transfor data for reports...')
    return 0

@flow
def customer_bi():
    print('')
    print('-----------------------------------------------------------------')    
    start_status = fn_run_status(runid, 'started')

    if start_status == 0:
        messageline = f'Runid {runid} Started.'
        fn_log_message(runid,'INFO',messageline,'customer_bi') 

        out1 = fn_extract_data()
        out2 = fn_load_data()
        out3 = fn_tranform_data()
        out4 = fn_model_data()
        end_status = fn_run_status(runid, 'ended')
    else:
        messageline = 'Failed Start'
        fn_log_message(runid,'ERROR',messageline,'customer_bi') 
        start_status = 1
        end_status = 1
    
    if start_status == 0 and end_status == 0:
        messageline = f'All tasks for runid {runid} are completed successfully.'
        fn_log_message(runid,'INFO',messageline,'customer_bi') 
    else:
        messageline = f'Some tasks have got errors please check system_db.dbo.ach_logs table data for runid {runid}.'
        fn_log_message(runid,'ERROR',messageline,'customer_bi') 

    print('-----------------------------------------------------------------')
    print('')

if __name__ == "__main__" :
    customer_bi()
    fn_disconnct_dbs()