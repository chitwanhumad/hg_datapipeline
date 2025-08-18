from prefect import task, flow
import pandas as pd
import pyodbc

# Connection string
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost\\SQLEXPRESS;"  # or "SERVER=your_server,1433"
    "DATABASE=Campaign;"
    "UID=sa;"                        # username
    "PWD=unica*03;"              # password
)

cursor = conn.cursor()
cursor.execute("SELECT TOP 10 * FROM customers")
for row in cursor.fetchall():
    print(row)
    print(row)

@task()
def extract_data():
    file = 'D:\\HGInsights\Git\\hg_datapipeline\\sample_data\\customer_churn_data.csv'
    df_customer = pd.read_csv(file)

    print('Data is being read from folder D:\HGInsights\Source')
    return 0

@task 
def load_data():
    print('Loading data into sqlserverdata base...')    
    return 0

@task 
def tranform_data():
    print('Transfor data for reports...')
    return 0

@task 
def model_data():
    print('Transfor data for reports...')
    return 0

@flow
def customer_bi():
    out1 = extract_data()
    out2 = load_data()
    out3 = tranform_data()
    out4 = model_data()
    print('Tasks are completed.')

if __name__ == "__main__" :
    customer_bi()
