from prefect import task, flow

@task()
def extract_data():
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
