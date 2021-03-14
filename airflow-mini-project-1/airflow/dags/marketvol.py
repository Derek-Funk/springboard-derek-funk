from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import date, datetime, timedelta
import yfinance as yf

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['derek.visitor11@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

dag = DAG(
    'marketvol',
    default_args=default_args,
    description='marketvol',
    schedule_interval='0 18 * * 1,2,3,4,5', #run every weekday at 6pm
    start_date=datetime(2021, 3, 12), #1st available weekday as of this dag's creation
    tags=['airflow-mini-project-1'],
)

# start_date = date(2021, 3, 12) #test
start_date = date.today()
end_date = start_date + timedelta(days=1)

# t0: this writes to the worker container (e.g. airflow-mini-project-1_airflow-worker_1)
t0 = BashOperator(
    task_id='t0_createTempFolder',
    bash_command='mkdir -p /tmp/data/{}'.format(start_date),
    dag=dag,
)

# t1, t2: this downloads today's stock files
def download_stock_data(ticker, start_date, end_date):
    df = yf.download(ticker, start=start_date, end=end_date, interval='1m')
    df.to_csv('{}_{}.csv'.format(ticker, start_date), header=False)
    return

t1 = PythonOperator(
    task_id='t1_downloadAaplData',
    python_callable=download_stock_data,
    op_kwargs={'ticker':'AAPL', 'start_date':start_date, 'end_date':end_date},
    dag=dag
)

t2 = PythonOperator(
    task_id='t2_downloadTslaData',
    python_callable=download_stock_data,
    op_kwargs={'ticker':'TSLA', 'start_date':start_date, 'end_date':end_date},
    dag=dag
)

# t3, t4: this moves the downloaded files to the tmp folder
t3 = BashOperator(
    task_id='t3_moveAaplFileToTempFolder',
    bash_command='''
        AAPL_FILE=AAPL_{}.csv
        PATH_TO_TEMP_FOLDER=/tmp/data/{}
        mv $HOME/$AAPL_FILE $PATH_TO_TEMP_FOLDER/$AAPL_FILE
    '''.format(start_date, start_date),
    dag=dag
)

t4 = BashOperator(
    task_id='t4_moveTslaFileToTempFolder',
    bash_command='''
        TSLA_FILE=TSLA_{}.csv
        PATH_TO_TEMP_FOLDER=/tmp/data/{}
        mv $HOME/$TSLA_FILE $PATH_TO_TEMP_FOLDER/$TSLA_FILE
    '''.format(start_date, start_date),
    dag=dag
)

# t5: email the csvs as attachments
t5 = EmailOperator(
    task_id="t5_emailData", 
    to='derek.visitor11@gmail.com',
    subject='AAPL & TSLA data for {}'.format(start_date),
    html_content="<p> Please find attached today's stock data for AAPL and TSLA.  <p>",
    files=['/tmp/data/{}/AAPL_{}.csv'.format(start_date, start_date), '/tmp/data/{}/TSLA_{}.csv'.format(start_date, start_date)],
    dag=dag
)

t0 >> [t1, t2]
t1 >> t3 >> t5
t2 >> t4 >> t5