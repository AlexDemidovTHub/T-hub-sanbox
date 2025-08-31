from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from email.message import EmailMessage
from datetime import timedelta
from airflow.utils.dates import days_ago
import pandas as pd
import requests
import io
import csv
import smtplib

# Чекаем существует ли таблица
check_table_sql = "SELECT 1 FROM public.airports;"

# Создаем таблицу для наших любимых аэропортов
create_table_sql = ("CREATE TABLE IF NOT EXISTS airports("
                    "iata VARCHAR(8),"
                    "lon REAL,"
                    "iso VARCHAR(8),"
                    "status SMALLINT,"
                    "name VARCHAR(64),"
                    "continent VARCHAR(8),"
                    "type VARCHAR(16),"
                    "lat REAL,"
                    "size VARCHAR(16));"
)


hook = PostgresHook(postgres_conn_id='pg_connection')

# Функция для нотификации об ошибках в тасках
def failure_alert(context):
    from email.message import EmailMessage
    import smtplib

    sender = Variable.get('email')
    password = Variable.get('mail_ru_pass')
    recipient = sender

    task_instance = context['task_instance']
    subject = f"Airflow: Task Failed: {task_instance.task_id}"
    body = f"""
    DAG: {task_instance.dag_id}<br>
    Task: {task_instance.task_id}<br>
    Execution Time: {context['data_interval_start']}<br>
    Log URL: {task_instance.log_url}<br>
    Error: {context.get('exception')}
    """

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = recipient
    msg.set_content(body, subtype="html")

    with smtplib.SMTP_SSL("smtp.mail.ru", 465) as server:
        server.login(sender, password)
        server.send_message(msg)

# Из API тянем данные и в виде DataFrame пушим в XCom
def get_airports(**context):

    response = requests.get('https://raw.githubusercontent.com/jbrooksuk/JSON-Airports/refs/heads/master/airports.json')

    data = response.json()
    df = pd.DataFrame(data)
    context['ti'].xcom_push(key='airports', value=df)

# Тянем данные из XCom и добавляем их в таблицу
def save_data(**context):
    data = context['ti'].xcom_pull(key='airports')
    output = io.StringIO()
    data.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.copy_from(output, 'airports', null="", sep='\t')

# Отправляем 10 строчек данных об аэропортах на почту
def send_data():
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT *
                FROM public.airports
                LIMIT 10
            """)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(columns)
    writer.writerows(rows)
    csv_bytes = output.getvalue().encode("utf-8")

    sender = Variable.get('email')
    password = Variable.get('mail_ru_pass')
    recipient = sender

    msg = EmailMessage()
    msg["Subject"] = 'Airports_data'
    msg["From"] = sender
    msg["To"] = recipient
    msg.add_attachment(csv_bytes, maintype="text", subtype="csv", filename="airports.csv")

    with smtplib.SMTP_SSL("smtp.mail.ru", 465) as server:
        server.login(sender, password)
        server.sendmail(sender, recipient, msg.as_string())

default_args = {
    'on_failure_callback': failure_alert,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'exponential_backoff': True,
    'depends_on_past': False
}


with DAG(
    dag_id='AIRPORTS_ONE_LOVE',
    schedule_interval='0 8 * * 1-5',
    start_date=days_ago(1),
    max_active_runs=1,
    default_args=default_args
) as dag:

    get_data = PythonOperator(
        task_id='get_airports',
        python_callable=get_airports
    )

    check_schema = SqlSensor(
        task_id='check_airports_schema',
        conn_id='pg_connection',
        sql=check_table_sql,
        mode='poke',
        poke_interval=1,
        timeout=10

    )

    create_schema = PostgresOperator(
        task_id='create_schema',
        postgres_conn_id='pg_connection',
        sql=create_table_sql,
        trigger_rule='one_failed'

    )

    data_save = PythonOperator(
        task_id='save_data',
        python_callable=save_data,
        trigger_rule='none_failed'
    )

    data_send = PythonOperator(
        task_id='send_data',
        python_callable=send_data,
        trigger_rule='none_failed'

    )


get_data >> check_schema >> create_schema >> data_save >> data_send
