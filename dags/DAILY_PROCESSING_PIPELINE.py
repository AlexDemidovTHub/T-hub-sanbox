from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.base_hook import BaseHook
from airflow.models import Connection
from airflow.utils.dates import days_ago
from airflow.models import Variable
import logging
import pandas as pd
from datetime import timedelta


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


def extract_activities(**context):
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(user_id)
                FROM de_assignment.user_activity
                GROUP BY user_id
            """)

            active_users = cur.fetchone()

            logging.info(f"Количество активных пользователей за вчерашний день - {active_users[0]}")

            # Получаем данные, где активность пользователей на каждом этапе не менее 5 минут
            cur.execute("""
                SELECT *
                FROM de_assignment.user_activity
                WHERE duration_minutes > 5 AND activity_date = CURRENT_DATE - 1;
            """)
            data = cur.fetchall()

    # Пушим данные в XCom
    context['ti'].xcom_push(key='user_activity', value=data)

def transform_data(**context):
    # Тянем данные из XCom и создаем Pandas DataFrame
    ti  = context['ti']
    data = ti.xcom_pull(key='user_activity')
    df = pd.DataFrame(data, columns=['user_id', 'activity_date', 'activity_type', 'duration_minutes'])

    selected_rows = len(df)

    # Дополняем DataFrame названием дня недели
    df['activity_date'] = pd.to_datetime(df['activity_date'])
    df['weekday_name'] = df['activity_date'].dt.day_name()

    # Делим данные на три группы, в зависимости от времени нахождения пользователя на каждом типе активности
    def set_duration_bucket(duration_minutes):
        if duration_minutes <= 30:
            return '0-30min'
        elif 60 < duration_minutes > 30:
            return '30-60min'
        else:
            return '60+ min'

    df['duration_bucket'] = df['duration_minutes'].apply(set_duration_bucket)

    context['ti'].xcom_push(key='enriched_user_activity', value=df)

    # Коннект к базе данных и создание временной таблицы, куда будем складывать преобразованные данные
    hook = PostgresHook(postgres_conn_id='pg_connection')
    engine = hook.get_sqlalchemy_engine()

    df.to_sql(
        name='temp_daily_activity_metrics',
        con=engine,
        schema='de_assignment',
        if_exists='replace',
        index=False
    )

# Переменная с транзакцией для PostgresOperator с созданием целевой таблицы и наполнением её данными с расчетами метрик
sql = '''
BEGIN;

create table if not exists de_assignment.daily_activity_metrics (
	user_id INT,
	activity_date TIMESTAMP,
	weekday_name VARCHAR(50),
	duration_bucket VARCHAR(50),
	activity_count INT,
	avg_minutes real
);

insert into de_assignment.daily_activity_metrics(
select 
    user_id,
    activity_date,
    weekday_name,
    duration_bucket,
    count(activity_type) as activity_count,
    avg(duration_minutes) as avg_minutes
from de_assignment.temp_daily_activity_metrics
group by user_id, activity_date, weekday_name, duration_bucket);

COMMIT;
'''


default_args = {
    'on_failure_callback': failure_alert,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'exponential_backoff': True,
    'depends_on_past': True
}


with DAG(
    dag_id='DAILY_PROCESSING_PIPELINE',
    schedule='@daily',
    start_date=days_ago(1),
    max_active_runs=1,
    default_args=default_args
) as dag:

    extract_daily_activities = PythonOperator(
        task_id='extract_daily_activities',
        python_callable=extract_activities
    )

    transform_activity_data = PythonOperator(
        task_id='transform_activity_data',
        python_callable=transform_data
    )

    load_daily_aggregates = PostgresOperator(
        task_id='load_daily_aggregates',
        postgres_conn_id='pg_connection',
        sql=sql
    )


extract_daily_activities >> transform_activity_data >> load_daily_aggregates
