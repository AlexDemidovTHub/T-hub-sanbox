from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import pandas as pd
import logging
from datetime import datetime, timedelta

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


def quality_check(**context):
    # Тянем данные из таблицы и преобразовываем в Pandas DataFrame
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT *
                FROM de_assignment.user_activity
            """)
            data = cur.fetchall()

    df = pd.DataFrame(data, columns=['user_id', 'activity_date', 'activity_type', 'duration_minutes'])

    # Проверяем ключевые поля на наличие нулевых значений
    user_id_nulls = df[df['user_id'].isnull()]
    activity_date_nulls = df[df['activity_date'].isnull()]

    # Если пустые поля есть, то выводим их в логи
    if df['activity_date'].isnull().sum() > 0:
        logging.info('Обнаружены Null в следующих строчках: \n{}'.format(activity_date_nulls.to_string(index=False)))

    if df['user_id'].isnull().sum() > 0:
        logging.info('Обнаружены Null в следующих строчках: \n{}'.format(user_id_nulls.to_string(index=False)))

    df['activity_date'] = pd.to_datetime(df['activity_date'])

    # Проверяем валидность дат
    current_date = datetime.now()
    future_dates = df[df['activity_date'] > current_date]

    # Если есть поля с невалидной датой, то выводим их в логи
    if (df['activity_date'] > current_date).any():
        logging.info('Обнаружены записи с невалидными данными в следующих строчках: \n{}'.format(future_dates.to_string(index=False)))

    # Логируем кастомные метрики
    logging.info(f'Количество пустых полей в user_id: {len(user_id_nulls)}')
    logging.info(f'Количество пустых полей в activity_date: {len(activity_date_nulls)}')
    logging.info(f'Количество невалидных значений даты: {len(future_dates)}')

# Транзакция для создания архивной таблицы и перекладка в неё данных старше года
def archive_data():
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                BEGIN;

                create table if not exists de_assignment.user_activity_archive (
                    user_id INT,
                    activity_date TIMESTAMP,
                    weekday_name VARCHAR(50),
                    duration_bucket VARCHAR(50),
                    activiti_count INT,
                    avg_minutes real
                );

                insert into de_assignment.user_activity_archive(
                select *
                from de_assignment.user_activity
                where activity_date < CURRENT_date - 365
                );

                delete from de_assignment.user_activity
                where activity_date < CURRENT_date - 365
                ;

                COMMIT;
            """)

vacuum_user_activity = """
    VACUUM ANALYZE de_assignment.user_activity;
"""

vacuum_user_profiles = """
    VACUUM ANALYZE de_assignment.user_profiles;
"""

vacuum_daily_metrics = """
    VACUUM ANALYZE de_assignment.daily_activity_metrics;
"""

default_args = {
    'on_failure_callback': failure_alert,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'exponential_backoff': True,
    'depends_on_past': False

}


with DAG(
        dag_id='AD_HOC_MAINTENANCE',
        schedule=None,
        default_args=default_args
) as dag:
    validate_data_quality = PythonOperator(
        task_id='validate_data_quality',
        python_callable=quality_check
    )

    archive_old_data = PythonOperator(
        task_id='archive_old_data',
        python_callable=archive_data
    )

    vacuum_user_activity_task = PostgresOperator(
        task_id='vacuum_user_activity',
        postgres_conn_id='pg_connection',
        sql=vacuum_user_activity,
        autocommit=True
    )

    vacuum_user_profiles_task = PostgresOperator(
        task_id='vacuum_user_profiles',
        postgres_conn_id='pg_connection',
        sql=vacuum_user_profiles,
        autocommit=True
    )

    vacuum_daily_metrics_task = PostgresOperator(
        task_id='vacuum_daily_metrics',
        postgres_conn_id='pg_connection',
        sql=vacuum_daily_metrics,
        autocommit=True
    )

validate_data_quality >> archive_old_data >> [vacuum_user_activity_task, vacuum_user_profiles_task,
                                              vacuum_daily_metrics_task]