from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd

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

# Тянем данные из таблиц user_profile и user_activity и пушим в XCom
def extract_users_data(**context):
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT *
                FROM de_assignment.user_profiles;
            """)
            user_profiles = cur.fetchall()
            context['ti'].xcom_push(key='user_profiles', value=user_profiles)

            cur.execute(f"""
                            SELECT *
                            FROM de_assignment.user_activity;
                        """)
            user_activity = cur.fetchall()
            context['ti'].xcom_push(key='user_activity', value=user_activity)



def transform_user_profiles_data(**context):
    ti  = context['ti']
    user_activity = ti.xcom_pull(key='user_activity')
    user_profiles = ti.xcom_pull(key='user_profiles')
    df_ua = pd.DataFrame(user_activity, columns=['user_id', 'activity_date', 'activity_type', 'duration_minutes'])
    df_up = pd.DataFrame(user_profiles, columns=['user_id', 'signup_date', 'tier', 'region'])

    # Джоиним таблицы user_profile и user_activity и фильтруем данные за последнюю неделю
    df_merged = pd.merge(df_ua, df_up, how='inner', on='user_id')
    max_date = df_merged['activity_date'].max()
    last_week = max_date - pd.Timedelta(days=7)
    df_filtered = df_merged[df_merged['activity_date'] >= last_week]

    # Считаем количество активностей пользователей и суммарное время активности для каждого пользователя
    df_final = (
        df_filtered
        .groupby('user_id', as_index=False)
        .agg(
            user_activities=('activity_type', 'count'),
            total_user_duration=('duration_minutes', 'sum')
        ))

    # Список куда будем складывать запросы на изменение tier значений
    query_list = list()

    # Строка куда в последующем будут добавляться запросы изменения данных
    final_query = str()

    # Находим пользователей с активностью меньше 5 действий и сохраняем запрос для изменения данных в список
    bronze_tier = df_final[df_final['user_activities'] < 5]
    bronze_tier = tuple(bronze_tier['user_id'])
    if bronze_tier:
        sql = f'''
            UPDATE de_assignment.user_profiles
            SET tier = 'bronze'
            WHERE user_id in {bronze_tier};'''
        query_list.append(sql)

    else:
        print('За последнюю неделю отсутствуют пользователи с активностью меньше 5 действий')


    silver_tier = df_final[(df_final['user_activities'] >= 5) & (df_final['user_activities'] <= 15)]
    silver_tier = tuple(silver_tier['user_id'])
    if silver_tier:
        sql = f'''
            UPDATE de_assignment.user_profiles
            SET tier = 'silver'
            WHERE user_id in {silver_tier};'''
        query_list.append(sql)
    else:
        print('За последнюю неделю отсутствуют пользователи с активностью от 5 до 15 действий')


    gold_tier = df_final[df_final['user_activities'] >= 15]
    gold_tier = tuple(gold_tier['user_id'])
    if gold_tier:
        sql = f'''
            UPDATE de_assignment.user_profiles
            SET tier = 'gold'
            WHERE user_id in {gold_tier};'''
        query_list.append(sql)
    else:
        print('За последнюю неделю отсутствуют пользователи с активностью более 15 действий')

    for i in range(len(query_list)):
        final_query += query_list[i].strip()

    context['ti'].xcom_push(key='final_query', value=final_query)


default_args = {
    'on_failure_callback': failure_alert,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'exponential_backoff': True,
    'depends_on_past': False
}


with DAG(
    dag_id='WEEKLY_USER_SEGMENTATION',
    schedule='0 0 * * MON',
    start_date=datetime(2025, 8, 22),
    default_args=default_args
) as dag:

    extract_user_profiles = PythonOperator(
        task_id='extract_user_profiles',
        python_callable=extract_users_data
    )

    calculate_user_metrics = PythonOperator(
        task_id='calculate_user_metrics',
        python_callable=transform_user_profiles_data
    )

    update_user_tiers = PostgresOperator(
        task_id='update_user_tiers',
        postgres_conn_id='pg_connection',
        sql='{{ ti.xcom_pull(key="final_query") }}'
    )


extract_user_profiles >> calculate_user_metrics >> update_user_tiers
