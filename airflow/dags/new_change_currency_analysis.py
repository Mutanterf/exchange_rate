from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import requests
import csv
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from airflow.providers.postgres.hooks.postgres import PostgresHook


def fetch_and_save_exchange_rate_from_csv_to_db(**kwargs):
    filename = '/tmp/exchange_rates.csv'
    inverse_rates = {}

    with open(filename, mode='r') as file:
        reader = csv.reader(file)
        next(reader) 
        for row in reader:
            currency, rate = row
            inverse_rates[currency] = float(rate)

    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    
    for currency, rate in inverse_rates.items():
        sql = """
            INSERT INTO exchange_rates (date, currency, rate)
            VALUES (%s, %s, %s)
            ON CONFLICT (date, currency) DO UPDATE SET rate = EXCLUDED.rate;
        """
        pg_hook.run(sql, parameters=(datetime.today().date(), currency, rate))



def analyze_and_generate_signals(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    
    sql = """
        SELECT date, currency, rate FROM exchange_rates
        WHERE date = (SELECT MAX(date) FROM exchange_rates) 
        OR date = (SELECT MAX(date) - INTERVAL '1 day' FROM exchange_rates);
    """
    records = pg_hook.get_records(sql)

    if len(records) < 2:
        return "Недостаточно данных для анализа курса"

    today_records = {record[1]: record[2] for record in records if record[0] == datetime.today().date()}
    yesterday_records = {record[1]: record[2] for record in records if record[0] == datetime.today().date() - timedelta(days=1)}

    for currency, today_rate in today_records.items():
        yesterday_rate = yesterday_records.get(currency)
        if not yesterday_rate:
            continue

        change = ((today_rate - yesterday_rate) / yesterday_rate) * 100

        if change > 2:
            signal = "Можно продавать!"
        elif change < -2:
            signal = "Пора покупать!"
        else:
            signal = None

        if signal:
            sql = """
                INSERT INTO trade_signals (date, currency, signal, rate) 
                VALUES (%s, %s, %s, %s);
            """
            pg_hook.run(sql, parameters=(datetime.today().date(), currency, signal, today_rate))

    return "Сигналы обновлены успешно!"


def fetch_exchange_rates(**kwargs):
    url = 'https://v6.exchangerate-api.com/v6/4c012a40bda498b9decb2f82/latest/KZT'
    response = requests.get(url)
    data = response.json()
    inverse_rates = {currency: round(1 / rate, 4) for currency, rate in data['conversion_rates'].items() if rate != 0}
    
    kwargs['ti'].xcom_push(key='inverse_rates', value=inverse_rates)

def save_to_csv(**kwargs):
    inverse_rates = kwargs['ti'].xcom_pull(key='inverse_rates', task_ids='fetch_exchange_rates')
    filename = '/tmp/exchange_rates.csv'
    
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Currency', 'KZT Equivalent (1 / rate)'])
        for currency, rate in inverse_rates.items():
            writer.writerow([currency, rate])

def send_email_manual():
    sender = 'mutanterfmika@gmail.com'
    receiver = 'mutanterfmika@gmail.com'
    password = 'jyru gqcq dsvq ccmt' 

    msg = MIMEMultipart()
    msg['From'] = sender
    msg['To'] = receiver
    msg['Subject'] = 'Задача выполнена'


    body = 'Валютный курс:'
    msg.attach(MIMEText(body, 'plain'))

    filename = '/tmp/exchange_rates.csv'

    with open(filename, 'rb') as attachment:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename={filename.split("/")[-1]}')
        msg.attach(part)

    with smtplib.SMTP('smtp.gmail.com', 587) as server:
        server.starttls()
        server.login(sender, password)
        server.send_message(msg)

default_args = {
    'start_date': datetime(2025, 4, 1),
}

with DAG(
    dag_id='exchange_rate_pipeline_new',
    default_args=default_args,
    schedule_interval= '@daily',
    catchup=False,
    description='Загрузка валюты, сохранение и отправка по почте',
) as dag:

    task_fetch = PythonOperator(
        task_id='fetch_exchange_rates',
        python_callable=fetch_exchange_rates,
        provide_context=True
    )

    task_save = PythonOperator(
        task_id='save_to_csv',
        python_callable=save_to_csv,
        provide_context=True
    )

    task_notify = PythonOperator(
        task_id='send_email_notification',
        python_callable=send_email_manual
    )

    task_save_to_db = PythonOperator(
        task_id='fetch_and_save_exchange_rate_from_csv_to_db',
        python_callable=fetch_and_save_exchange_rate_from_csv_to_db,
        provide_context=True
    )

    task_generate_signals = PythonOperator(
        task_id='analyze_and_generate_signals',
        python_callable=analyze_and_generate_signals,
        provide_context=True
    )

    task_fetch >> task_save >> task_notify >> task_save_to_db >> task_generate_signals
