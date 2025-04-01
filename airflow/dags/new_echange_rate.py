from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime
import requests
import csv
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

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
    dag_id='exchange_rate_pipeline',
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

    task_fetch >> task_save >> task_notify
