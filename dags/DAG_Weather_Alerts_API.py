from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr
from datetime import datetime, timedelta
import requests
import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv('/opt/airflow/dags/Weather_Credentials.env')

# Configuración de la DAG
default_args = {
    'owner': 'immperiaal',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=20)
}
dag = DAG(
    'etl_weather_alerts',
    default_args=default_args,
    description='ETL Weather Alerts API',
    schedule_interval='30 10 * * *',
    catchup=False
)

#Funcion enviar email
def send_email(subject, body):
    try:
        smtp_server = 'smtp.gmail.com'
        smtp_port = 587
        email = os.getenv('EMAIL', 'default_email')
        app_password = os.getenv('EMAIL_PASSWORD', 'default_password')

        msg = MIMEText(body, 'plain')
        msg['From'] = formataddr(('Sender Name', email))
        msg['To'] = email
        msg['Subject'] = subject

        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(email, app_password)
        server.sendmail(email, [email], msg.as_string())
        server.quit()
    except Exception as e:
        print(f"Error sending error email: {e}")

# Función para extraer datos de la API
def extract(**kwargs):
    try:
        api_key = os.getenv('API_KEY', 'default_key')
        city = 'Murcia'
        url_alerts = f'https://api.weatherbit.io/v2.0/alerts?key={api_key}&city={city}'
        response_alerts = requests.get(url_alerts)
        data_alerts = response_alerts.json()
        kwargs['ti'].xcom_push(key='extract_alerts', value=data_alerts)

    except Exception as e:
        # Crear el correo electrónico
        subject = "ERROR on DAG_Weather_Alerts_API extract_alerts"
        body = (f"ERROR {e}")
        send_email(subject, body)
        raise

# Función para seleccionar los campos de weather_alerts
def transform(**kwargs):
    try:    
        data_alerts = kwargs['ti'].xcom_pull(key='extract_alerts', task_ids='extract_weather_alerts')

        if data_alerts.get('alerts'):

            general_info = {key: data_alerts.get(key, [None]) for key in [
                'country_code', 'state_code', 'city_name', 'lat', 'lon', 'timezone'
            ]}

            entry = data_alerts.get('alerts')[0]

            alert_data = {key: entry.get(key, [None]) for key in [
                'regions', 'title', 'description', 'ends_utc', 'onset_utc', 'expires_utc', 'effective_utc'
            ]}
            
            alert_data['ends_utc'] = datetime.strptime(alert_data['ends_utc'], "%Y-%m-%dT%H:%M:%S")
            alert_data['onset_utc'] = datetime.strptime(alert_data['onset_utc'], "%Y-%m-%dT%H:%M:%S")
            alert_data['expires_utc'] = datetime.strptime(alert_data['expires_utc'], "%Y-%m-%dT%H:%M:%S")
            alert_data['effective_utc'] = datetime.strptime(alert_data['effective_utc'], "%Y-%m-%dT%H:%M:%S")
            
            # Unir todos los campos en un solo diccionario
            select_data = {**general_info, **alert_data}

            kwargs['ti'].xcom_push(key='transform_weather', value=select_data)

    except Exception as e:
        # Crear el correo electrónico
        subject = "ERROR on DAG_Weather_Alerts_API select_weather_alerts"
        body = (f"ERROR {e}")
        send_email(subject, body)
        raise

# Función para cargar los datos en PostgreSQL
def load(**kwargs):
    try:
        # Obtener datos desde XCom
        alerts_json = kwargs['ti'].xcom_pull(key='transform_weather', task_ids='transform_weather_alerts')

        # Convertir JSON a DataFrames
        df_alerts = pd.DataFrame([alerts_json]) if alerts_json else pd.DataFrame()


        # Cargar datos en PostgreSQL       
        if not df_alerts.empty:

            # Configuración de conexión a la base de datos
            user = os.getenv('DB_USER', 'default_user')
            password = os.getenv('DB_PASSWORD', 'default_password')
            host = os.getenv('DB_HOST', 'localhost')
            db = os.getenv('DB_NAME', 'weather')
            engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}/{db}')

            df_alerts.to_sql('weather_alerts', engine, index=False, if_exists='append')
            kwargs['ti'].xcom_push(key='load_weather', value=True)

            return "Data loaded successfully!"
        
        kwargs['ti'].xcom_push(key='load_weather', value=False)
        return "No data to load"
    
    except Exception as e:
        # Crear el correo electrónico
        subject = "ERROR on DAG_Weather_Alerts_API load_data"
        body = (f"ERROR {e}")
        send_email(subject, body)
        raise
    
# Función para enviar correo electrónico
def send_email_alerts(**kwargs):
    try:
        # Verificar si los datos fueron cargados
        data_loaded = kwargs['ti'].xcom_pull(key='load_weather', task_ids='load_postgres')

        if data_loaded:
            # Obtener datos desde XCom
            alerts_json = kwargs['ti'].xcom_pull(key='transform_weather', task_ids='transform_weather_alerts')
            df_alerts = pd.DataFrame([alerts_json]) if alerts_json else pd.DataFrame()

            # Crear el correo electrónico
            subject = "WEATHER ALERT!!!"
            body = (f"title: {df_alerts['title'].values[0]}\n"
                    f"description: {df_alerts['description'].values[0]}\n"
                    f"regions: {df_alerts['regions'].values[0]}\n"
                    f"ends_utc: {df_alerts['ends_utc'].values[0]}\n"
                    f"expires_utc: {df_alerts['expires_utc'].values[0]}\n")

            send_email(subject, body)

            return "Email sent successfully!"
        return "No data to send"
    
    except Exception as e:
        # Crear el correo electrónico
        subject = "ERROR on DAG_Weather_Alerts_API send_email_weather_alerts"
        body = (f"ERROR {e}")
        send_email(subject, body)
        raise

# Definición de las tareas
extract_weather_alerts = PythonOperator(
    task_id='extract_weather_alerts',
    python_callable=extract,
    provide_context=True,
    dag=dag
)

transform_weather_alerts = PythonOperator(
    task_id='transform_weather_alerts',
    python_callable=transform,
    provide_context=True,
    dag=dag
)

load_postgres = PythonOperator(
    task_id='load_postgres',
    python_callable=load,
    provide_context=True,
    dag=dag
)

email_weather_alert = PythonOperator(
    task_id='email_weather_alert',
    python_callable=send_email_alerts,
    provide_context=True,
    dag=dag
)

# Definición del flujo de tareas
extract_weather_alerts >> transform_weather_alerts >> load_postgres >> email_weather_alert
