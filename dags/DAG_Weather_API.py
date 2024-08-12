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
    'start_date': days_ago(0),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}
dag = DAG(
    'etl_weather',
    default_args=default_args,
    description='ETL Weather API',
    schedule_interval='30 * * * *',
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
        url_weather = f'https://api.weatherbit.io/v2.0/current?key={api_key}&city={city}'
        response_weather = requests.get(url_weather)
        data_weather = response_weather.json()
        kwargs['ti'].xcom_push(key='extract_weather', value=data_weather)
    
    except Exception as e:
        # Crear el correo electrónico
        subject = "ERROR on DAG_Weather_API extract_weather"
        body = (f"ERROR {e}")
        send_email(subject, body)
        raise
    
# Función para seleccionar los campos de weather_data
def transform(**kwargs):
    try:
        data_weather = kwargs['ti'].xcom_pull(key='extract_weather', task_ids='extract_weather_data')

        if data_weather.get('data'):
        
            entry = data_weather.get('data')[0]

            # Seleccionar los campos necesarios
            select_data = {key: entry.get(key, [None]) for key in [
                'country_code', 'state_code', 'city_name', 'lat', 'lon', 'station', 'datetime',
                'timezone', 'temp', 'app_temp', 'dewpt', 'rh', 'clouds', 'precip',
                'snow', 'aqi', 'wind_spd', 'gust', 'wind_cdir', 'wind_dir', 'solar_rad', 'dhi', 'dni','ghi', 'elev_angle',
                'h_angle', 'pres','slp', 'sunrise', 'sunset',
            ]}
            select_data['datetime'] = datetime.strptime(select_data['datetime'], "%Y-%m-%d:%H")

            kwargs['ti'].xcom_push(key='transform_weather', value=select_data)
    except Exception as e:
        # Crear el correo electrónico
        subject = "ERROR on DAG_Weather_API transform_weather_data"
        body = (f"ERROR {e}")
        send_email(subject, body)
        raise

# Función para cargar los datos en PostgreSQL
def load(**kwargs):
    try:
        # Obtener datos desde XCom
        data_json = kwargs['ti'].xcom_pull(key='transform_weather', task_ids='transform_weather_data')

        # Convertir JSON a DataFrames
        df_data = pd.DataFrame([data_json]) if data_json else pd.DataFrame()

        # Cargar datos en PostgreSQL
        if not df_data.empty:

            # Configuración de conexión a la base de datos
            user = os.getenv('DB_USER', 'default_user')
            password = os.getenv('DB_PASSWORD', 'default_password')
            host = os.getenv('DB_HOST', 'localhost')
            db = os.getenv('DB_NAME', 'weather')
            engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}/{db}')

            df_data.to_sql('weather_data', engine, index=False, if_exists='append')
            kwargs['ti'].xcom_push(key='load_weather', value=True)

            return "Data loaded successfully!"
        
        kwargs['ti'].xcom_push(key='load_weather', value=False)
        return "No data to load"
    
    except Exception as e:
        # Crear el correo electrónico
        subject = "ERROR on DAG_Weather_API load_data"
        body = (f"ERROR {e}")
        send_email(subject, body)
        raise

# Función para enviar correo electrónico
def send_email_data(**kwargs):
    try:
        # Verificar si los datos fueron cargados
        data_loaded = kwargs['ti'].xcom_pull(key='load_weather', task_ids='load_postgres')

        if data_loaded:
            # Crear el correo electrónico
            subject = "Postgre DB"
            body = ("Weather data loaded successfully into Weather table!!")

            send_email(subject, body)

            return "Email sent successfully!"
        return "No data to send"
    
    except Exception as e:
        # Crear el correo electrónico
        subject = "ERROR on DAG_Weather_Alerts_API send_email_data"
        body = (f"ERROR {e}")
        send_email(subject, body)
        raise


# Definición de las tareas
extract_weather_data = PythonOperator(
    task_id='extract_weather_data',
    python_callable=extract,
    provide_context=True,
    dag=dag
)

transform_weather_data = PythonOperator(
    task_id='transform_weather_data',
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

email_data_loaded = PythonOperator(
    task_id='email_weather_alert',
    python_callable=send_email_data,
    provide_context=True,
    dag=dag
)

# Definición del flujo de tareas
extract_weather_data >> transform_weather_data >> load_postgres >> email_data_loaded
