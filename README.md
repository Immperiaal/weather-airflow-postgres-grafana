# weather-airflow-dags
## 🇪🇸 Descripción
En este repositorio, exploramos el uso de Airflow para la extracción de datos meteorológicos y alertas de Murcia desde una API, y su carga en una base de datos PostgreSQL. Además, automatizamos este proceso para asegurar que las tareas se ejecuten de manera programada. En caso de éxito o fallo se notificará al usuario vía email y posteriormente se analizarán los datos con Grafana. Bajo la carpeta dags/ podemos encontrar:
- **DAG_Weather_Alerts_API.py**: Extracción y carga de alertas meteorológicas, y notificación al usuario en caso de existir alertas o en caso fallido en la ejecución.
- **DAG_Weather_API.py**: Extracción y carga de datos meteorológicos, y notificación al usuario en la carga de los datos o en caso fallido en la ejecución.

## Características
- **Extracción de datos**: Conexión y extracción de datos desde una API de datos meteorológicos.
- **Tranformación de datos**: Seleccionamos rigurosamente los campos de interés.
- **Carga de datos**: Transferencia de datos seleccionados a una base de datos PostgreSQL.
- **Automatización**: Configuración del DAG para automatizar el proceso de extracción y carga.
- **Notificación**: Notificación del estado exitoso o fallo vía email.
- **Análisis de datos**: Estudio de las tendencias y patrones de los datos.

## Requisitos
- Un servicio Airflow funcional.
- Un servidor de base de datos PostgreSQL, puede optar por otras altenativas.
- Un fichero con las credenciales de la base de datos y del email.
- Un email con su respectiva app password.
- Un servicio Grafana conectado a la base de datos.

## Contribución
Las contribuciones son bienvenidas. Por favor, abre un issue para discutir cualquier cambio importante antes de realizarlo.

## 🇬🇧 Description
In this repository, we explore the use of Airflow for extracting weather data and alerts from Murcia using an API, and loading them into a PostgreSQL database. Additionally, we automate this process to ensure tasks are executed on schedule. In case of success or failure, the user will be notified via email and data will be analized using Grafana. Under the dags/ folder, you can find:
- **DAG_Weather_Alerts_API.py**: Extraction and loading of weather alerts, and notification to the user in case alerts exist or in case of execution failure.
- **DAG_Weather_API.py**: Extraction and loading of weather data, and notification to the user upon successful data loading or in case of execution failure.

## Features
- **Data Extraction**: Connection and extraction of data from a weather data API.
- **Data Transformation**: Careful selection of fields of interest.
- **Data Loading**: Transfer of selected data to a PostgreSQL database.
- **Automation**: DAG configuration to automate the extraction and loading process.
- **Notification**: Notification of successful or failed status via email.
- **Data Analysis**: Study of data trends and patterns.

## Requirements
- A functional Airflow service.
- A PostgreSQL database server; other alternatives can also be considered.
- A file containing credentials for the database and email.
- An email with its respective app password.
-A Grafana service connected to the database.

## Contribution
Contributions are welcome. Please open an issue to discuss any major changes before implementing them.
