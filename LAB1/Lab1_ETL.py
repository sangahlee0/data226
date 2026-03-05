# In Cloud Composer, add apache-airflow-providers-snowflake to PYPI Packages
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests



def return_snowflake_conn(con_id):
    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id=con_id)
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()


@task
def extract(latitude, longitude):
    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "past_days": 60,
        "forecast_days": 0,  # only past weather
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "temperature_2m_mean",
            "precipitation_sum",
            "weather_code"
        ],
        "timezone": "America/Los_Angeles"
    }

    response = requests.get(url, params=params)
    if response.status_code != 200:
        raise RuntimeError(f"Error: {response.status_code}")
    return response.json()


@task
def transform(raw_data, latitude, longitude, city):
    if "daily" not in raw_data:
        raise ValueError("Missing 'daily' in API response")

    data = raw_data["daily"]
    ## Build list of record dicts
    records = []
    for i in range(len(data["time"])):
        records.append({
            "latitude": latitude,
            "longitude": longitude,
            "date": data["time"][i],
            "temp_max": data["temperature_2m_max"][i],
            "temp_min": data["temperature_2m_min"][i],
            "temp_mean": data["temperature_2m_mean"][i],
            "precipitation": data["precipitation_sum"][i],
            "weather_code": data["weather_code"][i],
            "city": city
        })

    return records

@task
#def load(con, target_table, records):
def load(target_table, records):
    cur = return_snowflake_conn("snowflake_con")
    try:
        cur.execute("BEGIN;")

        cur.execute(f"""CREATE TABLE IF NOT EXISTS {target_table} (
            latitude FLOAT,
            longitude FLOAT,
            date DATE,
            temp_max FLOAT,
            temp_min FLOAT,
            temp_mean FLOAT,
            precipitation FLOAT,
            weather_code VARCHAR(3),
            city VARCHAR(100),
            PRIMARY KEY (latitude, longitude, date, city)
            );
        """)
        cur.execute(f"""DELETE FROM {target_table}""")

        insert_sql = f"""INSERT INTO {target_table} (latitude, longitude, date, temp_max, temp_min, temp_mean, precipitation, weather_code, city) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);"""
                
        data = [
            (   r['latitude'],
                r['longitude'],
                r['date'],
                r['temp_max'],
                r['temp_min'],
                r['temp_mean'],
                r['precipitation'],
                str(r['weather_code']),
                r['city']
            )
            for r in records
        ]
        cur.executemany(insert_sql, data)

        cur.execute("COMMIT;")
        print(f"Loaded {len(records)} records into {target_table}")

    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise e

with DAG(
    dag_id = 'Lab1_ETL',
    start_date = datetime(2026,3,1),
    catchup=False,
    tags=['ETL'],
    schedule = '30 3 * * *'
) as dag:
    #LATITUDE = Variable.get("LATITUDE")
    #LONGITUDE = Variable.get("LONGITUDE")
    #CITY  = "Fremont"

    # Multiple locations
    locations = Variable.get("MULTI_LOCATIONS", deserialize_json=True)
    target_table = "raw.lab1etl"

    for l in locations:
        lat = l["lat"]
        lon = l["lon"]
        city = l["city"]
        
        # Remove/replace spaces in city name
        city_id = city.replace(" ", "_")

        raw_data = extract.override(task_id=f"extract_{city_id}")(lat, lon)
        data = transform.override(task_id=f"transform_{city_id}")(raw_data, lat, lon, city)
        #load(cur, target_table, data)
        load.override(task_id=f"load_{city_id}")(target_table, data)