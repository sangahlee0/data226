from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task

from datetime import timedelta
from datetime import datetime
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import requests


'''def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_con')
    
    # Execute the query and fetch results
    cur = hook.get_conn()
    return cur.cursor()
'''
def get_hook():
    return SnowflakeHook(snowflake_conn_id='snowflake_con')

@task
def train(train_input_table, train_view, model_name):
    hook = get_hook()
    conn = hook.get_conn()
    conn.autocommit(True)
    cur = conn.cursor()

    try:
        cur.execute("CREATE SCHEMA IF NOT EXISTS ADHOC")
        cur.execute("CREATE SCHEMA IF NOT EXISTS ANALYTICS")

        create_view_sql = f"""
        CREATE OR REPLACE VIEW {train_view} AS
        SELECT
            DATE AS DS,
            AVG(TEMP_MAX) AS TEMP_MAX,
            CITY
        FROM {train_input_table}
        WHERE TEMP_MAX IS NOT NULL
        GROUP BY DS, CITY
        """
        cur.execute(create_view_sql)

        create_model_sql = f"""
        CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {model_name} (
            INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{train_view}'),
            TIMESTAMP_COLNAME => 'DS',
            TARGET_COLNAME => 'TEMP_MAX',
            SERIES_COLNAME => 'CITY',
            CONFIG_OBJECT => {{ 'ON_ERROR': 'SKIP' }}
        )
        """
        cur.execute(create_model_sql)

        print(f"Model {model_name} created successfully.")

    finally:
        cur.close()
        conn.close()


@task
def predict(model_name, forecast_table, final_table, train_input_table):
    hook = get_hook()
    conn = hook.get_conn()

    # prevent stored-procedure scoped transaction conflict
    conn.autocommit(True)

    cur = conn.cursor()
    try:
        call_sql = f"""
        CALL {model_name}!FORECAST(
            FORECASTING_PERIODS => 7,
            CONFIG_OBJECT => {{'prediction_interval': 0.95}}
        );
        """
        cur.execute(call_sql)

        # Grab the CALL query id (same session)
        cur.execute("SELECT LAST_QUERY_ID()")
        last_qid = cur.fetchone()[0]
        if not last_qid:
            raise RuntimeError("LAST_QUERY_ID() returned empty after FORECAST call.")

        # Materialize forecast results
        cur.execute(
            f"CREATE OR REPLACE TABLE {forecast_table} AS "
            f"SELECT * FROM TABLE(RESULT_SCAN('{last_qid}'));"
        )

        # Build final table
        create_final_table_sql = f"""
        CREATE OR REPLACE TABLE {final_table} AS
            SELECT CITY, DATE,
                   TEMP_MAX as actual_max,
                   TEMP_MIN,
                   TEMP_MEAN AS actual_mean,
                   PRECIPITATION,
                   NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
            FROM {train_input_table}
        UNION ALL
            SELECT REPLACE(series, '\"', '') as CITY,
                   ts as DATE,
                   NULL AS actual_max, NULL AS TEMP_MIN, NULL AS actual_mean, NULL AS PRECIPITATION,
                   forecast, lower_bound, upper_bound
            FROM {forecast_table};
        """
        cur.execute(create_final_table_sql)

        print(f"Forecast table: {forecast_table}")
        print(f"Final table: {final_table}")

    finally:
        cur.close()
        conn.close()


with DAG(
    dag_id = 'lab1_forecast',
    start_date = datetime(2026,3,1),
    catchup=False,
    tags=['ML', 'ELT'],
    schedule = '30 3 * * *'
) as dag:
    
    train_input_table = "raw.lab1etl"
    train_view = "adhoc.lab1city_weather_view"
    forecast_table = "adhoc.lab1_city_weather_forecast"
    model_name = "analytics.lab1_predict_city_weather"
    final_table = "analytics.lab1_city_weather_final"
    #cur = return_snowflake_conn()

    #rain_task = train(cur, train_input_table, train_view, model_name)
    #predict_task = predict(cur, model_name, forecast_table, final_table, train_input_table)
    
    train_task = train(train_input_table, train_view, model_name)
    predict_task = predict(model_name, forecast_table, final_table, train_input_table)
    train_task >> predict_task