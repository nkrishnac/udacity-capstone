from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators import PostgresOperator
from airflow.operators import CsvToRedshiftOperator
from airflow.models import Variable

default_args = {
            'owner': 'airflow',
            'depends_on_past': 'False',
            'retries': 0,
            'email_on_failure' : False,
            'email_on_retry': False,
            'start_date': datetime.now(),
            'provide_context' :  True
}

dag = DAG('us_immigration_dag',
          catchup=False,
          default_args=default_args,
          description='Load and Transform data in Redshift with Airflow'
         )

def some_task(ds, **kwargs):
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open("test.txt", "a") as myfile:
            myfile.write(current_time + '\n')


start_operator = DummyOperator(
        task_id = 'Begin_Execution',
        dag = dag
        )

create_tables = PostgresOperator(
        task_id = 'create_tables',
        dag = dag,
        postgres_conn_id = 'redshift',
        sql = 'capstone_create_tables.sql'
        )

load_dim_city = CsvToRedshiftOperator(
        task_id = 'load_dim_city',
        dag = dag,
        table = 'dim_city',
        redshift_conn_id = 'redshift',
        iam_role = Variable.get("iam_role"),
        s3_bucket = Variable.get("s3_bucket"),
        provide_context =  True,
        s3_key = 'csv/lake/city/'
        )

load_dim_airport_code = CsvToRedshiftOperator(
        task_id = 'load_dim_airport_code',
        dag = dag,
        table = 'dim_airport_codes',
        redshift_conn_id = 'redshift',
        iam_role = Variable.get("iam_role"),
        s3_bucket = Variable.get("s3_bucket"),
        provide_context = True,
        s3_key = 'csv/lake/codes/airport_code/'
        )

load_dim_us_cities_demographics = CsvToRedshiftOperator(
        task_id = 'load_dim_us_cities_demographics',
        dag = dag,
        table = 'dim_us_cities_demographics',
        redshift_conn_id = 'redshift',
        iam_role = Variable.get("iam_role"),
        s3_bucket = Variable.get("s3_bucket"),
        provide_context = True,
        s3_key = 'csv/lake/us_cities_demographics/'
        )

load_dim_us_cities_temperatures = CsvToRedshiftOperator(
        task_id = 'load_dim_us_cities_temperatures',
        dag = dag,
        table = 'dim_us_cities_temperatures',
        redshift_conn_id = 'redshift',
        iam_role = Variable.get("iam_role"),
        s3_bucket = Variable.get("s3_bucket"),
        provide_context = True,
        s3_key = 'csv/lake/us_cities_temperatures/'
        )

load_dim_us_airports_weather = CsvToRedshiftOperator(
        task_id = 'load_dim_us_airports_weather',
        dag = dag,
        table = 'dim_us_airports_weather',
        redshift_conn_id = 'redshift',
        iam_role = Variable.get("iam_role"),
        s3_bucket = Variable.get("s3_bucket"),
        provide_context = True,
        s3_key = 'csv/lake/us_airports_weather/'
        )

load_fact_immigrant_info = CsvToRedshiftOperator(
        task_id = 'load_fact_immigrant_info',
        dag = dag,
        table = 'fact_immigrant_info',
        redshift_conn_id = 'redshift',
        iam_role = Variable.get("iam_role"),
        s3_bucket = Variable.get("s3_bucket"),
        provide_context = True,
        s3_key = 'csv/lake/immigrant/'
        )

load_fact_immigration_info = CsvToRedshiftOperator(
        task_id = 'load_fact_immigration_info',
        dag = dag,
        table = 'fact_immigration_info',
        redshift_conn_id = 'redshift',
        iam_role = Variable.get("iam_role"),
        s3_bucket = Variable.get("s3_bucket"),
        provide_context = True,
        s3_key = 'csv/lake/immigration/'
        )

    
load_fact_immigration_demographics = CsvToRedshiftOperator(
        task_id = 'load_fact_immigration_demographics',
        dag = dag,
        table = 'fact_immigration_demographics',
        redshift_conn_id = 'redshift',
        iam_role = Variable.get("iam_role"),
        s3_bucket = Variable.get("s3_bucket"),
        provide_context = True,
        s3_key = 'csv/lake/immigration_demographic/'
        )

end_operator = DummyOperator(
        task_id = 'Stop_execution',
        dag = dag
        )

start_operator >> create_tables
create_tables >> [load_dim_city, load_dim_airport_code, load_dim_us_cities_demographics, load_dim_us_cities_temperatures, load_dim_us_airports_weather] >> load_fact_immigrant_info >> load_fact_immigration_info >> load_fact_immigration_demographics >> end_operator
