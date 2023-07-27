from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import logging
import pandas as pd


def extract(**kwargs):
    src_db = PostgresHook(postgres_conn_id="stackoverflow_source_db")
    src_conn = src_db.get_conn()

    last_loaded_id = Variable.get('last_loaded_id', default_var=0)
    logging.info(f"Extract function: last_loaded_id from Variable is {last_loaded_id}")
    last_loaded_id = int(last_loaded_id)

    df = pd.read_sql(
        f'''
        SELECT id, title, body, owner_user_id, creation_date
        FROM posts
        WHERE body IS NOT NULL
        AND owner_user_id IS NOT NULL
        AND id > {last_loaded_id}
        ORDER BY id ASC
        LIMIT 1000;
        ''',
        src_conn
    )

    kwargs['ti'].xcom_push(key='dataset', value=df.to_json())
    logging.info(f"Extract function: pushing dataset to Xcom")


def load(**kwargs):
    target_db = PostgresHook(postgres_conn_id="analytical_db_rds")

    create_posts_table = '''
    CREATE TABLE IF NOT EXISTS posts (
    id INT PRIMARY KEY,
    title VARCHAR,
    body VARCHAR,
    owner_user_id INT,
    creation_date TIMESTAMP
    );
    '''

    load_post_data = '''
    INSERT INTO posts (id, title, body, owner_user_id, creation_date)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (id) DO NOTHING;
    '''

    df = pd.read_json(kwargs['ti'].xcom_pull(key='dataset'))
    logging.info(f"Load function: pulled dataset from Xcom. DataFrame shape is {df.shape}")

    df = df[['id', 'title', 'body', 'owner_user_id', 'creation_date']]
    df['creation_date'] = pd.to_datetime(df['creation_date'], unit='ms')

    with target_db.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(create_posts_table)
                for row in df.itertuples():
                    data = row[1:]
                    logging.info(f'Loading data: {data}')
                    cursor.execute(load_post_data, data)
                conn.commit()

    max_id = int(df['id'].max() or 0)
    Variable.set('last_loaded_id', max_id)
    logging.info(f"Load function: last_loaded_id pushed to Variable is {max_id}")


dag = DAG(
    "stackoverflow_dag",
    description="ETL pipeline for Stack Overflow data held on Amazon RDS.",
    schedule_interval=timedelta(minutes=15),
    start_date=datetime(2023, 7, 17),
    catchup=False,
    max_active_runs=1,
    tags=["EC2"]
)

t1 = PythonOperator(
    task_id="extract_task",
    python_callable=extract,
    provide_context=True,
    dag=dag
)

t2 = PythonOperator(
    task_id="load_task",
    python_callable=load,
    provide_context=True,
    dag=dag
)

t1 >> t2