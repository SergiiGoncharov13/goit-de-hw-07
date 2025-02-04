from airflow import DAG
from airflow.sensors.sql import SqlSensor
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

import random
import time
from datetime import datetime, timedelta

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 4, 0, 0),
}

# MySQL connection
connection_name = "goit_mysql_db_sergii_g"

# Choose medal type dynamically
def chose_medal_type():
    medal = random.choice(['Bronze', 'Silver', 'Gold'])
    return f'calc_{medal.lower()}' 

# Simulate delay
def delayed_execution():
    time.sleep(20)

# Create DAG
with DAG(
        'sergii_hw7',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
        tags=["sergii_g"]
) as dag:

    # Create schema
    create_schema = MySqlOperator(
        task_id='create_schema',
        mysql_conn_id=connection_name,
        sql="CREATE DATABASE IF NOT EXISTS sergii_g;"
    )

    # Create table
    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id=connection_name,
        sql="""
        CREATE TABLE IF NOT EXISTS sergii_g.medals_statistics (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(255),
            count INT,
            created_at DATETIME
        );
        """
    )

    # Branch to choose medal type
    choose_medal = BranchPythonOperator(
        task_id='choose_medal',
        python_callable=chose_medal_type
    )

    # Medal count calculations
    calc_bronze = MySqlOperator(
        task_id='calc_bronze',
        mysql_conn_id=connection_name,
        sql="""
        INSERT INTO sergii_g.medals_statistics (medal_type, count, created_at)
        SELECT 'Bronze', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Bronze';
        """
    )

    calc_silver = MySqlOperator(
        task_id='calc_silver',
        mysql_conn_id=connection_name,
        sql="""
        INSERT INTO sergii_g.medals_statistics (medal_type, count, created_at)
        SELECT 'Silver', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Silver';
        """
    )

    calc_gold = MySqlOperator(
        task_id='calc_gold',
        mysql_conn_id=connection_name,
        sql="""
        INSERT INTO sergii_g.medals_statistics (medal_type, count, created_at)
        SELECT 'Gold', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Gold';
        """
    )

    # Introduce delay
    generate_delay = PythonOperator(
        task_id='generate_delay',
        python_callable=delayed_execution,
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

    # Check records in the database
    check_records = SqlSensor(
        task_id='check_records',
        conn_id=connection_name,
        sql="""
        SELECT 1 FROM sergii_g.medals_statistics
        WHERE TIMESTAMPDIFF(SECOND, created_at, NOW()) <= 30
        ORDER BY created_at DESC
        LIMIT 1;
        """,
        mode='poke',
        poke_interval=5,
        timeout=30,
    )

    # Define task dependencies
    create_schema >> create_table >> choose_medal
    choose_medal >> [calc_bronze, calc_silver, calc_gold] >> generate_delay >> check_records
