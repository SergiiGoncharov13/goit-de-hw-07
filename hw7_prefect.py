from prefect import flow, task
import pymysql

import random
import time
import pymysql

from configs import DB_CONFIG

# # MySQL connection details
# DB_CONFIG = {
#     "host": "host",
#     "user": "username",
#     "password": "password",
#     "database": "database_name",
# }

# Task: Create MySQL Schema
@task
def create_schema():
    conn = pymysql.connect(**DB_CONFIG)
    with conn.cursor() as cursor:
        cursor.execute("CREATE DATABASE IF NOT EXISTS sergii_g;")
    conn.commit()
    conn.close()

# Task: Create Table
@task
def create_table():
    conn = pymysql.connect(**DB_CONFIG)
    with conn.cursor() as cursor:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS sergii_g.medals_statistics (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(255),
            count INT,
            created_at DATETIME DEFAULT NOW()
        );
        """)
    conn.commit()
    conn.close()

# Task: Choose a medal type
@task
def choose_medal_type() -> str:
    return random.choice(['Bronze', 'Silver', 'Gold'])

# Task: Insert medal count into MySQL
@task
def insert_medal_count(medal: str):
    conn = pymysql.connect(**DB_CONFIG)
    with conn.cursor() as cursor:
        query = f"""
        INSERT INTO sergii_g.medals_statistics (medal_type, count, created_at)
        SELECT '{medal}', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = '{medal}';
        """
        cursor.execute(query)
    conn.commit()
    conn.close()

# Task: Introduce Delay
@task
def delayed_execution():
    time.sleep(20)

# Task: Check if records exist in the last 30 seconds
@task
def check_records():
    conn = pymysql.connect(**DB_CONFIG)
    with conn.cursor() as cursor:
        cursor.execute("""
        SELECT 1 FROM sergii_g.medals_statistics
        WHERE TIMESTAMPDIFF(SECOND, created_at, NOW()) <= 30
        ORDER BY created_at DESC
        LIMIT 1;
        """)
        result = cursor.fetchone()
    conn.close()
    if not result:
        raise ValueError("No new records found in the last 30 seconds. Task failed!")
    return True 

# Prefect Flow
@flow(name="sergii_hw7_flow")
def main_flow():
    create_schema()
    create_table()
    
    medal = choose_medal_type()
    insert_medal_count(medal)
    
    delayed_execution()
    
    records_exist = check_records()
    if records_exist:
        print("New records detected in the last 30 seconds.")
    else:
        print("No recent records found.")


# Run the flow
if __name__ == "__main__":
    main_flow()