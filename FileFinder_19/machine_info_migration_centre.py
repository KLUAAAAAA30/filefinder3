import os
from datetime import datetime

import mysql.connector
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = int(os.getenv("MYSQL_PORT"))
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_USER = os.getenv("MYSQL_USERNAME")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")

df = pd.read_excel('pc_data_info.xlsx')
df_assessment = df[df['groupType'] == 'Assessment']

# Error Handling for Database Connection
try:
    connection = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        database=MYSQL_DATABASE,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD
    )

    cursor = connection.cursor()

    insert_query = """
    INSERT INTO machine_info_migration_centre (
        name, create_time, ip, model, os_name, 
        total_processor, total_memory, free_memory
    ) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        create_time = VALUES(create_time),
        ip = VALUES(ip), 
        model = VALUES(model),
        os_name = VALUES(os_name), 
        total_processor = VALUES(total_processor),
        total_memory = VALUES(total_memory), 
        free_memory = VALUES(free_memory)
    """

    for _, row in df_assessment.iterrows():
        # Data Extraction and Formatting
        name = row['name']
        create_time = row['createDate'].strftime('%Y-%m-%d')  # Assuming createDate is already datetime
        ip, model, os_name = row['collectedIpAddress'], row['model'], row['osName']
        total_processor, total_memory, free_memory = (
            row['processorCount'], row['memoryInMb'], row['driveTotalFreeInGb']
        )

        # Execute Query
        values = (name, create_time, ip, model, os_name, 
                  total_processor, total_memory, free_memory)
        cursor.execute(insert_query, values)

    connection.commit()
    print("Data migration completed successfully!")

except mysql.connector.Error as err:
    print(f"Something went wrong: {err}") 
    
finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed")
