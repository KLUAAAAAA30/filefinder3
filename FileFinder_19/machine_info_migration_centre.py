import pandas as pd
import mysql.connector
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables from .env file (best practice for credentials)
load_dotenv()

# MySQL database configuration (fetched from environment for security)
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = int(os.getenv("MYSQL_PORT"))  # Ensure port is an integer
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_USER = os.getenv("MYSQL_USERNAME")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")

# Read Excel file, filtering for 'Assessment' rows
df = pd.read_excel('pc_data_info.xlsx')
df_assessment = df[df['groupType'] == 'Assessment']

# Establish MySQL database connection
connection = mysql.connector.connect(
    host=MYSQL_HOST,
    port=MYSQL_PORT,
    database=MYSQL_DATABASE,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD
)
cursor = connection.cursor()

# SQL query (template for later use)
insert_query = """
INSERT INTO machine_info_migration_centre(
    name, create_time, ip, model, os_name, total_processor, total_memory, free_memory
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

# Process data and insert/update into database
for _, row in df_assessment.iterrows():
    name = row['name']
    create_time = datetime.strptime(row['createDate'], '%d-%b-%Y').strftime('%Y-%m-%d')
    ip = row['collectedIpAddress']
    model = row['model']
    os_name = row['osName']
    total_processor = row['processorCount']
    total_memory = row['memoryInMb']
    free_memory = row['driveTotalFreeInGb']

    values = (name, create_time, ip, model, os_name, total_processor, total_memory, free_memory)
    cursor.execute(insert_query, values)

# Commit changes and close connection (always close resources!)
connection.commit()
cursor.close()
connection.close()

print("Data migration completed successfully!")
