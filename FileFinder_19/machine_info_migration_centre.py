import pandas as pd
import mysql.connector
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()

# Replace with your MySQL database configuration
host = os.getenv("MYSQL_HOST")  # MySQL server address
port = os.getenv("MYSQL_PORT")  # MySQL server port
database_name = os.getenv("MYSQL_DATABASE")
username = os.getenv("MYSQL_USERNAME")
password = os.getenv("MYSQL_PASSWORD")  # Ensure password is loaded from the environment

# Read the Excel file
df = pd.read_excel("FileFinder_19/pc_data_info.xlsx")

# Filter the DataFrame based on the "groupType" column
df_assessment = df[df["groupType"] == "Assessment"]

# Establish a MySQL database connection
try:
    connection = mysql.connector.connect(
        host=host, port=port, database=database_name, user=username, password=password
    )
    cursor = connection.cursor()

    # If necessary, create the table (uncomment if needed)
    create_table_sql = '''
    CREATE TABLE IF NOT EXISTS machine_info_migration_centre (
        pc_data_pk INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255),
        create_time DATE,
        ip VARCHAR(15),
        model VARCHAR(255),
        os_name VARCHAR(255),
        total_processor INT,
        total_memory INT,
        free_memory FLOAT,
        UNIQUE INDEX idx_name (name),
        INDEX idx_pc_data_pk (pc_data_pk)
    );
    '''
    cursor.execute(create_table_sql)

    # Loop through the rows of the filtered DataFrame and insert data into the database
    for index, row in df_assessment.iterrows():
        name = row["name"]
        create_time_str = row["createDate"]
        create_time = datetime.strptime(create_time_str, "%d-%b-%Y").strftime("%Y-%m-%d")
        ip = row["collectedIpAddress"]
        model = row["model"]
        os_name = row["osName"]
        total_processor = row["processorCount"]
        total_memory = row["memoryInMb"]
        free_memory = row["driveTotalFreeInGb"]

        # Insert data into your MySQL database
        query = """
            INSERT INTO machine_info_migration_centre (name, create_time, ip, model, os_name, total_processor, total_memory, free_memory)
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

        values = (
            name,
            create_time,
            ip,
            model,
            os_name,
            total_processor,
            total_memory,
            free_memory,
        )

        cursor.execute(query, values)

    # Commit the changes
    connection.commit()

except mysql.connector.Error as err:
    print(f"Error: {err}")
    if connection:
        connection.rollback()
finally:
    # Ensure the cursor and connection are properly closed
    if cursor:
        cursor.close()
    if connection:
        connection.close()
