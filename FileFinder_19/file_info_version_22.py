import sys
import os
import pandas as pd
import psutil
import mysql.connector
import csv
from datetime import datetime
import logging
from dotenv import load_dotenv
import platform
import time
import socket
from questionary import select
from rich import print
import keyboard
import subprocess
import win32net
from loguru import logger

load_dotenv()

# Variables that can be fetched from database or .env
# Define the list of file extensions to search for
d_file_details_file_extensions = "test"
# Define word patterns to identify sensitive data in file names
sensitive_patterns = "test"
is_sensitive_file_extensions = "test"
# Enables the count of files with extensions. By default, the total files are counted.
enable_file_ext_count_in_scan = "test"
# Enable scan of excel files. Enable read of the excel files
enable_excel_file_data_scan = "test"
enable_excel_file_data_scan_min_row = 3
n_days = 0

# Remove default logger
logger.remove()


def create_db_connection(host, port, database_name, username, password):
    try:
        # Define your MySQL database connection details
        connection = mysql.connector.connect(
            host=host,
            port=port,
            database=database_name,
            user=username,
            password=password
        )

        if connection.is_connected():
            print("[bright_green]Database connection is completed [/bright_green]")
            logger.success("Database connection is completed ")
            return connection
        else:
            logger.error("Error getting Database connection", exc_info=True)

    except Exception as e:
        print(f"Error getting Database connection: {str(e)}", exc_info=True)
        logger.error(
            f"Error getting Database connection: {str(e)}", exc_info=True)
        return None


def retrieve_env_values(enable_env_from_db, connection):
    if enable_env_from_db == 'true':
        get_values_from_db(connection)
    else:
        get_values_from_env()


def get_values_from_db(connection):
    cursor = connection.cursor()
    query = "SELECT env_key, env_value FROM env_info"
    cursor.execute(query)

    global config_values
    config_values = {env_key: env_value for env_key, env_value in cursor}
    global d_file_details_file_extensions
    d_file_details_file_extensions = config_values.get("D_FILE_DETAILS_FILE_EXTENSIONS")
    global sensitive_patterns
    sensitive_patterns = config_values.get("FILE_PATH_SCAN_SENSITIVE_PATTERNS")
    global is_sensitive_file_extensions
    is_sensitive_file_extensions = config_values.get("IS_SENSITIVE_FILE_EXTENSIONS")
    global enable_file_ext_count_in_scan
    enable_file_ext_count_in_scan = config_values.get("ENABLE_FILE_EXT_COUNT_IN_SCAN")
    global enable_excel_file_data_scan
    enable_excel_file_data_scan = config_values.get("ENABLE_EXCEL_FILE_DATA_SCAN")
    global enable_excel_file_data_scan_min_row
    enable_excel_file_data_scan_min_row = config_values.get("ENABLE_EXCEL_FILE_DATA_SCAN_MIN_ROW")
    global n_days
    n_days = config_values.get("N_DAYS")

    cursor.close()


def get_values_from_env():
    # Variables that can be fetched from .env
    # Define the list of file extensions to search for
    global d_file_details_file_extensions
    d_file_details_file_extensions = os.getenv(
        "D_FILE_DETAILS_FILE_EXTENSIONS"
    ).split(",")  # Add more extensions as needed
    # Define word patterns to identify sensitive data in file names
    global sensitive_patterns
    sensitive_patterns = os.getenv("FILE_PATH_SCAN_SENSITIVE_PATTERNS").split(",")
    global is_sensitive_file_extensions
    is_sensitive_file_extensions = os.getenv("IS_SENSITIVE_FILE_EXTENSIONS").split(",")
    # enables the count of files with extensions. By default, the total files are counted.
    global enable_file_ext_count_in_scan
    enable_file_ext_count_in_scan = os.getenv("ENABLE_FILE_EXT_COUNT_IN_SCAN").lower()
    # Enable scan of excel files. Enable read of the excel files
    global enable_excel_file_data_scan
    enable_excel_file_data_scan = os.getenv("ENABLE_EXCEL_FILE_DATA_SCAN").lower()
    global enable_excel_file_data_scan_min_row
    enable_excel_file_data_scan_min_row = os.getenv("ENABLE_EXCEL_FILE_DATA_SCAN_MIN_ROW")
    global n_days
    n_days = int(os.getenv("N_DAYS"))


def get_ip_address():
    """
    This is a function that checks for the modified days of a file.
    Return modified or not modified - true or false

    Args:
        file_path (int): file path
        n_days (int): days modified from the env file

    Returns:
        boolean: true or false
    """
    try:
        # Check the operating system
        system_name = platform.system()

        if system_name == 'Linux':
            # Run the 'hostname -I' command and capture the output
            result = subprocess.run(['hostname', '-I'], capture_output=True, text=True)

            # Extract the IP address from the command output
            ip_addresses = result.stdout.strip().split()

            # Return the first IP address in the list
            if ip_addresses:
                return ip_addresses[0]
            else:
                return None
        elif system_name == 'Windows':
            # For Windows, use a different method to get the local IP address
            return socket.gethostbyname(socket.gethostname())
        else:
            print(f"Unsupported operating system: {system_name}")
            logger.error(f"Unsupported operating system: {system_name}")
            return None
    except Exception as e:
        print(f"Error getting IP address: {str(e)}")
        logger.error(f"Error getting IP address: {str(e)}")
        return None


def get_removable_drives():
    """
    This is a function that checks for the modified days of a file.
    Return modified or not modified - true or false

    Args:
        file_path (int): file path
        n_days (int): days modified from the env file

    Returns:
        boolean: true or false
    """
    removable_drives = []
    # Refactor the below piece of code
    try:
        for partition in psutil.disk_partitions():
            try:
                if 'removable' in partition.opts or 'cdrom' in partition.opts:
                    removable_drives.append(partition.device)
            except Exception as inner_exception:
                print(
                    f"An error occurred while processing partition {partition.device}:"
                    f"{inner_exception}"
                )
    except Exception as outer_exception:
        print(f"Error get_removable_drives: {outer_exception}")

    return removable_drives


def get_drives():
    all_drives = []
    try:
        partitions = psutil.disk_partitions(all=True)  # Include all drives
        for partition in partitions:
            if partition.device:
                all_drives.append(partition.device)
        return all_drives
    except Exception as e:
        # Log the error to the log file
        logger.error(f"Error retrieving drive information: {str(e)}", exc_info=True)
        return None


# Define a custom exception class for file-related errors
class FileError(Exception):
    pass


def is_recently_accessed_or_modified(file_path, n_days):
    """
    This is a function that checks for the modified days of a file.
    Return modified or not modified - true or false

    Args:
        file_path (int): file path
        n_days (int): days modified from the env file

    Returns:
        boolean: true or false
    """
    try:
        now = datetime.now()
        file_info = os.stat(file_path)
        file_mtime = datetime.fromtimestamp(file_info.st_mtime)
        file_atime = datetime.fromtimestamp(file_info.st_atime)
        delta_mtime = now - file_mtime
        delta_atime = now - file_atime
        return delta_mtime.days <= n_days or delta_atime.days <= n_days
    except Exception as e:
        # Log the error to the log file
        logger.error(f"Error checking file modification/access time: {str(e)}", exc_info=True)
        return False



def is_sensitive_file(file_path, sensitive_patterns):
    try:
        # Check if the file extension is in the allowed list
        allowed_extensions = is_sensitive_file_extensions
        if not any(file_path.lower().endswith(ext) for ext in allowed_extensions):
            return False

        file_name = os.path.basename(file_path).lower()

        # Check if any sensitive pattern is present in the file name
        for pattern in sensitive_patterns:
            if pattern in file_name:
                return True

        # If you want to check sensitive patterns in the file content, uncomment the following code:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as file:
            file_content = file.read().lower()
            for pattern in sensitive_patterns:
                if pattern in file_content:
                    return True

    except Exception as e:
        # Log the error to the log file
        logger.error(f"Error checking file for sensitive data: {str(e)}", exc_info=True)

    return False


def search_files(root_dir, extensions, n_days, sensitive_patterns):
    found_assets = []
    try:
        for foldername, subfolders, filenames in os.walk(root_dir):
            for filename in filenames:
                if d_file_details_file_extensions == "all":
                    file_path = os.path.join(foldername, filename)
                    # File modified date check. If "File modified date" more than 0 then get only modified files
                    if n_days > 0:
                        if is_recently_accessed_or_modified(file_path, n_days):
                            found_assets.append(file_path)
                    # File modified date check. If "File modified date" is 0 then get all the files
                    else:
                        found_assets.append(file_path)
                else:
                    if any(filename.lower().endswith(ext) for ext in extensions):
                        file_path = os.path.join(foldername, filename)
                        # File modified date check. If "File modified date" more than 0 then get only modified files
                        if n_days > 0:
                            if is_recently_accessed_or_modified(file_path, n_days):
                                found_assets.append(file_path)
                        # File modified date check. If "File modified date" is 0 then get all the files
                        else:
                            found_assets.append(file_path)
    except Exception as e:
        # Log the error to the log file
        logger.error(f"Error scanning files: {str(e)}", exc_info=True)
    return found_assets


def upsert_to_database(file_path, connection, employee_username, start_time):
    try:
        if platform.system() == "Windows":
            import win32api
            import win32con
            import win32security

            # get_owner_name = get_owner_name_windows
            sd = win32security.GetFileSecurity(file_path, win32security.OWNER_SECURITY_INFORMATION)
            owner_sid = sd.GetSecurityDescriptorOwner()
            name, domain, _ = win32security.LookupAccountSid(None, owner_sid)
            owner_name = f"{domain}\\{name}"
        elif platform.system() == "Linux":
            import pwd

            stat_info = os.stat(file_path)
            owner_uid = stat_info.st_uid
            owner_name = pwd.getpwuid(owner_uid).pw_name

        hostname = socket.gethostname()
        ipaddrs = socket.gethostbyname(hostname)
        cursor = connection.cursor()
        file_size = os.path.getsize(file_path)
        file_name = os.path.basename(file_path)
        file_extension = os.path.splitext(file_name)[1]
        modification_time = datetime.fromtimestamp(os.path.getmtime(file_path))
        access_time = datetime.fromtimestamp(os.path.getatime(file_path))
        creation_time = datetime.fromtimestamp(os.path.getctime(file_path))
        file_is_sensitive_data = is_sensitive_file(file_path, sensitive_patterns)

        truncated_file_path = file_path[:759]

        # Perform an upsert based on file_path
        cursor.execute('''
            INSERT INTO d_file_details (
                f_machine_files_summary_count_fk, 
                ip_address, hostname, file_path, file_size_bytes, file_name, file_extension, file_owner, 
                file_creation_time, file_modification_time, file_last_access_time, file_is_sensitive_data, 
                row_creation_date_time, row_created_by, row_modification_date_time, row_modification_by
            )
            VALUES (
                (SELECT f_machine_files_summary_count_pk FROM f_machine_files_summary_count WHERE hostname = %s), 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, FROM_UNIXTIME(%s), %s, FROM_UNIXTIME(%s), %s
            )
            ON DUPLICATE KEY UPDATE
                file_size_bytes = %s, row_modification_date_time = FROM_UNIXTIME(%s), row_modification_by = %s;
        ''', (
            hostname, ipaddrs, hostname, truncated_file_path, file_size, file_name, file_extension, owner_name, 
            creation_time, modification_time, access_time, file_is_sensitive_data, start_time, 
            employee_username, start_time, employee_username, file_size, start_time, employee_username
        ))

        connection.commit()
    except Exception as e:
        # Log the error to the log file
        logger.error(f"Error upsert_to_database: {str(e)}", exc_info=True)


def create_xls_file_sheet_table(connection, xls_files, employee_username, start_time):
    try:
        cursor = connection.cursor()
        for xls_file in xls_files:
            xls_data = pd.read_excel(xls_file, sheet_name=None)  # Read all sheets

            for sheet_name, sheet in xls_data.items():
                num_rows, num_cols = sheet.shape

                cursor.execute('''
                    INSERT INTO xls_file_sheet (
                        d_file_details_fk, sheet_name, total_cols, total_rows, row_creation_date_time, 
                        row_created_by, row_modification_date_time, row_modification_by
                    )
                    VALUES (
                        (SELECT d_file_details_pk FROM d_file_details WHERE file_path = %s),
                        %s, %s, %s, FROM_UNIXTIME(%s), %s, FROM_UNIXTIME(%s), %s
                    )
                    ON DUPLICATE KEY UPDATE
                        total_cols = VALUES(total_cols),
                        total_rows = VALUES(total_rows),
                        row_modification_date_time = FROM_UNIXTIME(%s), row_modification_by = %s;
                ''', (
                    xls_file, sheet_name, num_cols, num_rows, start_time, employee_username, start_time, 
                    employee_username, start_time, employee_username
                ))
                connection.commit()
        print("[bright_green]Data inserted into xls_file_sheet table.[/bright_green]")
        logger.success("Data inserted into xls_file_sheet table.")
    except Exception as e:
        logger.error(f"Error create_xls_file_sheet_table: {str(e)}", exc_info=True)


# Function to create a new table for .xls file rows
def create_xls_file_sheet_row_table(connection, xls_files, employee_username, start_time):
    try:
        cursor = connection.cursor()
        for xls_file in xls_files:
            xls_data = pd.read_excel(xls_file, sheet_name=None, header=None)  # Read all sheets

            for sheet_name, sheet in xls_data.items():
                num_rows, num_cols = sheet.shape

                # Insert the first 10 columns of data into the table, or all if there are fewer than 10 columns
                for row_idx in range(min(int(enable_excel_file_data_scan_min_row), num_rows)):  # Read up to the first few rows
                    is_row = "no" if row_idx == 0 else "yes"  # First row is a heading, the rest are data
                    col_data = sheet.iloc[row_idx, :10].tolist()  # Take the first 10 columns
                    col_data.extend(["NULL"] * (10 - len(col_data)))  # Fill the remaining columns with "NULL"
                    col_data = [str(data)[:255] for data in col_data]  # Truncate data if necessary
                    # Check for truncation if there are more than 10 columns
                    is_truncate = "yes" if num_cols > 10 else "no"

                    cursor.execute(f'''
                        INSERT INTO xls_file_sheet_row (
                            xls_file_sheet_fk, sheet_name, col_no, row_no, is_row,
                            col_data_1, col_data_2, col_data_3, col_data_4, col_data_5,
                            col_data_6, col_data_7, col_data_8, col_data_9, col_data_10, 
                            is_truncate, row_creation_date_time, row_created_by, 
                            row_modification_date_time, row_modification_by
                        )
                        VALUES (
                            (
                                SELECT xls_file_sheet_pk 
                                FROM xls_file_sheet 
                                WHERE sheet_name = %s AND d_file_details_fk = (
                                    SELECT d_file_details_pk
                                    FROM d_file_details 
                                    WHERE file_path = %s LIMIT 1
                                ) LIMIT 1
                            ),
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                            FROM_UNIXTIME(%s), %s, FROM_UNIXTIME(%s), %s
                        )
                        ON DUPLICATE KEY UPDATE
                            xls_file_sheet_fk = VALUES(xls_file_sheet_fk),
                            sheet_name = VALUES(sheet_name),
                            col_no = VALUES(col_no),
                            row_no = VALUES(row_no),
                            col_data_1 = VALUES(col_data_1),
                            col_data_2 = VALUES(col_data_2),
                            col_data_3 = VALUES(col_data_3),
                            col_data_4 = VALUES(col_data_4),
                            col_data_5 = VALUES(col_data_5),
                            col_data_6 = VALUES(col_data_6),
                            col_data_7 = VALUES(col_data_7),
                            col_data_8 = VALUES(col_data_8),
                            col_data_9 = VALUES(col_data_9),
                            col_data_10 = VALUES(col_data_10),
                            row_modification_date_time = FROM_UNIXTIME(%s), row_modification_by = %s;
                    ''', (
                        sheet_name, xls_file, sheet_name, num_cols, row_idx + 1, is_row, *col_data, is_truncate, 
                        start_time, employee_username, start_time, employee_username, start_time, employee_username
                    ))
                    connection.commit()
        print("[bright_green]Data inserted into xls_file_sheet_row table.[/bright_green]")
        logger.success("Data inserted into xls_file_sheet_row table.")
    except Exception as e:
        logger.error(f"create_xls_file_sheet_row_table: {str(e)}", exc_info=True)


# Function for audit table
def create_audit_table(connection, employee_username, ip, start_time, end_time, elapsed_time, scan):
    if scan == "File Count":
        scan = 'Count'
    activity = 'Completed'
    try:
        cursor = connection.cursor()

        cursor.execute('''
            INSERT INTO audit_info (
                f_machine_files_summary_count_fk, pc_ip_address, employee_username, start_time, 
                end_time, duration, filefinder_activity, activity_status, row_creation_date_time, 
                row_created_by, row_modification_date_time, row_modification_by
            )
            VALUES (
                (SELECT f_machine_files_summary_count_pk FROM f_machine_files_summary_count WHERE ip_address = %s),
                %s, %s, FROM_UNIXTIME(%s), FROM_UNIXTIME(%s), %s, %s, %s, FROM_UNIXTIME(%s), %s, FROM_UNIXTIME(%s), %s
            );
        ''', (
            ip, ip, employee_username, start_time, end_time, end_time - start_time, scan, activity, 
            start_time, employee_username, start_time, employee_username
        ))
        connection.commit()
        print("[bright_green]Data inserted into audit_info table.[/bright_green]")
        logger.success("Data inserted into audit_info table.")
    except Exception as e:
        logger.error(f"Error create_audit_table: {str(e)}", exc_info=True)


def get_shared_drives():
    shared_drives = []
    resume = 0
    while True:
        drives_data, total, resume = win32net.NetShareEnum(None, 2, resume)
        shared_drives.extend(drives_data)
        if resume == 0:
            break
    return shared_drives




def insert_f_machine_files_summary_count(connection, ipaddress, hostname, ops, os_name, os_version, system_info, employee_username, start_time):
    try:
        if ops == "Windows":
            drives = get_drives()
            removeable_drives = get_removable_drives()
            drive_names = ""

            for i, drive in enumerate(drives, start=1):
                if drive in removeable_drives:
                    drive_names += f"{i}. {drive} (removable),"
                    
                else:
                    drive_names += f"{i}. {drive},"
                
            shared_drives =""    
            shared_drives = get_shared_drives()
            total_files = 0
            total_n_xls = 0
            total_n_xlsx = 0
            total_n_doc = 0
            total_n_docx= 0
            total_n_pdf = 0
            total_n_zip = 0
            total_n_sql = 0
            total_n_bak = 0 
            total_n_csv = 0
            total_n_txt = 0
            total_n_jpg = 0
            total_n_psd = 0
            total_n_mp4 =0
            total_n_png = 0
            total_n_dll = 0
            total_n_exe = 0
            total_n_tif = 0
            total_n_avi = 0
            total_n_pst = 0
            total_n_log = 0
            for drive in drives:
                #total_n_files += count_all_files(drive)
                #Above to be deleted
                #Arun: 30 Dec 2023: Below to be uncommented for production. 
                total_files += count_all_files(drive) 
                #Do the extension count only if enable_file_ext_count_in_scan == "true"
                if enable_file_ext_count_in_scan.lower() == "true"  :  
                    total_n_xls += count_files_with_extension(drive, ".xls")
                    #total_n_xlsx += count_files_with_extension(drive, ".xlsx")
                    #total_n_doc += count_files_with_extension(drive, ".doc")
                    #total_n_docx+= count_files_with_extension(drive, ".docx")
                    #total_n_pdf += count_files_with_extension(drive, ".pdf")
                    #total_n_zip += count_files_with_extension(drive, ".zip")
                    #total_n_sql += count_files_with_extension(drive, ".sql")
                    #total_n_bak += count_files_with_extension(drive, ".bak")
                    #total_n_csv += count_files_with_extension(drive, ".csv") 
                    #total_n_txt += count_files_with_extension(drive, ".txt") 
                    #total_n_jpg += count_files_with_extension(drive, ".jpg") 
                    #total_n_psd += count_files_with_extension(drive, ".psd")
                    #total_n_mp4 += count_files_with_extension(drive, ".mp4") 
                    #total_n_png += count_files_with_extension(drive, ".png") 
                    #total_n_dll += count_files_with_extension(drive, ".dll")
                    #total_n_exe += count_files_with_extension(drive, ".exe") 
                    #total_n_tif += count_files_with_extension(drive, ".tif") 
                    #total_n_avi += count_files_with_extension(drive, ".avi") 
                    #total_n_pst += count_files_with_extension(drive, ".pst")
                    #total_n_log += count_files_with_extension(drive, ".log")


        elif ops == "Linux":
            # For Linux, set number_of_drives and name_of_drives to NULL
            drives = None
            drive_names = None
            total_files = count_all_files("/")
            #Do the extension count only if enable_file_ext_count_in_scan == "true"
            if enable_file_ext_count_in_scan.lower() == "true"  :  
                total_n_xls = count_files_with_extension("/", ".xls")
                total_n_xlsx = count_files_with_extension("/", ".xlsx")
                total_n_doc = count_files_with_extension("/", ".doc")
                total_n_docx= count_files_with_extension("/", ".docx")
                total_n_pdf = count_files_with_extension("/", ".pdf")
                total_n_zip = count_files_with_extension("/", ".zip")
                total_n_sql = count_files_with_extension("/", ".sql")
                total_n_bak = count_files_with_extension("/", ".bak") 
                total_n_csv += count_files_with_extension("/", ".csv") 
                total_n_txt += count_files_with_extension("/", ".txt") 
                total_n_jpg += count_files_with_extension("/", ".jpg") 
                total_n_psd += count_files_with_extension("/", ".psd")
                total_n_mp4 += count_files_with_extension("/", ".mp4") 
                total_n_png += count_files_with_extension("/", ".png") 
                total_n_dll += count_files_with_extension("/", ".dll")
                total_n_exe += count_files_with_extension("/", ".exe") 
                total_n_tif += count_files_with_extension("/", ".tif") 
                total_n_avi += count_files_with_extension("/", ".avi") 
                total_n_pst += count_files_with_extension("/", ".pst")
                total_n_log += count_files_with_extension("/", ".log")

        else:
            print("Incorrect input")
            return 0

        cursor = connection.cursor()

        # Extract relevant information from system_info
        system_info_str = " ".join(str(info) for info in system_info)
        system_info_str = system_info_str[:255]  # Truncate to fit in VARCHAR(255)

        # Handle the case when drives is None

        if drives is None:
            cursor.execute('''
                    INSERT INTO f_machine_files_summary_count (
                    hostname, ip_address, os_name, os_version, system_info, number_of_drives, name_of_drives, 
                    total_n_files, total_n_xls, total_n_xlsx, total_n_doc, total_n_docx, total_n_pdf, total_n_zip, total_n_sql, total_n_bak,
                    total_n_csv,total_n_txt,total_n_jpg,total_n_psd,total_n_mp4,total_n_png,total_n_dll,total_n_exe,total_n_tif,total_n_avi,total_n_pst,total_n_log,
                    row_creation_date_time, row_created_by,row_modification_date_time,row_modification_by
                    )
                    VALUES (%s, %s, %s, %s, %s, NULL, NULL, 
                    %s, %s, %s,%s,%s,%s,%s,%s,%s,
                    %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    FROM_UNIXTIME(%s),%s,FROM_UNIXTIME(%s),%s
                    ) ON DUPLICATE KEY UPDATE
                            total_n_files=VALUES(total_n_files), 
                            total_n_xls=VALUES(total_n_xls), 
                            total_n_xlsx=VALUES(total_n_xlsx), 
                            total_n_doc=VALUES(total_n_doc), 
                            total_n_docx=VALUES(total_n_docx), 
                            total_n_pdf=VALUES(total_n_pdf), 
                            total_n_zip=VALUES(total_n_zip), 
                            total_n_sql=VALUES(total_n_sql), 
                            total_n_bak=VALUES(total_n_bak),
                            total_n_csv =VALUES(total_n_csv), 
                            total_n_txt =VALUES(total_n_txt), 
                            total_n_jpg =VALUES(total_n_jpg), 
                            total_n_psd =VALUES(total_n_psd), 
                            total_n_mp4 =VALUES(total_n_mp4), 
                            total_n_png =VALUES(total_n_png),          
                            total_n_dll =VALUES(total_n_dll), 
                            total_n_exe =VALUES(total_n_exe), 
                            total_n_tif   =VALUES(total_n_tif),      
                            total_n_avi   =VALUES(total_n_avi), 
                            total_n_pst   =VALUES(total_n_pst),      
                            total_n_log  =VALUES(total_n_log), 
                            row_modification_date_time = FROM_UNIXTIME(%s),row_modification_by=%s; 

                ''', (
                hostname, ipaddress, os_name, os_version, system_info_str, 
                total_files, total_n_xls, total_n_xlsx, total_n_doc, total_n_docx, total_n_pdf, total_n_zip, total_n_sql, total_n_bak,
                total_n_csv,total_n_txt,total_n_jpg,total_n_psd,total_n_mp4,total_n_png,total_n_dll,total_n_exe,total_n_tif,total_n_avi,total_n_pst,total_n_log,
                start_time, employee_username, start_time, employee_username,
                start_time, employee_username
                
                ))
        #the below is for windows
        else:
            cursor.execute('''
                    INSERT INTO f_machine_files_summary_count (
                    hostname, ip_address, os_name, os_version, system_info, number_of_drives, name_of_drives, 
                    total_n_files, total_n_xls, total_n_xlsx, total_n_doc, total_n_docx, total_n_pdf, total_n_zip, total_n_sql, total_n_bak,
                    total_n_csv,total_n_txt,total_n_jpg,total_n_psd,total_n_mp4,total_n_png,total_n_dll,total_n_exe,total_n_tif,total_n_avi,total_n_pst,total_n_log,
                    row_creation_date_time, row_created_by,row_modification_date_time,row_modification_by
                    
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s,%s,%s,%s,%s,%s,%s,
                    %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    
                    FROM_UNIXTIME(%s),%s,FROM_UNIXTIME(%s),%s
                    ) ON DUPLICATE KEY UPDATE
                            total_n_files=VALUES(total_n_files), 
                            total_n_xls=VALUES(total_n_xls), 
                            total_n_xlsx=VALUES(total_n_xlsx), 
                            total_n_doc=VALUES(total_n_doc), 
                            total_n_docx=VALUES(total_n_docx), 
                            total_n_pdf=VALUES(total_n_pdf), 
                            total_n_zip=VALUES(total_n_zip), 
                            total_n_sql=VALUES(total_n_sql), 
                            total_n_bak=VALUES(total_n_bak),
                            total_n_csv =VALUES(total_n_csv), 
                            total_n_txt =VALUES(total_n_txt), 
                            total_n_jpg =VALUES(total_n_jpg), 
                            total_n_psd =VALUES(total_n_psd), 
                            total_n_mp4 =VALUES(total_n_mp4), 
                            total_n_png =VALUES(total_n_png),          
                            total_n_dll =VALUES(total_n_dll), 
                            total_n_exe =VALUES(total_n_exe), 
                            total_n_tif   =VALUES(total_n_tif),      
                            total_n_avi   =VALUES(total_n_avi), 
                            total_n_pst   =VALUES(total_n_pst),      
                            total_n_log  =VALUES(total_n_log), 
                            row_modification_date_time = FROM_UNIXTIME(%s),row_modification_by=%s; 

                ''', (
                hostname, ipaddress, os_name, os_version, system_info_str,len(drives), drive_names,
                total_files, total_n_xls, total_n_xlsx, total_n_doc, total_n_docx, total_n_pdf, total_n_zip, total_n_sql, total_n_bak,
                total_n_csv,total_n_txt,total_n_jpg,total_n_psd,total_n_mp4,total_n_png,total_n_dll,total_n_exe,total_n_tif,total_n_avi,total_n_pst,total_n_log,
                start_time, employee_username, start_time, employee_username,
                start_time, employee_username
                
                ))
            
           

            for sh_drive in shared_drives:
                if sh_drive['path']:
                    shared_folder_name = sh_drive['netname']
                    shared_folder_path = sh_drive['path']
                    truncated_shared_folder_path = shared_folder_path[:2499]
                    shared_folder_description = sh_drive['remark']
                    #Insert into d_shared_folders
                    cursor.execute('''
                        INSERT INTO d_shared_folders (f_machine_files_summary_count_fk,
                        hostname, ip_address, os_name, os_version, system_info, number_of_drives, name_of_drives, 
                        shared_folder_name,shared_folder_path,shared_folder_description,
                        row_creation_date_time, row_created_by,row_modification_date_time,row_modification_by
                    
                        )
                        VALUES ((SELECT f_machine_files_summary_count_pk FROM f_machine_files_summary_count WHERE hostname = %s),
                        %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,
                        FROM_UNIXTIME(%s),%s,FROM_UNIXTIME(%s),%s
                        ) ON DUPLICATE KEY UPDATE
                                shared_folder_path=VALUES(shared_folder_path), 
                                shared_folder_description=VALUES(shared_folder_description), 
                                row_modification_date_time = FROM_UNIXTIME(%s),row_modification_by=%s;
                            
                        ''', (hostname,
                        hostname, ipaddress, os_name, os_version, system_info_str,len(drives), drive_names,
                        shared_folder_name,truncated_shared_folder_path,shared_folder_description,
                        start_time, employee_username, start_time, employee_username,
                        start_time, employee_username                       
                    
                        ))
            connection.commit()
        #print(query)
        print("[bright_green]Data inserted into f_machine_files_summary_count table,d_shared_folders .[/bright_green]")
        logger.success("Data inserted into f_machine_files_summary_count table,d_shared_folders .")



    except Exception as e:
        print("[bright_red]An error occurred during database operations.[/bright_red]")
        logger.error(f"Error insert_f_machine_files_summary_count: {str(e)}", exc_info=True)
        #logger.error(f"{query}", exc_info=True)
#To be commented and not used


def windows(connection ):
    drives = get_drives()
    
    removeable_drives = get_removable_drives()
    extension = (".xls", ".xlsx")
    if not drives:
        print("[bright_yellow]No drives found.[/bright_yellow]")
        logger.warning("No drives found.")
    else:
        print("Drives Detected on this PC:")
        for i, drive in enumerate(drives, start=1):
            if drive in removeable_drives:
                print(f"{i}. {drive} (removable)")
            else:
                print(f"{i}. {drive}")

        scan_option_choices = ["All Drive Scan", "Specific Drive Scan"]
        scan_option = select("Select the type of scan:", choices=scan_option_choices).ask()

        try:
            if scan_option == "All Drive Scan":
                print(f"Performing a full scan for data files modified or accessed in the last {n_days} days.")
                print('***')
                print("The Tool is now scanning for Data Files. Please Wait...")

                found_assets = []
                for drive in drives:
                    found_assets.extend(search_files(drive, d_file_details_file_extensions, n_days, sensitive_patterns))
                print(
                    "[bright_green]The File Scanning is now complete!! Please wait while we insert the data into the database...[/bright_green]")
                logger.success("The File Scanning is now complete!! Please wait while we insert the data into the database.")
            elif scan_option == "Specific Drive Scan":
                drive_choice = input(
                    r"Enter the drive letter to scan (e.g., C:\, D:\, E:\, ...) or drive path (e.g., C:\Users\Username): ").upper()

                # if drive_choice in [d[0] for d in drives]:
                # selected_drive = [d for d in drives if d[0] == drive_choice][0]
                print(f"Scanning {drive_choice} for data files modified or accessed in the last {n_days} days:")
                print('***')
                print("Windows Scan: The Tool is now counting For  Files. Please Wait...")
                found_assets = search_files(drive_choice, d_file_details_file_extensions, n_days, sensitive_patterns)
                print('[bright_green]Windows Scan: The Tool has completed count For  Files. Please Wait...[/bright_green]')
                logger.success("Windows Scan: The Tool has completed count For  Files. Please Wait...")
                # else:
                #     print("Invalid drive choice.")
                #     found_assets = []
            else:
                print("Invalid option selected.")
                logger.error("Invalid option selected.")
                found_assets = []
        except ValueError:
            print("Invalid input. Please enter a valid option or drive letter.")
            logger.error("Invalid input. Please enter a valid option or drive letter.")

        
        try:
           
            # inserts data into f_machine_files_summary_count;
            print('[bright_green]Windows Scan: insert_f_machine_files_summary_count in progress. Please Wait...[/bright_green]')
            logger.success("Windows Scan: insert_f_machine_files_summary_count in progress. Please Wait...")
            
            insert_f_machine_files_summary_count(connection, ipaddrs, hostname, ops, os_name, os_version, system_info,employee_username,start_time )
            print('[bright_green]Windows Scan: insert_f_machine_files_summary_count complete. Please Wait...[/bright_green]')
            logger.success("Windows Scan: insert_f_machine_files_summary_count complete. Please Wait......")
            
                
            if found_assets:
                for asset in found_assets:
                    #print('[bright_green]Windows Scan: Files details upsert_to_database. Please Wait...[/bright_green]')
                    #logger.success("Windows Scan: Files details upsert_to_database. Please Wait......")
                    upsert_to_database(asset, connection, employee_username, start_time)
                print(
                    f"[bright_green]Scan results for the last {n_days} days saved to the MySQL database...[/bright_green]")
                logger.info("Scan results for the last {n_days} days saved to the MySQL database.")
                print(
                    f"[bright_green]Scan result inserted into details table.[/bright_green]")
                logger.info("Scan result inserted into details table.")
            else:
                print("[bright_yellow]No data assets found.[/bright_yellow]")
                logger.warning("No data assets found.")
        except Exception as e:
            # Log the error to the log file
            logger.error(f"Error connecting to the database: {str(e)}")
        # finally:
        #     if connection:
        #         connection.close()
        #if (os.getenv("ENABLE_EXCEL_FILE_DATA_SCAN")).lower()=="true":
        if enable_excel_file_data_scan =="true":    
            if ".xls" or ".xlsx" in d_file_details_file_extensions:
                # xls_files = [file for file in found_assets if file.lower().endswith(".xls")]
                xls_files = [file for file in found_assets if file.lower().endswith(extension)]
                if xls_files:
                    create_xls_file_sheet_table(connection, xls_files,employee_username,start_time)
                    create_xls_file_sheet_row_table(connection, xls_files,employee_username,start_time)
                    connection.close()
                else:
                    print("No .xls files found.")
    end_time = time.time()
    elapsed_time = end_time - start_time
    ip = get_ip_address()
    scan = 'Scanning'
    create_audit_table(connection, employee_username, ip, start_time, end_time, elapsed_time, scan)
    


def linux(connection):
    start_time = time.time()
    try:
        extension = (".xls", ".xlsx")
        root_dir = '/'
        scan_option_choices = ["All Drive Scan", "Specific Path Scan"]
        scan_option = select("Select the type of scan:", choices=scan_option_choices).ask()
        if scan_option == "All Drive Scan":
            print(f"Performing a full scan for data files modified or accessed in the last {n_days} days: ")
            print('***')
            print("The Tool is now scanning for Data Files. Please Wait...")
            found_assets = []

            found_assets.extend(search_files(root_dir, d_file_details_file_extensions, n_days, sensitive_patterns))
            print("[bright_green]The File Scanning is now complete!! Please wait while we insert the data into the database...[/bright_green]")
            logger.success("The File Scanning is now complete!! Please wait while we insert the data into the database.")
        elif scan_option == "Specific Path Scan":
            path_choice = input("Enter the path (eg: root/home/gg): ").upper()
            print(f"Scanning {path_choice} for data files modified or accessed in the last {n_days} days:")
            print('***')
            print("The Tool is now scanning for Data Files. Please Wait...")
            found_assets = search_files(path_choice, d_file_details_file_extensions, n_days, sensitive_patterns)
            print("[bright_green]The File Scanning is now complete!! Please wait while we insert the data into the database...[/bright_green]")
            logger.success("The File Scanning is now complete!! Please wait while we insert the data into the database.")
        else:
            print("Invalid option selected.")
            logger.error("Invalid option selected.")
            found_assets = []

        #connection = None
        try:
            insert_f_machine_files_summary_count(connection, ipaddrs, hostname, ops, os_name, os_version, system_info,employee_username,start_time )

            if found_assets:
                for asset in found_assets:
                    upsert_to_database(asset, connection, employee_username, start_time)
                print(f"Scan results for the last {n_days} days saved to the MySQL database.")
            else:
                print("No data assets found.")
                logger.warning("No data assets found.")
        except Exception as e:
            # Log the error to the log file
            logger.error(f"Error connecting to the database: {str(e)}")
        # finally:
        #     if connection:
        #         connection.close()

        #if (os.getenv("ENABLE_EXCEL_FILE_DATA_SCAN")).lower() == "true":
        if enable_excel_file_data_scan =="true":        
            if ".xls" or ".xlsx" in d_file_details_file_extensions:
                xls_files = [file for file in found_assets if file.lower().endswith(extension)]
                if xls_files:
                    create_xls_file_sheet_table(connection, xls_files,employee_username,start_time)
                    create_xls_file_sheet_row_table(connection, xls_files,employee_username,start_time)
                    #connection.close()
                else:
                    print("No .xls files found.")
                    logger.warning("No .xls files found.")
    except Exception as e:
        print("[bright_red]An error occurred during the scan and database operations.[/bright_red]")
        logger.error(f"Error in the linux function: {str(e)}", exc_info=True)
    finally:
        end_time = time.time()
        elapsed_time = end_time - start_time
        ip = get_ip_address()
        scan = 'Scanning'
        create_audit_table(connection, employee_username, ip, start_time, end_time, elapsed_time, scan)


def count_all_files(directory):

    try:
        total_files = 0
        for root, _, files in os.walk(directory):
            total_files += len(files)
        return total_files

    except Exception as e:
         # Log the error to the log file
        logger.error(f"Counting all files: {str(e)}")
        return None


def count_files_with_extension(directory, extension):
    try:
        count = 0
        for root, dirs, files in os.walk(directory):
            for file in files:
                if file.lower().endswith(extension.lower()):
                    count += 1

        return count
    except Exception as e:
         # Log the error to the log file
        logger.error(f"Counting files with extensions: {str(e)}")
        return None
    
def insert_log_file_to_mysql(connection, log_folder, ip_address, hostname, employee_username,start_time):
    try:
        log_file_path = os.path.join(log_folder, f"{hostname}_{ip_address}.log")
        if os.path.exists(log_file_path):
            with open(log_file_path, 'r') as log_file:
                log_content = log_file.read()
            cursor = connection.cursor()
            cursor.execute('''
                INSERT INTO app_log_file (
                    f_machine_files_summary_count_fk,
                    ip_address,
                    hostname,
                    app_log,
                    row_creation_date_time,
                    row_created_by,
                    row_modification_date_time,
                    row_modification_by
                )
                VALUES (
                    (SELECT f_machine_files_summary_count_pk FROM f_machine_files_summary_count WHERE ip_address = %s),%s,%s,%s,FROM_UNIXTIME(%s),%s,FROM_UNIXTIME(%s),%s
                );
            ''', (
                ip_address,
                ip_address,
                hostname,
                log_content,
                start_time,
                employee_username,
                start_time,
                employee_username
            ))

            connection.commit()
            print("[bright_green]Log file content inserted into app_log_file table.[/bright_green]")
        else:
            print("[bright_yellow]Log file not found. Skipping insertion.[/bright_yellow]")

    except Exception as e:
        print("[bright_red]An error occurred during log file insertion.[/bright_red]")
        print(f"Error: {str(e)}")
        #should there not be a rollback everywhere?
        connection.rollback()





if __name__ == "__main__":
    
    # Change this flag to 'file' or 'database' based on your needs
    #if "enable_env_from_db = true" then the env values will be fetched from the database
       
        start_time = time.time()
        import platform
        from dotenv import load_dotenv
        load_dotenv()
        
        # Get the database connection strings
        #global host
        host = os.getenv("MYSQL_HOST")  # Replace with the MySQL server address
        #global port
        port = os.getenv("MYSQL_PORT")  # Replace with the MySQL server port
        
        
        #global database_name 
        #database_name = None
       
        
        database_name = os.getenv("MYSQL_DATABASE")
        #global username 
        username = os.getenv("MYSQL_USERNAME")
        #global password 
        password = os.getenv("MYSQL_PASSWORD")

        
                
        os_name = platform.system()
        # Get the OS release version
        os_version = platform.release()
        # Get more detailed system information
        system_info = platform.uname()
        hostname = socket.gethostname()
        ipaddrs = get_ip_address()
        app_log=logger.add(f"{hostname}_{ipaddrs}.log")
        logger.info("********************Start-Log********************")
        logger.info(f"Your IP Address:, {ipaddrs}")
        logger.info(f"Your Host Name: , {hostname}")
        logger.info(f"Operating System: {os_name}")
        logger.info(f"OS Version: {os_version}")
        logger.info(f"System Information: {system_info}")
        logger.info(f"database_name: {database_name}")
        print("Your IP Address:", ipaddrs)
        print("Your Host Name: ", hostname)
        print(f"Operating System: {os_name}")
        print(f"OS Version: {os_version}")
        print(f"System Information: {system_info}")
        #this is picking up old database information
        print(f"database_name: {database_name}")
        print(f"username: {username}")
        print(f"password: {password}")
        
        employee_username = input("Enter your Employee username: ")
        scan_choices = ["File Count", "File Data Scan"]
        scan = select("Select the type of scan:", choices=scan_choices).ask()

        ops_choices = ["Windows", "Linux"]
        ops = select("Select the Operating System:", choices=ops_choices).ask()
        
        
        connection = create_db_connection(host, port, database_name,username,password)
        print(f"connection: {connection}")
        retrieve_env_values(enable_env_from_db,connection)
        print(f"enable_env_from_db: {enable_env_from_db}")
        
        if scan == "File Count":
            print('***')
            print("The tool is now counting the Data Files. Please Wait...")
            
            
            insert_f_machine_files_summary_count(connection, ipaddrs, hostname, ops, os_name, os_version, system_info,employee_username,start_time )
            end_time = time.time()
            elapsed_time = end_time - start_time
            create_audit_table(connection, employee_username, ipaddrs, start_time, end_time, elapsed_time, scan)
            #Enter the error data here 
            #connection.close()
            print('[bright_green]The File Counting is now complete.[/bright_green]')
            logger.success("The File Counting is now complete.")
        elif scan == "File Data Scan":
            if ops == "Windows":
                
                windows(connection)
                
            elif ops == "Linux":
                linux(connection)
            else:
                print("Incorrect input")
        else:
            print("Incorrect input")

        logger.info("********************End-Log********************")
        log_folder = os.path.dirname(os.path.abspath(__file__))
        if os.getenv("ENABLE_APP_LOG_TO_DB")=="true":
            insert_log_file_to_mysql(connection, log_folder, ipaddrs, hostname, employee_username,start_time)
        print("Press Esc to exit...")
        connection.close()
        while keyboard.is_pressed('Esc') == False:
            pass
