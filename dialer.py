import os
import sys
import csv
import json
import sqlite3
import logging
import psycopg2
import requests
from pathlib import Path
from datetime import datetime
import subprocess as sp

# Get config variables to authenticate
def get_authentication_credentials(config: dict, num_runs=3):
    """get_authentication_credentials sends a post request to login and retrieve
       client, access-token and uid credentials

    Args:
        config (dict): Environmental variables
        num_runs (:obj:`int`, optional): Number of times to retry the request
            if there is an error. Default value is 3

    Returns:
        dict: The return is a dictionary containing the client, access-token and uid credentials

    """
    authentication_credentials = {"client": None, "access_token": None, "uid": None}

    # Get temporary credentials (sign-in)
    sign_in_url = config['sign_in_url']

    payload = {
        "email": config['sign_in_email'],
        "password": config['sign_in_password']
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }

    for run in range(num_runs):
        try:
            response = requests.post(sign_in_url, json=payload, headers=headers)
        except requests.exceptions.HTTPError as error:  # Raised if an HTTP error occurs (e.g., 4xx or 5xx status codes).
            if run < num_runs-1:
                logging.info(f"HTTP Error. Trying again {run}")
                continue
            logging.error(error)
            break
        except requests.exceptions.ConnectionError as error:  # Raised if a connection error occurs, such as a timeout or refused connection.
            if run < num_runs-1:
                logging.info(f"Connection Error. Trying again {run}")
                continue
            logging.error(error)
            break
        except requests.exceptions.Timeout as error:  # Raised if the request times out.
            if run < num_runs-1:
                logging.info(f"Timeout Error. Trying again {run}")
                continue
            logging.error(error)
            break
        except requests.exceptions.RequestException as error:  # This is a generic exception raised for all request errors. It's a good catch-all for handling any unexpected errors.
            if run < num_runs-1:
                logging.info(f"Unknown Error. Trying again {run}")
                continue
            logging.error(error)
            break
        else:
            if response.status_code != 200:
                logging.info("Status code is not 200. Response was not what was expected.")
                break

            logging.info("Authentication successful. Signed in.")

            response_headers = response.headers

            authentication_credentials['client'] = response_headers['client']
            authentication_credentials['access_token'] = response_headers['access-token']
            authentication_credentials['uid'] = response_headers['uid']
            break

    return authentication_credentials


# Get API response data on dialer users
def get_dialer_users_data(config: dict, authentication_credentials: dict, num_runs=3):
    """get_dialer_users_data sends a post request to get users data from the dialer.

    Args:
        config (dict): Environmental variables
        authentication_credentials (dict): client, access-token and uid credentials
        num_runs (:obj:`int`, optional): Number of times to retry the request
            if there is an error. Default value is 3

    Returns:
        dict | None: The return value is a dictionary containing the data from the API response
        or None if the post request fails

    """
    response_json = None

    # Access environment variables
    last_timestamp = config['last_updated_at_timestamp']
    limit = config['limit']

    # Get users information
    users_url = config['users_url']
    admin_account_id = config['admin_account_id']
    technical_support_id = config['technical_support_id']

    body = {
            "fields": ["first_name", "last_name", "email", "created_by", "updated_by", "created_at", "updated_at", "departments", "priority_departments", "restricted_departments", "telephone", "contact", "roles", "user_groups", "country", "job_title", "legacy_email", "supervisor_groups", "user_type", "webrtc_device", "permission_messaging", "permission_report", "permission_settings", "permission_supervisor", "auto_voice_answer_timeout", "channel_priority", "channel_restriction", "default_currency", "primary_device", "disable_auto_logout", "divert_destination", "voice_queue_timeout", "enabled", "enforce_outbound_endpoint", "extension", "followed_by", "inbound_transfer_department", "force_turn", "force_turn_tcp", "outbound_endpoint", "ice_timeout", "idle_auto_recovery", "idle_auto_recovery_timeout", "custom_interaction_capacity", "linked_entities", "locked", "voicemail_timeout", "mobile_device", "not_responding_auto_recovery", "not_responding_timeout", "permission_restrict_acd_routing", "permission_restrict_acd_to_voice", "pbx_user", "remote_access", "permission_restrict_login", "use_custom_interaction_capacity", "voicemail_email", "voicemail_enabled", "voicemail_message", "whatsapp_notifications_telephone"],
            "filters": [{
            "operator": "all",
            "conditions": [{
                "name": "updated_at",
                "value": last_timestamp,
                "operator": "more-than"
                },{
                "name": "id",
                "value": admin_account_id,
                "operator": "not-equal"
                },{
                "name": "id",
                "value": technical_support_id,
                "operator": "not-equal"
                }]
            }],
            "sort": "updated_at asc",
            "limit": limit
        }

    headers = {
        "client": authentication_credentials['client'],
        "access-token": authentication_credentials['access_token'],
        "uid": authentication_credentials['uid']
    }

    for run in range(num_runs):
        try:
            logging.info(f"Making the Dialer API request. Fields: {body['fields']}, Filters: {body['filters']}, Sort: {body['sort']}, Limit: {body['limit']}")
            response = requests.post(users_url, headers=headers, json=body)
        except requests.exceptions.HTTPError as error:  # Raised if an HTTP error occurs (e.g., 4xx or 5xx status codes).
            if run < num_runs-1:
                logging.info(f"HTTP Error. Trying again {run}")
                continue
            logging.error(error)
            break
        except requests.exceptions.ConnectionError as error:  # Raised if a connection error occurs, such as a timeout or refused connection.
            if run < num_runs-1:
                logging.info(f"Connection Error. Trying again {run}")
                continue
            logging.error(error)
            break
        except requests.exceptions.Timeout as error:  # Raised if the request times out.
            if run < num_runs-1:
                logging.info(f"Timeout Error. Trying again {run}")
                continue
            logging.error(error)
            break
        except requests.exceptions.RequestException as error:  # This is a generic exception raised for all request errors. It's a good catch-all for handling any unexpected errors.
            if run < num_runs-1:
                logging.info(f"Unknown Error. Trying again {run}")
                continue
            logging.error(error)
            break
        else:
            if response.status_code != 200:
                logging.info("Status code is not 200. Response was not what was expected.")
                break

            response_text = response.text

            # Parse the JSON string into a Python dictionary
            response_json = json.loads(response_text)
            logging.info(f"API response data has been retrieved.")
            break
    return response_json


# Create sqlite database
def create_sqlite_database():
    """create_sqlite_database creates an sqlite creates and connects to a database and then
       creates a table named staging_table if it doesnt exist.

    Args:

    Returns:
        dict: returns sqlite connection and cursor if successful, None otherwise.
    """

    # Check if the temp_files folder exists, if not create the folder
    if not os.path.exists("./temp_files"):
        os.makedirs("./temp_files")

    logging.info(f"Connecting to SQLite")

    # Connect to SQLite database
    try:
        conn = sqlite3.connect('temp_files/staging.db')
    except Exception as exp:
        return {
            'conn': None,
            'cursor': None
        }
    cursor = conn.cursor()

    # Execute a query to check if the table exists
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='staging_table'")

    # Fetch the result (the table metadata)
    result = cursor.fetchone()

    # Check if the table exists
    if not result:
        cursor.executescript(Path('queries/init_sqlite.sql').read_text())

    sqlite_connections = {}
    sqlite_connections['cursor'] = cursor
    sqlite_connections['conn'] = conn

    return sqlite_connections


# Insert data into sqlite
def insert_data_into_sqlite(conn, cursor, api_data):
    """insert_data_into_sqlite inserts the data gotten from the get_api_data function into
       a table(staging_table) in an sqlite database(staging.db)

    Args:
        conn: sqlite connection
        cursor: sqlite cursor
        api_data (list): a list containing a maximum of 1000 records of dialer users.

    Returns:
        bool: True if successful, False otherwise,
    """
    sqlite_insertion_successful = True
    # Insert new records into staging table
    logging.info(f"Inserting records to SQLite.")

    records = [(record['id'], record['entity_type'], record['label'], record['icon'], record['first_name'], \
                record['last_name'], record['email'], record['created_by'], record['updated_by'], \
                record['created_at'], record['updated_at'], json.dumps(record['departments']), \
                json.dumps(record['priority_departments']), json.dumps(record['restricted_departments']), \
                record['telephone'], record['contact'], json.dumps(record['roles']), json.dumps(record['user_groups']), \
                record['country'], record['job_title'], record['legacy_email'], json.dumps(record['supervisor_groups']), \
                record['user_type'], record['webrtc_device'], record['permission_messaging'], \
                record['permission_report'], record['permission_settings'], record['permission_supervisor'], \
                record['auto_voice_answer_timeout'], json.dumps(record['channel_priority']), json.dumps(record['channel_restriction']), \
                json.dumps(record['default_currency']), record['primary_device'], record['disable_auto_logout'], \
                record['divert_destination'], record['voice_queue_timeout'], record['enabled'], \
                record['enforce_outbound_endpoint'], record['extension'], json.dumps(record['followed_by']), \
                json.dumps(record['inbound_transfer_department']), record['force_turn'], record['force_turn_tcp'], \
                json.dumps(record['outbound_endpoint']), record['ice_timeout'], record['idle_auto_recovery'], record['idle_auto_recovery_timeout'], \
                json.dumps(record['custom_interaction_capacity']), json.dumps(record['linked_entities']), \
                record['locked'], record['voicemail_timeout'], json.dumps(record['mobile_device']), \
                record['not_responding_auto_recovery'], record['not_responding_timeout'], record['permission_restrict_acd_routing'], \
                record['permission_restrict_acd_to_voice'], record['pbx_user'], record['remote_access'], \
                record['permission_restrict_login'], record['use_custom_interaction_capacity'], record['voicemail_email'], record['voicemail_enabled'], \
                record['voicemail_message'], record['whatsapp_notifications_telephone'], record['__created_by'], \
                record['__updated_by'], json.dumps(record['__departments']), json.dumps(record['__roles']), \
                record['__webrtc_device'], record['__primary_device'], json.dumps(record['__priority_departments']), \
                json.dumps(record['__restricted_departments']), json.dumps(record['__user_groups']), \
                json.dumps(record['__supervisor_groups']), json.dumps(record['__followed_by']), \
                json.dumps(record['__linked_entities']), json.dumps(record['__channel_priority']), \
                json.dumps(record['__channel_restriction']), record['__country'], \
                record['__country__color'], record['__user_type'], record['__user_type__color'], json.dumps(record['user_status'])) for record in api_data]

    try:
        cursor.executemany('''
            INSERT OR REPLACE INTO staging_table (id, entity_type, label, icon, first_name, last_name, \
                    email, created_by, updated_by, created_at, updated_at, departments, priority_departments, \
                    restricted_departments, telephone, contact, roles, user_groups, country, job_title, \
                    legacy_email, supervisor_groups, user_type, webrtc_device, permission_messaging, \
                    permission_report, permission_settings, permission_supervisor, auto_voice_answer_timeout, \
                    channel_priority, channel_restriction, default_currency, primary_device, disable_auto_logout, \
                    divert_destination, voice_queue_timeout, enabled, enforce_outbound_endpoint, extension, \
                    followed_by, inbound_transfer_department, force_turn, force_turn_tcp, outbound_endpoint, ice_timeout, \
                    idle_auto_recovery, idle_auto_recovery_timeout, custom_interaction_capacity, linked_entities, \
                    locked, voicemail_timeout, mobile_device, not_responding_auto_recovery, not_responding_timeout, \
                    permission_restrict_acd_routing, permission_restrict_acd_to_voice, pbx_user, remote_access, \
                    permission_restrict_login, use_custom_interaction_capacity, voicemail_email, voicemail_enabled, voicemail_message, \
                    whatsapp_notifications_telephone, __created_by, __updated_by, \
                    __departments, __roles, __webrtc_device, __primary_device, __priority_departments, __restricted_departments, \
                    __user_groups, __supervisor_groups, __followed_by, __linked_entities, __channel_priority, __channel_restriction, \
                     __country, __country__color, __user_type, __user_type__color, user_status) \
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', records)

    except sqlite3.IntegrityError as error:
        logging.info("Integrity Error")
        logging.error(error)
        sqlite_insertion_successful = False
    except sqlite3.OperationalError as error:
        logging.info("Operational Error")
        logging.error(error)
        sqlite_insertion_successful = False
    except sqlite3.DataError as error:
        logging.info("Data Error")
        logging.error(error)
        sqlite_insertion_successful = False
    except sqlite3.ProgrammingError as error:
        logging.info("Programming Error")
        logging.error(error)
        sqlite_insertion_successful = False
    except Exception as error:
        logging.info("Unknown Error")
        logging.error(error)
        sqlite_insertion_successful = False
    else:
        # Commit changes to the staging table
        conn.commit()
    return sqlite_insertion_successful


def fetch_and_store_api_data_locally(config: dict):
    """fetch_and_store_api_data_locally gets data via the API and stores it in a sqlite
       database.

    Args:
        config (dict): Environmental variables

    Returns:
        bool: True if successful, False otherwise,
    """
    # Create sqlite database
    sqlite_connections = create_sqlite_database()
    sqlite_cursor, sqlite_conn = sqlite_connections['cursor'], sqlite_connections['conn']

    if sqlite_cursor is None or sqlite_conn is None:
        logging.info("Could not connect to SQLite.")
        return False

    # Get temporary credentials
    authentication_credentials = get_authentication_credentials(config)

    # Check if request for authentication credentials was successful
    if authentication_credentials['client'] is None or authentication_credentials['access_token'] is None or authentication_credentials['uid'] is None:
        logging.info("Unable to sign-in to get API keys.")
        return False

    # Time that data was requested
    data_request_time = datetime.now()
    logging.info(f'Data was requested at {data_request_time}')

    # Total number of inserted records
    total_records_inserted_into_sqlite = 0

    while True:
        api_response_json = get_dialer_users_data(config, authentication_credentials)

        if api_response_json is None:
            logging.info("Unable to get response from API.")
            return False

        api_data = api_response_json['results']
        api_data_count = len(api_data)

        if api_data_count > 0:
            sqlite_insertion_successful = insert_data_into_sqlite(sqlite_conn, sqlite_cursor, api_data)

            # Check to see that data was inserted into sqlite
            if not sqlite_insertion_successful:
                logging.info("An error occurred. Data was not inserted into SQLite")
                return False

            # Keep track of total inserted records
            total_records_inserted_into_sqlite += api_data_count
            logging.info(f'API request loop ended. {api_data_count} records inserted into SQLite.')

            # Get the last users updated_at timestamp to mark where last API data was pulled from
            last_record_timestamp = api_data[-1]['updated_at']
            config['last_updated_at_timestamp'] = last_record_timestamp

            logging.info(f'Last timestamp - {last_record_timestamp}')

        else:
            logging.info('Response is empty. There is no additional data to insert to SQLite.')
            logging.info(f"{total_records_inserted_into_sqlite} records total inserted into SQLite.")
            break

    # Function executed successfully. Return True.
    return True


def move_sqlite_data_to_csv(cursor, csv_file_path):
    """move_sqlite_data_to_csv moves the data from the staging table in sqlite to the local
       computer and saves it as a csv file.

    Args:
        cursor: sqlite cursor
        csv_file_path: file path to the csv file with the data from the sqlite database

    Returns:
        bool: returns True if data was written to csv, False otherwise.
    """
    csv_insertion_successful = True

    try:
        logging.info("Saving inserted records in sqlite to csv")
        # Execute a SELECT query to retrieve inserted data
        cursor.execute("SELECT * FROM staging_table")

        # Fetch all rows from the result set
        rows = cursor.fetchall()

        # Write the fetched results to a CSV file
        with open(csv_file_path, 'w') as file:
            writer = csv.writer(file)
            writer.writerow([i[0] for i in cursor.description])  # Write header row
            writer.writerows(rows)  # Write data rows
    except sqlite3.Error as error:
        logging.info("SQLite Error")
        logging.error(error)
        csv_insertion_successful = False
    except IOError as error:
        logging.info("IO Error")
        logging.error(error)
        csv_insertion_successful = False
    except MemoryError as error:
        logging.info("Memory Error")
        logging.error(error)
        csv_insertion_successful = False
    except csv.Error as error:
        logging.info("csv Error")
        logging.error(error)
        csv_insertion_successful = False
    except Exception as error:
        logging.info("Unknown Error")
        logging.error(error)
        csv_insertion_successful = False

    return csv_insertion_successful


def total_records_inserted_into_database(cursor):
    """total_records_inserted_into_database returns the number of records that is inserted
       into a database.

    Args:
        cursor: sqlite cursor

    Returns:
        int: returns the number of records
    """
    # Execute the query to get the number of records
    cursor.execute("SELECT COUNT(*) FROM staging_table")
    result = cursor.fetchone()

    # Extract the count from the result
    total_records_inserted = result[0]

    return total_records_inserted


def create_postgres_database(config, num_runs=3):
    """create_postgres_database connects to postgresql and creates the staging table and main
       table if they don't already exist.

    Args:
        config (dict): Environmental variables
        num_runs (:obj:`int`, optional): Number of times to retry the connection
            if there is an error. Default value is 3

    Returns:
        dict: returns postgres connection and cursor if successful, None otherwise.
    """
    # Connect to PostgreSQL
    # Access environment variables
    dbname = config['db_name']
    user = config['user']
    password = config['password']
    host = config['host']

    logging.info("Connecting to PostgreSQL")

    # Connect to your PostgreSQL database
    for run in range(num_runs):
        try:
            conn = psycopg2.connect(
                dbname=dbname,
                user=user,
                password=password,
                host=host
            )
            break
        except psycopg2.OperationalError as error:
            logging.info(f"Operational Error. Incorrect username or password. Could not connect to PostgreSQL.")
            logging.error(error)
            return {'conn': None,'cursor': None}
        except Exception as error:
            if run < num_runs-1:
                logging.info(f"Unknown Error. Trying again {run}")
                continue
            logging.info("Could not connect to PostgreSQL.")
            logging.error(error)
            return {'conn': None,'cursor': None}

    # Create a cursor object
    cur = conn.cursor()

    logging.info("Creating PostgreSQL staging users table if it doesn't exist")
    # Check if staging table exists already
    cur.execute(f"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'staging_users')")
    result = cur.fetchone()[0]

    if not result:
        # Create the staging_users table if it doesnt exist
        cur.execute(Path('queries/init_staging_table_postgres.sql').read_text())

    logging.info("Creating PostgreSQL users table if it doesn't exist")
    # Check if users table exists already
    cur.execute(f"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'users')")
    result = cur.fetchone()[0]

    if not result:
        # Create the users table if it doesnt exist
        cur.execute(Path('queries/init_postgres.sql').read_text())

    postgresql_connections = {}
    postgresql_connections['conn'] = conn
    postgresql_connections['cursor'] = cur

    return postgresql_connections


def insert_data_into_postgres(conn, cur, csv_file_path, num_runs=3):
    """insert_data_into_postgres_main_table moves the data from the postgres staging table
       to the main table after deleting records in the main table that have their IDs in the
       staging table.

    Args:
        conn: postgreSQL connection
        cur: postgreSQL cursor
        csv_file_path: file path to the csv file with the data from the sqlite database
        num_runs (:obj:`int`, optional): Number of times to retry the request
            if there is an error. Default value is 3

    Returns:
        bool: True if successful, False otherwise,
    """
    postgres_insertion_successful=True

    for run in range(num_runs):
        try:
            logging.info("Inserting csv data into the PosgreSQL staging table.")
            # Truncate the staging table if there is any record there
            cur.execute("TRUNCATE TABLE staging_users")

            # Execute the COPY command to import data from the CSV file
            with open(csv_file_path, 'r') as file:
                cur.copy_expert(f"COPY staging_users FROM STDIN WITH CSV HEADER", file)

            logging.info("Inserting data into PostgreSQL users table")
            # Read SQL script from file
            with open('queries/insert_data_into_postgres.sql', 'r') as file:
                sql_script = file.read()

            # Execute SQL script
            cur.execute(sql_script)

            # Commit the transaction
            conn.commit()
            break
        except Exception as error:
            # Rollback the transaction if an error occurs
            if run < num_runs-1:
                logging.info("Unknown error occurred while inserting data to PostgreSQL table. Trying again {run}")
                continue
            logging.error(error)
            conn.rollback()
            postgres_insertion_successful = False

    return postgres_insertion_successful


def remove_sqlite_database(database_file_path):
    """remove_sqlite_database deletes the sqlite database on the local system.

    Args:
        database_file_path (str): file path to where the SQLite database is

    Returns:
    """
    # Delete sqlite database
    logging.info("Removing SQLite database")

    if os.path.exists(database_file_path):
        os.remove(database_file_path)

    logging.info("SQLite database has been deleted.")


def move_sqlite_data_to_postgres(config):
    """move_sqlite_data_to_postgres moves the data from the local csv file into postgreSQL.

    Args:
        config (dict): Environmental variables

    Returns:

    """
    # Create/connect to sqlite database
    sqlite_connections = create_sqlite_database()
    sqlite_cursor, sqlite_conn = sqlite_connections['cursor'], sqlite_connections['conn']

    if sqlite_cursor is None or sqlite_conn is None:
        logging.info("Could not connect to SQLite.")
        sys.exit()

    # Create/connect to postgreSQL database
    postgresql_connections = create_postgres_database(config)
    postgresql_cursor, postgresql_conn = postgresql_connections['cursor'], postgresql_connections['conn']

    if postgresql_cursor is None or postgresql_connections is None:
        logging.info("Could not connect to PostgreSQL.")
        sys.exit()

    # Specify the file path to save csv file on local computer
    csv_file_path = 'temp_files/user_data.csv'

    # Execute function to retrieve inserted data from sqlite
    csv_insertion_successful = move_sqlite_data_to_csv(sqlite_cursor, csv_file_path)

    if not csv_insertion_successful:
        logging.info("Data from sqlite database was not written to the csv file. An error occurred.")
        logging.info('Exiting...')

        # Delete the SQLite database
        remove_sqlite_database("temp_files/staging.db")

        # Close connection to sqlite and postgreSQL
        sqlite_conn.close()
        postgresql_conn.close()

        sys.exit()

    # Get the number of records to be inserted into PostgreSQL
    total_records_inserted_into_postgresql = total_records_inserted_into_database(sqlite_cursor)

    # Run function to insert csv data into postgres
    postgres_insertion_successful = insert_data_into_postgres(postgresql_conn, postgresql_cursor, csv_file_path)

    if postgres_insertion_successful:
        logging.info(f"{total_records_inserted_into_postgresql} records total inserted into PostgreSQL.")

        # Write new data back to the credentials JSON file
        with open('config.json', 'w') as config_json_file:
            json.dump(config, config_json_file, indent=4)

        logging.info('Last timestamp has been stored!')

    else:
        logging.info("An error occurred. Data was not inserted into postgreSQL main table.")

    # Delete the SQLite database
    remove_sqlite_database("temp_files/staging.db")

    # Close connection to sqlite and postgreSQL
    sqlite_conn.close()
    postgresql_conn.close()


def push_data_to_aws(config):
    """
    The function retrieves data from a local SQLite database and transfers it to AWS S3.
    Then, the data is migrated from S3 to Redshift.
    """
    # Create folder on local file system to save csv file(s).
    if not os.path.exists('data'):
        os.makedirs('data')

    # Connect to the SQLite database.
    conn = sqlite3.connect('temp_files/staging.db')
    cursor = conn.cursor()

    # Get the data out of SQLite database and save to our local system.
    data_dir = os.path.dirname( os.path.realpath(__file__) ) + '/data'
    file_path = data_dir + '/user_data.csv'
    if not move_sqlite_data_to_csv(cursor, file_path):
        logging.error('There was an error moving data from sqlite to csv.')
        sys.exit()

    # Get AWS credentials.
    s3_bucket = config['aws_s3_bucket']
    iam_role = config['aws_iam_role']
    secret = config['aws_secret']
    database = config['aws_database']
    schema = config['aws_schema']

    # Push data to AWS S3 using the AWS CLI.
    try:
        os.system('aws s3 sync ' + data_dir + ' ' + s3_bucket)
    except Exception as error:
        logging.info('There was an unknown error pushing data to AWS S3 using the AWS CLI')
        logging.error(error)
        sys.exit()

    # Insert data into Redshift from AWS S3.
    sql = Path('queries/users.sql').read_text()
    sql = sql.replace('IAM_ROLE', iam_role)
    sql = sql.replace('DATABASE', database).replace('SCHEMA', schema).replace('S3_BUCKET', s3_bucket)

    exec_str = 'aws redshift-data batch-execute-statement  --region us-east-1 --secret ' + secret + ' --cluster-identifier td-dw-01 --sql "' + sql + '"  --database tradedepot'
    try:
        aws_response = sp.getoutput(exec_str)
    except Exception as error:
        logging.info("There was an error executing the `aws redshift-data batch-execute-statement`.")
        logging.error(error)

    logging.info("Response info from AWS: %s. \n Get details by running  aws redshift-data describe-statement --id ID_STRING_FROM_AWS", aws_response)

    # Get the response ID from the AWS response.
    # it will be needed later to check the batch's execution status
    config['aws_response_id'] = json.loads(aws_response)['Id']

    # Write new data back to the config JSON file
    with open('config.json', 'w') as config_json_file:
        json.dump(config, config_json_file, indent=4)

    # Get the number of records to be inserted into AWS
    total_records_inserted_into_aws = total_records_inserted_into_database(cursor)
    logging.info(f"{total_records_inserted_into_aws} records total inserted into AWS.")

    # Delete SQLite database and close the connection
    remove_sqlite_database("temp_files/staging.db")
    conn.close()

    # Returns an indication for success or failure.


def is_last_run_successful(config):
    """Checks the status of the previous data ingestion on AWS

    The previous ingestion needs to be successful before attempting another one

    Args:
        config (dict): Configuration read in from config.json

    Returns:
        bool: True if last run was successful. False otherwise
    """

    # Get ID of last run from config
    try:
        response_id = config['aws_response_id']
    except KeyError as e:
        logging.error("AWS Response ID not found")
        return False
    except BaseException as e:
        logging.error("Unknown error reading AWS Response ID: %s", str(e))
        return False

    # Poll AWS for the status
    exec_str = 'aws redshift-data describe-statement --id ' + response_id

    try:
        aws_response = json.loads( sp.getoutput(exec_str) )
    except Exception as error:
        logging.info("There is an error getting the aws response for the last run. The ID may have expired.")
        logging.error(error)
        return False

    if aws_response['Status'] == 'FINISHED':
        logging.info("Last AWS run %s successful", response_id)
        return True
    elif aws_response['Status'] == 'FAILED':
        logging.critical("Last AWS run %s failed", response_id)
        return False
    else:
        return False