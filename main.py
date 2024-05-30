import os
import sys
import json
from GoogleSpacesLogger import GoogleSpacesLogger
import logging, logging.handlers
from tendo import singleton
from datetime import datetime
from dialer import  fetch_and_store_api_data_locally, move_sqlite_data_to_postgres, push_data_to_aws
from pathlib import Path

# Prevent multiple instances of this script from running
try:
    me = singleton.SingleInstance()
except singleton.SingleInstanceException:
    logging.critical('%s already being executing. Exiting', __file__)
    exit()
except BaseException as e:
    logging.critical('An unknown error occurred while instantiating singleton.SingleInstance(). Exiting')
    exit()

os.chdir(Path(__file__).resolve().parent)

# Create a log file and write errors to it
logpath = f'./log/{datetime.now().strftime("%Y")}/{datetime.now().strftime("%Y-%m")}'
if not os.path.exists(logpath):
    os.makedirs(logpath)

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y/%m/%d %I:%M:%S %p',
                    handlers=[
            logging.FileHandler(f'{logpath}/{datetime.now().strftime("%Y-%m-%d")}.log')
        ])

with open('config.json', 'r') as config_json_file:
    config = json.load(config_json_file)

# # Set up the Google chat webhook
# gLogger = GoogleSpacesLogger(config['google_chat_webhook'])
# # Log only errors and critical errors
# gLogger.setLevel(logging.ERROR)
# logging.getLogger().addHandler(gLogger)

if config['verbose']:
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

logging.info('Starting API request loop.')

if fetch_and_store_api_data_locally(config):
    move_sqlite_data_to_postgres(config)
    # push_data_to_aws(config)

logging.info('API request loop has ended.')