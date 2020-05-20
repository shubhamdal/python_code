import os
import sys
import re
import math
import logging
from time import sleep
import requests
from requests.exceptions import RequestException
from collections import OrderedDict
sys.path.insert(1, '/home/sburman4/MR_Jobs/credentials')
import cred



logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(formatter)
logger.addHandler(ch)

QUERY_SERVICE_API = os.getenv("QUERY_SERVICE_API", "https://query-engine.apple.com/query")
MAX_RETRIES = 2
SLEEP_TIME_SEC = 7200


class SQLFailedError(Exception):
    pass


def execute_sql(sql, db_url, username, password):
    n_retry = 0
    result = None
    while True:
        try:
            r = requests.post(QUERY_SERVICE_API, data={
                "dsn": db_url,
                "username": username,
                "password": password,
                "sql": sql
            },verify='/etc/ssl/certs/ca-bundle.crt')
            r.raise_for_status()
            result = r.json()
            break
        except RequestException:
            try:
                logger.exception('Error message: {}'.format(r.json()))
                break
            except ValueError:
                logger.exception('No specific error message')
            n_retry += 1
            if n_retry <= MAX_RETRIES:
                logger.info('Attempt {}/{} failed, retrying'.format(n_retry, MAX_RETRIES+1))
            else:
                logger.error('Attempt {}/{} failed, max retry reached'.format(n_retry, MAX_RETRIES+1))
                break
            sleep(SLEEP_TIME_SEC)
    return result


def exec_teradata_sql(sql):
    db_url = os.getenv("TERADATA_URL")
    username = os.getenv("TERADATA_USERNAME")
    password = os.getenv("TERADATA_PASSWORD")
    print("Inside sql query")
    result = execute_sql(sql, db_url, username, password)
    print("checking")
    print(type(result))
    if not result:
        raise SQLFailedError("Teradata SQL failed")
    columns = OrderedDict()  # columns is the column_name => array_index map
    for i, c in enumerate(result["json"]["columns"]):
        columns[c["name"].lower()] = i  # lower-cased
    rows = result["json"]["results"]
    return columns, rows


def run_query_file(infile, verbose=False):
    with open(infile, 'r') as f:
        queries = [q.strip() for q in re.split(r';[\s\r\n]+', f.read()) if q.strip()]
    for i, q in enumerate(queries):
        if verbose:
            logger.info('Running query: {}/{}\n{}'.format(i+1, len(queries), q))
        columns, rows = exec_teradata_sql(q)
        if not columns:
            logger.info('No output result')
        else:
            for k, v in columns.items():
                logger.info('{}: {}'.format(k, rows[v]))


def batch_execute_sql(query_list, batch_size):
    size = len(query_list)
    for i in range(0, size, batch_size):
        logger.info("Executing batch: {}/{}".format(int(i / batch_size) + 1, int(math.ceil(size / batch_size))))
        _, _ = exec_teradata_sql(''.join(query_list[i:i+batch_size]))
