import json
import boto3
import csv
import io
import logging
from datetime import datetime
import traceback
import sys
from pathlib import Path
import requests
import time
import re
import uuid
import bz2
from shutil import copyfileobj
import subprocess
import os
from urllib.parse import urlparse


def compress(filename, delete_original=True, compress_level=9):
    filename_bz2 = Path(Path(__file__).parent, 'tmp', "{}.bz2".format(filename))
    logging.info("Compress file %s. New file: %s. Compression level: %d. Delete original? %s",
                 filename, filename_bz2, compress_level, delete_original)
    with open(filename, 'rb') as input_file:
        with bz2.BZ2File(filename_bz2, 'wb', compresslevel=compress_level) as output_file:
            copyfileobj(input_file, output_file)
    if delete_original:
        os.remove(filename)
    return filename_bz2


def generate_orc_file(filename_json, filename_orc, structure):
    logging.info('Remove temporary ORC file if exists: %s', filename_orc)
    # if there is already an ORC file, delete it. Otherwise orc-tools will issue an error message
    try:
        os.remove(filename_orc)
    except OSError:
        logging.info('Temporary ORC file does not exist')
        pass

    path_orc_tools = Path(Path(__file__).parent, 'tmp', 'orc-tools-1.5.6-uber.jar')

    subprocess.run(['java',
                    '-jar', str(path_orc_tools),
                    'convert', filename_json,
                    '-o', filename_orc,
                    '-s', "".join(structure.split())],
                   check=True)


def instantiate_ec2(ami, key_name, security_group, iam, instance_type="t3a.nano",
                    size=15, init_script="""#!/bin/bash\necho hi""", name="internet_scholar"):
    ec2 = boto3.resource('ec2')
    instance = ec2.create_instances(
        ImageId=ami,
        InstanceType=instance_type,
        MinCount=1,
        MaxCount=1,
        KeyName=key_name,
        InstanceInitiatedShutdownBehavior='terminate',
        UserData=init_script,
        SecurityGroupIds=[security_group],
        BlockDeviceMappings=[
            {
                'DeviceName': '/dev/sda1',
                'Ebs': {
                    'DeleteOnTermination': True,
                    'VolumeSize': size
                }
            },
        ],
        TagSpecifications=[{'ResourceType': 'instance',
                            'Tags': [{"Key": "Name", "Value": name}]}],
        IamInstanceProfile={'Name': iam}
    )
    return instance


def read_dict_from_s3_url(url):
    url_object = urlparse(url, allow_fragments=False)
    return read_dict_from_s3(bucket=url_object.netloc, key=url_object.path.lstrip('/'))


def read_dict_from_s3(bucket, key):
    s3 = boto3.resource('s3')
    content_object = s3.Object(bucket, key)
    file_content = content_object.get()['Body'].read().decode('utf-8')
    return json.loads(file_content)


class AthenaLogger:
    __CREATE_ATHENA_TABLE = """
    CREATE EXTERNAL TABLE log (
       created_at timestamp,
       level_name string,
       module string,
       function_name string,
       message string
    )
    PARTITIONED BY (app_name string, creation_date string, machine string)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
       'separatorChar' = ',',
       'quoteChar' = '"',
       'escapeChar' = '\\\\'
       )
    STORED AS TEXTFILE
    LOCATION 's3://{s3_location}/log/';
    """

    class __CSVFormatter(logging.Formatter):
        def __init__(self):
            super().__init__()
            self.output = io.StringIO()
            self.writer = csv.writer(self.output, quoting=csv.QUOTE_ALL, escapechar='\\', quotechar='"',
                                     doublequote=False)

        def format(self, record):
            message = record.msg % record.args

            if record.exc_info is not None:
                message = "Message: {message} - Type: {type} - Value: {value} - Traceback: {traceback}".format(
                    message=message,
                    type=str(record.exc_info[0]),
                    value=str(record.exc_info[1]),
                    traceback=traceback.format_tb(record.exc_info[2], 10)
                    )

            text = ' '.join(message.split())
            text = text.replace('\\', '\\\\')
            self.writer.writerow([str(datetime.now().timestamp()).replace('.', '')[0:13],
                                  record.levelname, record.module, record.funcName, text])
            data = self.output.getvalue()
            self.output.truncate(0)
            self.output.seek(0)
            return data.strip()

    def __init__(self, app_name, s3_bucket, athena_db):
        self.app_name = app_name
        self.s3_bucket = s3_bucket
        self.athena_db = athena_db
        self.local_filename = Path(Path(__file__).parent,
                                   'tmp',
                                   '{app_name}-{timestamp}.log'.format(
                                       app_name=app_name, timestamp=datetime.utcnow().strftime("%Y%m%d-%H%M%S")))
        try:
            response = requests.get('http://169.254.169.254/latest/meta-data/instance-id')
            self.machine = response.text
        except requests.exceptions.ConnectionError:
            self.machine = "dev_machine"
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        Path(self.local_filename).parent.mkdir(parents=True, exist_ok=True)
        log_file = logging.FileHandler(self.local_filename, mode="w")
        formatter_csv = self.__CSVFormatter()
        log_file.setFormatter(formatter_csv)
        logger.addHandler(log_file)
        sys.excepthook = self.__exception_logging
        self.root_logger = True

    @staticmethod
    def __exception_logging(exc_type, exc_value, exc_traceback):
        logging.exception("Unhandled exception.", exc_info=(exc_type, exc_value, exc_traceback))

    def save_to_s3(self):
        s3_filename = "log/app_name={app_name}/creation_date={date}/machine={machine}/{timestamp}.csv".format(
            app_name=self.app_name,
            date=datetime.utcnow().strftime("%Y-%m-%d"),
            machine=self.machine,
            timestamp=datetime.utcnow().strftime("%Y%m%d-%H%M%S"))
        s3 = boto3.resource('s3')
        s3.Bucket(self.s3_bucket).upload_file(str(self.local_filename), s3_filename)

    def recreate_athena_table(self):
        athena = AthenaDatabase(s3_output=self.s3_bucket, database=self.athena_db)
        athena.query_athena_and_wait(query_string='DROP TABLE if exists log')
        athena.query_athena_and_wait(query_string=self.__CREATE_ATHENA_TABLE.format(s3_location=self.s3_bucket))
        athena.query_athena_and_wait(query_string="MSCK REPAIR TABLE log")


class AthenaDatabase:
    ATHENA_TIMEOUT = 120
    MAX_ATHENA_ERRORS = 5

    def __init__(self, database, s3_output):
        self.athena = boto3.client('athena')
        self.database = database
        self.s3_output = s3_output
        self.s3_output_prefix = "tmp/athena/{}".format(uuid.uuid4().hex)
        self.s3_output_full_path = "s3://{}/{}/".format(self.s3_output, self.s3_output_prefix)
        logging.info("Athena output: %s", self.s3_output_full_path)
        self.athena_failures = 0

    def query_athena(self, query_string):
        logging.info("Query to Athena database '%s'. Query string: %s", self.database, query_string)
        execution = self.athena.start_query_execution(
            QueryString=query_string,
            QueryExecutionContext={'Database': self.database},
            ResultConfiguration={'OutputLocation': self.s3_output_full_path})
        execution_id = execution['QueryExecutionId']
        logging.info("Execution ID: %s", execution_id)
        return execution_id

    def query_athena_and_wait(self, query_string, delete_results=True):
        execution_id = self.query_athena(query_string)

        # Wait until query ends or timeout
        state = 'RUNNING'
        elapsed_time = 0
        response = None
        while elapsed_time <= self.ATHENA_TIMEOUT and state in ['RUNNING']:
            elapsed_time = elapsed_time + 1
            response = self.athena.get_query_execution(QueryExecutionId=execution_id)
            state = response.get('QueryExecution', {}).get('Status', {}).get('State')
            if state not in ['SUCCEEDED', 'FAILED']:
                logging.info("Waiting for response: sleep for 1 second")
                time.sleep(1)

        # if timeout or failed
        if state != 'SUCCEEDED':
            self.athena_failures = self.athena_failures + 1
            logging.error("Error executing query. Athena failure: '%d', Current state: '%s', Response: %s",
                          self.athena_failures, state, json.dumps(response, default=self.__default))
            assert self.athena_failures <= self.MAX_ATHENA_ERRORS,\
                "Exceeded max number of consecutive Athena errors (%d errors): terminate".format(self.MAX_ATHENA_ERRORS)
            logging.info("Wait five seconds before trying the same Athena query again")
            time.sleep(5)
            return self.query_athena_and_wait(query_string)
        else:
            self.athena_failures = 0
            logging.info("Query succeeded: %s", json.dumps(response, default=self.__default))
            if delete_results:
                # delete result files on S3 (just a log of the previous commands)
                logging.info('Delete result file on S3 for commands')
                s3 = boto3.resource('s3')
                s3.Bucket(self.s3_output).objects.filter(Prefix=self.s3_output_prefix).delete()
                return None
            else:
                s3_path = response['QueryExecution']['ResultConfiguration']['OutputLocation']
                # obtain file name
                filename_s3 = re.findall('.*/(.*)', s3_path)[0]
                logging.info("Filename on S3: %s", filename_s3)
                return filename_s3

    def query_athena_and_download(self, query_string, filename):
        filename_s3 = self.query_athena_and_wait(query_string, delete_results=False)
        filepath_s3 = "{}/{}".format(self.s3_output_prefix, filename_s3)
        local_filepath = Path(Path(__file__).parent, 'tmp', filename)
        Path(local_filepath).parent.mkdir(parents=True, exist_ok=True)
        logging.info("Download file '%s' from bucket %s. Local path: '%s'",
                     filepath_s3, self.s3_output, str(local_filepath))
        s3 = boto3.resource('s3')
        s3.Bucket(self.s3_output).download_file(filepath_s3, str(local_filepath))
        logging.info("Clean all files on bucket %s at prefix %s", self.s3_output, self.s3_output_prefix)
        s3.Bucket(self.s3_output).objects.filter(Prefix=self.s3_output_prefix).delete()
        return str(local_filepath)

    @staticmethod
    def __default(obj):
        if isinstance(obj, datetime):
            return {'_isoformat': obj.isoformat()}
        return super().default(obj)
