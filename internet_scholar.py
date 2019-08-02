import logging
import inspect
import os
import sys
import boto3
import time
import json
import re
import bz2
from shutil import copyfileobj
from datetime import datetime
import subprocess
import csv
import io


athena_create_table_log = """
CREATE EXTERNAL TABLE log (
   created_at timestamp,
   level_name string,
   module string,
   function_name string,
   message string
)
PARTITIONED BY (name string, creation_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   'separatorChar' = ',',
   'quoteChar' = '"',
   'escapeChar' = '\\\\'
   )
STORED AS TEXTFILE
LOCATION 's3://{s3_raw}/log/';
"""


class InternetScholar:
    ATHENA_TIMEOUT = 120
    MAX_ATHENA_ERRORS = 5

    def __init__(self, config_bucket, location=None, logger_name=None, prod=False, log_file=False):
        if location is not None:
            self.location = location
        else:
            # if prefix was not specified
            self.location = os.path.splitext(os.path.basename(inspect.getfile(type(self))))[0]
        if logger_name is not None:
            self.logger_name = logger_name
        else:
            self.logger_name = self.location

        # create folder and files in relation to python file that is top-level code
        self.local_path = os.path.dirname(sys.modules["__main__"].__file__)

        # Configure logging module to save on log file and present messages on the screen too
        self.logger = logging.getLogger(self.logger_name)
        self.logger.setLevel(logging.DEBUG)
        self.log_file = log_file
        if self.log_file:
            log_directory = os.path.join(self.local_path, 'logs')
            os.makedirs(log_directory, exist_ok=True)
            self.log_filename = os.path.join(log_directory, '{}.log'.format(self.logger_name))
            log_file = logging.FileHandler(self.log_filename, mode="w")
            formatter_csv = CSVFormatter()
            log_file.setFormatter(formatter_csv)
            self.logger.addHandler(log_file)
        else:
            console = logging.StreamHandler()
            console.setLevel(logging.DEBUG)
            formatter_screen = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            console.setFormatter(formatter_screen)
            self.logger.addHandler(console)

        self.logger.info("Create resource to access S3")
        self.s3 = boto3.resource('s3')

        self.config_bucket = config_bucket

        self.prod = prod
        if self.prod:
            self.logger.info("Read configuration file for production from %s", self.config_bucket)
            self.config = self.read_dict_from_s3(bucket=self.config_bucket, key='prod.json')
        else:
            self.logger.info("Read configuration file for development from %s", self.config_bucket)
            self.config = self.read_dict_from_s3(bucket=self.config_bucket, key='dev.json')

        self.logger.info("Read credentials from %s", self.config_bucket)
        self.credentials = self.read_dict_from_s3(bucket=self.config_bucket, key='credentials.json')

        self.logger.info('Create client for Athena')
        self.athena = boto3.client('athena')

        self.temp_dir = os.path.join(self.local_path, 'temp')
        self.logger.info('Directory for temporary files: %s', self.temp_dir)
        os.makedirs(self.temp_dir, exist_ok=True)

        # copy content of config file to variables just to make code more legible
        self.s3_temp = "s3://{}/{}/".format(self.config['s3']['temp'], self.location)
        self.s3_columnar = "s3://{}/{}/".format(self.config['s3']['columnar'], self.location)
        self.s3_raw = "s3://{}/{}/".format(self.config['s3']['raw'], self.location)

        # variable that manages the number of athena failures that are tolerable
        self.athena_failures = 0

    def recreate_log_table_on_athena(self):
        self.logger.info("Drop log table")
        self.query_athena_and_wait(query_string='DROP TABLE log')
        self.logger.info("Recreate log table on %s", self.config['s3']['raw'])
        self.query_athena_and_wait(query_string=athena_create_table_log.format(s3_raw=self.config['s3']['raw']))
        self.logger.info("Redo partitions for log table")
        self.query_athena_and_wait(query_string="MSCK REPAIR TABLE log")

    def read_dict_from_s3(self, bucket, key):
        content_object = self.s3.Object(bucket, key)
        file_content = content_object.get()['Body'].read().decode('utf-8')
        return json.loads(file_content)

    def save_logs(self, recreate_log_table=True):
        if self.log_file:
            s3_filename = "log/name={}/creation_date={}/{}.csv".format(self.logger_name,
                                                                       datetime.utcnow().strftime("%Y-%m-%d"),
                                                                       datetime.utcnow().strftime("%Y%m%d-%H%M%S"))
            self.logger.info("Save log file %s on bucket %s at %s and clear it locally",
                             self.log_filename, self.config['s3']['raw'], s3_filename)
            self.s3.Bucket(self.config['s3']['raw']).upload_file(self.log_filename, s3_filename)
            # clear log file
            with open(self.log_filename, 'w'):
                pass
            if recreate_log_table:
                self.recreate_log_table_on_athena()

    def query_athena(self, query_string):
        self.logger.info("Query to Athena database '%s'. Query string: %s", self.config['athena']['database'], query_string)
        execution = self.athena.start_query_execution(
            QueryString=query_string,
            QueryExecutionContext={'Database': self.config['athena']['database']},
            ResultConfiguration={'OutputLocation': self.s3_temp})
        execution_id = execution['QueryExecutionId']
        self.logger.info("Execution ID: %s", execution_id)
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
                self.logger.info("Waiting for response: sleep for 1 second")
                time.sleep(1)

        # if timeout or failed
        if state != 'SUCCEEDED':
            self.athena_failures = self.athena_failures + 1
            logging.error("Error executing query. Athena failure: '%d', Current state: '%s', Response: %s",
                          self.athena_failures, state, json.dumps(response, default=default))
            if self.athena_failures > self.MAX_ATHENA_ERRORS:
                self.logger.info("Excedeed max number of consecutive Athena errors (%d errors): terminate",
                                 self.MAX_ATHENA_ERRORS)
                raise Exception("Error executing query. Read log to see Athena's response.")
            else:
                self.logger.info("Wait five seconds before trying the same Athena query again")
                time.sleep(5)
                return self.query_athena_and_wait(query_string)
        else:
            self.athena_failures = 0
            self.logger.info("Query succeeded: %s", json.dumps(response, default=default))
            if delete_results:
                # delete result files on S3 (just a log of the previous commands)
                self.logger.info('Deleted result file on S3 for commands')
                self.s3.Bucket(self.config['s3']['temp']).objects.filter(Prefix="{}/".format(self.location)).delete()
            # obtain file name
            s3_path = response['QueryExecution']['ResultConfiguration']['OutputLocation']
            filename_s3 = re.findall('.*/(.*)', s3_path)[0]
            self.logger.info("Filename on S3: %s", filename_s3)
            return filename_s3

    def query_athena_and_download(self, query_string, filename):
        filename_s3 = self.query_athena_and_wait(query_string)
        filepath_s3 = "{}/{}".format(self.location, filename_s3)
        local_filepath = os.path.join(self.temp_dir, filename)
        self.logger.info("Download file '%s' from bucket %s. Local path: '%s'",
                         filepath_s3, self.config['s3']['temp'], local_filepath)
        self.s3.Bucket(self.config['s3']['temp']).download_file(filepath_s3, local_filepath)
        self.logger.info("Clean all files on bucket %s at prefix %s", self.config['s3']['temp'], self.location)
        self.s3.Bucket(self.config['s3']['temp']).objects.filter(Prefix="{}/".format(self.location)).delete()
        return local_filepath

    def compress(self, filename, delete_original=True, compress_level=9):
        filename_bz2 = "{}.bz2".format(filename)
        self.logger.info("Compress file %s. New file: %s. Compression level: %d. Delete original? %s",
                         filename, filename_bz2, compress_level, delete_original)
        with open(filename, 'rb') as input_file:
            with bz2.BZ2File(filename_bz2, 'wb', compresslevel=compress_level) as output_file:
                copyfileobj(input_file, output_file)
        if delete_original:
            os.remove(filename)
        return filename_bz2

    def generate_orc_file(self, filename_json, filename_orc, structure, delete_json=True):
        self.logger.info('Remove temporary ORC file if exists: %s', filename_orc)
        # if there is already an ORC file, delete it. Otherwise orc-tools will issue an error message
        try:
            os.remove(filename_orc)
        except OSError:
            self.logger.info('Temporary ORC file does not exist')
            pass
        subprocess.run(['java',
                        '-jar', os.path.join(self.local_path, 'utils/orc-tools-1.5.6-uber.jar'),
                        'convert', filename_json,
                        '-o', filename_orc,
                        '-s', "".join(structure.split())],
                       check=True)
        if delete_json:
            self.logger.info('Remove temporary JSON file: %s', filename_json)
            os.remove(filename_json)

    def upload_s3(self, bucket, local_filename, s3_filename, delete_original=True, partitions="", compress=True):
        if partitions == "":
            s3_full_filename = "{}/{}".format(self.location, s3_filename)
        else:
            partitions = partitions.strip('/')
            s3_full_filename = "{}/{}/{}".format(self.location, partitions, s3_filename)

        if compress:
            self.logger.info("Compress file %s before uploading.", local_filename)
            compressed_file = self.compress(local_filename, delete_original=delete_original)
            self.logger.info("Upload compressed file to bucket '%s'. Local filename: %s. "
                             "S3 filename: %s. Delete original? %s",
                             bucket, compressed_file, s3_full_filename, delete_original)
            self.s3.Bucket(bucket).upload_file(compressed_file, s3_full_filename)
            os.remove(compressed_file)
        else:
            self.logger.info("Upload file to bucket '%s'. Local filename: %s. S3 filename: %s. Delete original? %s",
                             bucket, local_filename, s3_full_filename, delete_original)
            self.s3.Bucket(bucket).upload_file(local_filename, s3_full_filename)
            if delete_original:
                os.remove(local_filename)

    def upload_raw_file(self, local_filename, s3_filename, delete_original=True, partitions="", compress=True):
        self.upload_s3(self.config['s3']['raw'], local_filename, s3_filename, delete_original, partitions, compress)

    def upload_temp_file(self, local_filename, s3_filename, delete_original=True, partitions="", compress=True):
        self.upload_s3(self.config['s3']['temp'], local_filename, s3_filename, delete_original, partitions, compress)

    def upload_columnar_file(self, local_filename, s3_filename, delete_original=True, partitions="", compress=True):
        self.upload_s3(self.config['s3']['columnar'],
                       local_filename, s3_filename, delete_original, partitions, compress)

    def instantiate_ec2(self, instance_type="t3a.nano",
                        size = 15, init_script="""#!/bin/bash\necho hi""", name="internet_scholar"):
        ec2 = boto3.resource('ec2')
        instance = ec2.create_instances(
            ImageId=self.credentials['aws']['ami'],
            InstanceType=instance_type,
            MinCount=1,
            MaxCount=1,
            KeyName=self.credentials['aws']['key_name'],
            InstanceInitiatedShutdownBehavior='terminate',
            UserData=init_script,
            SecurityGroupIds=[self.credentials['aws']['security_group']],
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
            IamInstanceProfile={'Name': self.credentials['aws']['iam']}
        )

    # @staticmethod
    # def prepare(text, *args, **kwargs):
    #     text = text.format(*args, **kwargs)
    #     text = ' '.join(text.split())
    #     return text


def default(obj):
    if isinstance(obj, datetime):
        return {'_isoformat': obj.isoformat()}
    return super().default(obj)


class CSVFormatter(logging.Formatter):
    def __init__(self):
        super().__init__()
        self.output = io.StringIO()
        self.writer = csv.writer(self.output, quoting=csv.QUOTE_ALL, escapechar='\\', quotechar='"',
                                 doublequote=False)

    def format(self, record):
        if record.exc_text is not None:
            text = record.exc_text.replace('\n', '\\\\n')
        else:
            message = record.msg % record.args
            text = ' '.join(message.split())
        self.writer.writerow([str(datetime.utcnow().timestamp()).replace('.', '')[0:13],
                              record.levelname, record.module, record.funcName, text])
        data = self.output.getvalue()
        self.output.truncate(0)
        self.output.seek(0)
        return data.strip()
