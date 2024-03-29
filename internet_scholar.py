import json
import boto3
import botocore
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
import os
from urllib.parse import urlparse
import urllib3
import sqlite3
from collections import OrderedDict
import shutil


def decompress(filename, delete_original=True):
    new_filepath = filename[:-4]
    with bz2.BZ2File(filename, 'rb') as input_file:
        with open(new_filepath, 'wb') as output_file:
            copyfileobj(input_file, output_file)
    if delete_original:
        os.remove(filename)
    return new_filepath


def compress(filename, delete_original=True, compress_level=9):
    filename_bz2 = Path(Path(__file__).parent, 'tmp', "{}.bz2".format(os.path.basename(filename)))
    logging.info("Compress file %s. New file: %s. Compression level: %d. Delete original? %s",
                 filename, filename_bz2, compress_level, delete_original)
    with open(filename, 'rb') as input_file:
        with bz2.BZ2File(filename_bz2, 'wb', compresslevel=compress_level) as output_file:
            copyfileobj(input_file, output_file)
    if delete_original:
        os.remove(filename)
    return filename_bz2


BASE_SCRIPT_NEW_INSTANCE = """#!/bin/bash
cd /home/ubuntu
su ubuntu -c 'mkdir .aws'
su ubuntu -c 'printf "[default]\\nregion={region}" > /home/ubuntu/.aws/config'
su ubuntu -c 'wget {init_script} -O {new_name}'
su ubuntu -c 'chmod +x {new_name}'
su ubuntu -c "echo '/home/ubuntu/{new_name} {parameters}' > call.txt"
su ubuntu -c 'sudo apt-get update'
su ubuntu -c 'sudo apt-get install -y screen'
su ubuntu -c "screen -dmS internet_scholar sh -c '/home/ubuntu/{new_name} {parameters} 2>&1 | tee output.txt; exec bash'"
"""


def instantiate_ec2(key_name, security_group, iam, init_script,
                    ami="ami-0ca5c3bd5a268e7db", instance_type="t3a.nano",
                    size=8, parameters="", region="us-west-2", name="internet_scholar"):
    ec2 = boto3.resource('ec2')
    new_name = f"{uuid.uuid4()}.sh"
    instance = ec2.create_instances(
        ImageId=ami,
        InstanceType=instance_type,
        MinCount=1,
        MaxCount=1,
        KeyName=key_name,
        InstanceInitiatedShutdownBehavior='terminate',
        UserData=BASE_SCRIPT_NEW_INSTANCE.format(region=region, init_script=init_script,
                                                 new_name=new_name, parameters=parameters),
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


def s3_prefix_exists(bucket, prefix):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket)
    return bool(list(bucket.objects.filter(Prefix=prefix)))


def delete_s3_objects_by_prefix(bucket, prefix):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket)
    bucket.objects.filter(Prefix=prefix).delete()


def s3_key_exists(bucket, key):
    s3 = boto3.resource('s3')
    try:
        s3.Object(bucket, key).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        else:
            raise
    else:
        return True


def s3_file_size_in_bytes(bucket, key):
    s3 = boto3.resource('s3')
    object = s3.Object(bucket, key)
    file_size = 0
    try:
        file_size = object.content_length
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            file_size = 0
        else:
            raise
    return file_size


def save_string_local_file(filename, content):
    with open(filename, 'w', encoding="utf-8") as content_file:
        content_file.write(content)


def read_dict_from_s3_url(url, compressed = False):
    url_object = urlparse(url, allow_fragments=False)
    return read_dict_from_s3(bucket=url_object.netloc, key=url_object.path.lstrip('/'), compressed=compressed)


def read_dict_from_s3(bucket, key, compressed = False):
    s3 = boto3.resource('s3')
    try:
        content_object = s3.Object(bucket, key)
        file_content = content_object.get()['Body'].read()
        if compressed:
            file_content = bz2.decompress(file_content)
        return json.loads(file_content.decode('utf-8'))
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "NoSuchKey":
            return None
        else:
            raise


def read_dict_from_url(url):
    page = requests.get(url)
    return json.loads(page.text)


def save_data_in_s3(content, s3_bucket, s3_key, prefix=None, partitions=None, compress_file=True):
    if partitions is not None:
        if not isinstance(partitions, OrderedDict):
            raise TypeError("partitions must be an OrderedDict")
        partition_string = ""
        for key, value in partitions.items():
            partition_string = partition_string + f"{key}={value}/"

    temp_dir = uuid.uuid4()
    Path(f'./{temp_dir}/').mkdir(parents=True, exist_ok=True)
    try:
        filename = f"./{temp_dir}/{s3_key.strip('.bz2')}"
        if (isinstance(content, list)):
            with open(filename, 'w', encoding="utf-8") as csv_file:
                fieldnames = list(content[0].keys())
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames, dialect='unix')
                for record in content:
                    writer.writerow(record)
        elif (isinstance(content, dict)):
            json_string = json.dumps(content)
            with open(filename, 'w', encoding="utf-8") as json_file:
                json_file.write(json_string)
        else:
            raise TypeError("content must be dict or list")
        if compress_file:
            filename = str(compress(filename))
        s3_path = ""
        if prefix is not None:
            s3_path = prefix + "/"
        if partitions is not None:
            s3_path = s3_path + partition_string
        s3_path = s3_path + os.path.basename(filename)
        s3 = boto3.resource('s3')
        s3.Bucket(s3_bucket).upload_file(filename, s3_path)
    finally:
        shutil.rmtree(f"./{temp_dir}")


def move_data_in_s3(bucket_name, origin, destination):
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket_name, destination).copy_from(CopySource= {'Bucket': bucket_name, 'Key': origin})
    s3_resource.Object(bucket_name, origin).delete()


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
            response = requests.get('http://169.254.169.254/latest/meta-data/instance-id', timeout=3)
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
    ATHENA_TIMEOUT = 300
    MAX_ATHENA_ERRORS = 5

    def __init__(self, database, s3_output):
        s3_client = boto3.client('s3')
        s3_region_response = s3_client.get_bucket_location(Bucket=s3_output)
        if s3_region_response is None:
            logging.info(f"Bucket {s3_output} probably does not exist or user is not allowed to access it.")
            self.athena = boto3.client('athena')
        else:
            s3_region = s3_region_response.get('LocationConstraint', '')
            if s3_region == '':
                logging.info(f"Athena region {s3_region} is an empty string.")
                self.athena = boto3.client('athena')
            else:
                self.athena = boto3.client('athena', region_name=s3_region)
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
        while elapsed_time <= self.ATHENA_TIMEOUT and state in ['RUNNING', 'QUEUED']:
            if state in ['RUNNING']:
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
            return self.query_athena_and_wait(query_string, delete_results=delete_results)
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

    def query_athena_and_get_result(self, query_string):
        filename_s3 = self.query_athena_and_wait(query_string, delete_results=False)
        s3 = boto3.resource('s3')
        filepath_s3 = "{}/{}".format(self.s3_output_prefix, filename_s3)
        content_object = s3.Object(self.s3_output, filepath_s3)
        file_content = content_object.get()['Body'].read().decode('utf-8')
        logging.info("Clean all files on bucket %s at prefix %s", self.s3_output, self.s3_output_prefix)
        s3.Bucket(self.s3_output).objects.filter(Prefix=self.s3_output_prefix).delete()
        memory_file = io.StringIO(file_content)
        reader = csv.DictReader(memory_file)
        rows = list(reader)
        assert len(rows) == 1, "Invalid operation: query to Athena resulted %d rows" % len(rows)
        return rows[0]

    def table_exists(self, table_name):
        filename_s3 = self.query_athena_and_wait(
            query_string="show tables in {database} '{table_name}'".format(
                database=self.database,
                table_name=table_name),
            delete_results=False)
        s3 = boto3.resource('s3')
        filepath_s3 = "{}/{}".format(self.s3_output_prefix, filename_s3)
        content_object = s3.Object(self.s3_output, filepath_s3)
        if content_object.content_length == 0:
            exist = False
        else:
            exist = True
        logging.info("Clean all files on bucket %s at prefix %s", self.s3_output, self.s3_output_prefix)
        s3.Bucket(self.s3_output).objects.filter(Prefix=self.s3_output_prefix).delete()
        return exist

    @staticmethod
    def __default(obj):
        if isinstance(obj, datetime):
            return {'_isoformat': obj.isoformat()}
        return super().default(obj)


class URLExpander:
    NOT_HTTP_HTTPS = 601
    EXCEPTION_DURING_ACCESS = 600

    def __init__(self, log_exceptions=True,
                 user_agent='Mozilla/5.0 (Windows NT 6.3; rv:36.0) Gecko/20100101 Firefox/36.0'):
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self.user_agent = {'User-Agent': user_agent}
        self.log_exceptions = log_exceptions

    def expand_url(self, url):
        expanded_url = []
        if urlparse(url).scheme not in ['https', 'http']:
            record = {'url': url,
                      'validated_url': url,
                      'status_code': self.NOT_HTTP_HTTPS,
                      'content_length': 0,
                      'created_at': str(datetime.now().timestamp()).replace('.', '')[0:13]
                      }
            expanded_url.append(record)
        else:
            try:
                r = requests.head(url, headers=self.user_agent,
                                  allow_redirects=True, verify=False, timeout=15)
            except Exception as e:
                if self.log_exceptions:
                    logging.exception("Exception for %s", url)
                record = {'url': url,
                          'validated_url': url,
                          'status_code': self.EXCEPTION_DURING_ACCESS,
                          'content_length': 0,
                          'created_at': str(datetime.now().timestamp()).replace('.', '')[0:13]
                          }
                expanded_url.append(record)
            else:
                try:
                    content_length = int(r.headers.get('content-length', "0").strip())
                except ValueError:
                    content_length = 0
                record = {'url': r.url,
                          'validated_url': r.url,
                          'status_code': r.status_code,
                          'content_type': r.headers.get('content-type', ''),
                          'content_length': content_length,
                          'created_at': str(datetime.now().timestamp()).replace('.', '')[0:13]
                          }
                expanded_url.append(record)

                if len(r.history) != 0:
                    for history_element in r.history:
                        try:
                            content_length = int(history_element.headers.get('content-length', "0").strip())
                        except ValueError:
                            content_length = 0
                        record = {'url': history_element.url,
                                  'validated_url': r.url,
                                  'status_code': history_element.status_code,
                                  'content_type': history_element.headers.get('content-type', ''),
                                  'content_length': content_length,
                                  'created_at': str(datetime.now().timestamp()).replace('.', '')[0:13]
                                  }
                        expanded_url.append(record)
        return expanded_url


class SqliteAWS:
    def __init__(self, database, s3_admin, s3_data, athena_db):
        self.database = database
        self.athena_db = athena_db
        self.s3_admin = s3_admin
        self.s3_data = s3_data

    def convert_s3_csv_to_sqlite(self, s3_path):
        s3 = boto3.resource('s3')
        compressed_file = 's3_file.csv.bz2'
        s3.Bucket(self.s3_data).download_file(s3_path, compressed_file)
        temporary_file = decompress(filename=compressed_file)
        table_name = s3_path.split('/')[0]
        self.convert_csv_to_sqlite(table_name=table_name, csv_path=temporary_file)

    def convert_sqlite_to_s3_csv(self, s3_path, order_by=None):
        table_name = s3_path.split('/')[0]
        self.database.row_factory = sqlite3.Row
        query = 'SELECT * FROM {table_name}'.format(table_name=table_name)
        if order_by is not None:
            query = '{query} order by {order_by}'.format(query=query, order_by=order_by)
        with open('./temp_file.csv', 'w') as csv_writer:
            first_record = True
            cursor_select = self.database.execute(query)
            for record in cursor_select:
                if first_record:
                    writer = csv.DictWriter(
                        csv_writer,
                        fieldnames=record.keys(),
                        dialect='unix'
                    )
                    writer.writeheader()
                    first_record = False
                record_csv = dict()
                for key in record.keys():
                    record_csv[key] = record[key]
                writer.writerow(record_csv)
        compressed_file = compress('./temp_file.csv')
        s3 = boto3.resource('s3')
        s3.Bucket(self.s3_data).upload_file(str(compressed_file), s3_path)
        os.remove(str(compressed_file))

    def convert_csv_to_sqlite(self, table_name, csv_path, delete_csv=True):
        with open(csv_path, newline='', encoding="utf8") as csv_file:
            csv_reader = csv.DictReader(csv_file)
            field_names = None
            insert_sql = ''
            for record in csv_reader:
                if field_names is None:
                    field_names = record.keys()
                    create_sql = 'CREATE TABLE {table_name} ' \
                                 '({field_names})'.format(table_name=table_name,
                                                          field_names=', '.join([i + ' TEXT' for i in field_names]))
                    self.database.execute(create_sql)
                    insert_sql = 'INSERT INTO {table_name} ' \
                                 '({field_names}) values ' \
                                 '({question_marks})'.format(table_name=table_name,
                                                             field_names=', '.join(field_names),
                                                             question_marks=', '.join(['?' for i in field_names]))
                self.database.execute(insert_sql, tuple(record.values()))
        self.database.commit()
        if delete_csv:
            os.remove(csv_path)

    def convert_athena_query_to_sqlite(self, table_name, query):
        athena = AthenaDatabase(database=self.athena_db, s3_output=self.s3_admin)
        query_result = athena.query_athena_and_download(query_string=query, filename='query_result.csv')
        self.convert_csv_to_sqlite(table_name=table_name, csv_path=query_result)