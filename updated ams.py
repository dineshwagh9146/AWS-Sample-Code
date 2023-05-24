import configparser
import logging
import logging.config
from time import strptime
import requests
import mysql.connector as mysqldb
import sys
import pandas as pd
import datetime
import gzip
import json
import boto3
from boto3.dynamodb.conditions import Key
from datetime import date, datetime, timezone, timedelta
import pytz
import time

configParser = configparser.RawConfigParser()


def encloseQuotes(constr):
    return "'" + constr + "'"


def closeDB():
    global dbConnection

    if (dbConnection):
        dbConnection.close()
    dbConnection = None


"""
This method is used to sends SNS Alerts when a method throws exception or when the job contains invalid or no data.
"""

"""
def sns_topic(err, sns_arn):
    try:
        print("triggring sns topic")
        sns = boto3.client('sns', region_name="us-east-1")
        sns.publish(TopicArn=sns_arn, Subject='NR Instrumentation Failed at {} '.format(
            datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
                    Message='NR Instrumentation failed with ' + str(err))
        print("Data is send to SNS topic")
    except Exception as e:
        print("error in sending message to sns topic ", str(e))
"""

"""
Load properties from config file
"""


def initializeConfig(configFile):
    global smtpObject
    global emailMessage
    global fromEmail
    global toEmail
    global configParser
    global logFileSize
    global logFilePath
    global apiKey
    global endPoint
    global sns_arn

    try:
        print("In initializeConfig")
        configParser = configparser.RawConfigParser()
        configParser.read(configFile)
        sns_arn = configParser.get('sns', 'sns_arn')
        logFileSize = configParser.get('Global', 'log_file_size')
        logFilePath = configParser.get('Global', 'log_file_path')
        apiKey = configParser.get('newrelic', 'api_key')
        endPoint = configParser.get('newrelic', 'end_point')
    except Exception as e:
        print("Exception in initializeConfig" + str(e))
        sns_topic(e, sns_arn)
        sys.exit(1)
    finally:
        print("initializeConfig completed")


"""
        Create a log file
"""


def getLogger():
    global oLogger

    # Set up a specific logger with our desired output level
    oLogger = logging.getLogger("ams_stats")
    oLogger.setLevel(logging.DEBUG)

    # Add the log message handler to the logger - initial max is 1mb per log file
    handler = logging.handlers.RotatingFileHandler(logFilePath, maxBytes=int(logFileSize), backupCount=1)
    format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(format)
    oLogger.addHandler(handler)


"""
Connection to db with given connection details, and get db connection.
"""


def establishDBConnection():
    global dbConnection
    global dbConnectiongg
    try:
        oLogger.info("In establishDBConnection")  # if configParser.get("Global", 'db_type') == "oracle":
        dbConnectString = configParser.get("mysql", 'connect_string')
        dbUser = configParser.get("mysql", 'db_user')
        dbPasswd = configParser.get("mysql", 'db_passwd')
        oLogger.debug("DBUser: " + dbUser + ",connectString: " + dbConnectString)
        dbConnection = mysqldb.connect(user=dbUser, password=dbPasswd, host=dbConnectString)
        oLogger.debug("DB connection successful")
    except Exception as e:
        oLogger.error("Exception in establishDBConnection: " + str(e))
        sns_topic(e, sns_arn)
        sys.exit(1)
    finally:
        oLogger.info("establishDBConnection completed")


"""
This method is to fetch records from the DB
"""


def fetchRecords(sql, dbConnection):
    try:
        oLogger.info("In fetchRecords")
        resultDF = pd.read_sql(sql, dbConnection)
        return resultDF
    except Exception as e:
        oLogger.error("Exception in fetchRecords" + str(e))
        sns_topic(e, sns_arn)
        closeDB()
        sys.exit(1)
    finally:
        oLogger.info("fetchRecords completed")


def retrieveCountStats(retrieveSQL):
    global df_file_log_count
    try:
        oLogger.info("In retrieveCountStats")
        df_file_log_count = fetchRecords(retrieveSQL, dbConnection)
        return df_file_log_count
    except Exception as e:
        oLogger.error("Exception in retrieveCountStats" + str(e))
        sns_topic(e, sns_arn)
        sys.exit(1)
    finally:
        oLogger.info("retrieveCountStats completed")


"""
This method is to get the count of archived file count and timestamp of newest file
"""


def get_archived_file_count_and_newest_file_time():
    env_var_eds_s3_output = "s3://aws-athena-query-results-124674640595-us-east-1"
    today_dt = datetime.today().strftime('%Y-%m-%d')
    athena_query_1 = """select count(distinct "dynamodb.newimage.tar_file_name.s") as sucess_file_count,
    max("dynamodb.newimage.process_end_time.s") as time_of_newest_file from "ams"."ams_streams" where load_date = '"""
    athena_query_2 = """' and "dynamodb.newimage.tar_file_name.s" like '%tar.gz' and
     "dynamodb.newimage.process_status.s" = 'SUCCESS' """
    athena_query = athena_query_1 + today_dt + athena_query_2
    athena_client = boto3.client('athena')
    response = athena_client.start_query_execution(QueryString=athena_query,
                                                   ResultConfiguration={'OutputLocation': env_var_eds_s3_output})
    # Retrieving the ID attached to Athena execution request
    queryid = response['QueryExecutionId']
    status = athena_client.get_query_execution(QueryExecutionId=queryid)['QueryExecution']['Status']['State']
    time.sleep(30)
    while status.upper() in ['QUEUED', 'RUNNING']:
        status = athena_client.get_query_execution(QueryExecutionId=queryid)['QueryExecution']['Status']['State']
    if status.upper() == 'SUCCEEDED':
        oLogger.info(f"Query Id : {queryid}, STATUS : {status}")
    elif status.upper() == 'FAILED':
        error_msg = athena_client.get_query_execution(QueryExecutionId=queryid)['QueryExecution']['Status']
        oLogger.info(status)
        oLogger.error(error_msg)
    query_results = athena_client.get_query_results(QueryExecutionId=queryid)
    num_of_archived_file = (query_results['ResultSet']['Rows'][1]['Data'][0]['VarCharValue'])
    date_of_newest_file = (query_results['ResultSet']['Rows'][1]['Data'][1]['VarCharValue'])
    now_utc = datetime.now(timezone.utc)
    time_diff = now_utc - datetime.strptime(date_of_newest_file, '%Y-%m-%d %H:%M:%S.%f').replace(tzinfo=timezone.utc)
    time_diff_in_sec = int(time_diff.total_seconds())
    return date_of_newest_file, num_of_archived_file, time_diff_in_sec


# fetching load_date, load_hr, cnt from athena

def get_result_athena():
    env_var_eds_s3_output = "s3://aws-athena-query-results-124674640595-us-east-1"
    yesterday_dt = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    athena_query_1 = """select load_date, load_hr, cnt from (SELECT load_date, load_hr, 
    count(distinct "dynamodb.newimage.tar_file_name.s") as cnt, row_number() over (order by load_date desc,
    load_hr desc) as row_num FROM ams.ams_streams where load_date>= '"""
    athena_query_2 = """' and "dynamodb.newimage.process_status.s" = 'SUCCESS' and "dynamodb.keys.ams_audit_log_key.s"
    like '%tar.gz' group by load_date,load_hr order by load_date desc, load_hr desc) where row_num= 2 """
    athena_query = athena_query_1 + yesterday_dt + athena_query_2
    athena_client = boto3.client('athena')
    response = athena_client.start_query_execution(QueryString=athena_query,
                                                   ResultConfiguration={'OutputLocation': env_var_eds_s3_output})
    # Retrieving the ID attached to Athena execution request
    queryid = response['QueryExecutionId']
    status = athena_client.get_query_execution(QueryExecutionId=queryid)['QueryExecution']['Status']['State']
    time.sleep(80)
    while status.upper() in ['QUEUED', 'RUNNING']:
        status = athena_client.get_query_execution(QueryExecutionId=queryid)['QueryExecution']['Status']['State']
    if status.upper() == 'SUCCEEDED':
        oLogger.info(f"Query Id : {queryid}, STATUS : {status}")
    elif status.upper() == 'FAILED':
        error_msg = athena_client.get_query_execution(QueryExecutionId=queryid)['QueryExecution']['Status']
        oLogger.info(status)
        oLogger.error(error_msg)
    query_results = athena_client.get_query_results(QueryExecutionId=queryid)
    column_values = []
    for row in query_results['ResultSet']['Rows'][1:]:
        column_values.append([data['VarCharValue'] for data in row['Data']])

    load_date = column_values[0][0]
    load_hr = column_values[0][1]
    cnt = column_values[0][2]

    return load_date, load_hr, cnt


"""
This method is to push dataframe to NewRelic
"""


def pushToNewRelic():
    try:
        oLogger.info("In pushToNewRelic")
        header = {'Content-Type': 'application/json', 'X-Insert-Key': apiKey}
        payload['eventType'] = configParser.get("newrelic", 'event_type_ams')
        payload['date_of_newest_file'] = date_of_newest_file
        payload['success_file_count'] = num_of_archived_file
        payload['time_diff_in_sec'] = time_diff_in_sec
        payload['load_date'] = load_date
        payload['load_hr'] = load_hr
        payload['Count'] = cnt
        jsonString = payload.to_json(orient='records')
        print(jsonString)
        # r = requests.post(url=endPoint, data=jsonString, headers=header)
    except Exception as e:
        oLogger.error("Exception in pushToNewRelic" + str(e))
        sns_topic(e, sns_arn)
    finally:
        oLogger.info("pushToNewRelic completed")


def main():
    global payload
    global date_of_newest_file
    global num_of_archived_file
    global time_diff_in_sec
    global load_date, load_hr, cnt
    initializeConfig(sys.argv[1])
    getLogger()
    establishDBConnection()
    retrieve_SQL_format = configParser.get("mysql", 'retrieve_file_log_sql_format')
    retrieve_SQL_appl = configParser.get("mysql", 'retrieve_file_log_sql_appl')
    fail_log_format = retrieveCountStats(retrieve_SQL_format)
    fail_log_appl = retrieveCountStats(retrieve_SQL_appl)
    payload = pd.concat([fail_log_format, fail_log_appl], axis=1)
    date_of_newest_file, num_of_archived_file, time_diff_in_sec = get_archived_file_count_and_newest_file_time()
    load_date, load_hr, cnt = get_result_athena()
    pushToNewRelic()
    closeDB()


if __name__ == '__main__':
    main()
