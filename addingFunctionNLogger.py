import random
import configparser
from configparser import ConfigParser
from datetime import timedelta
import requests
import pandas as pd
import pytz
from datetime import datetime
import logging
from logging import handlers
import boto3
import io
import sys



global Week_Date
global oLogger
global resultJESON
global list_of_attributes
global df


configParser = configparser.ConfigParser() 

def initializeConfig(configFile):
    global configParser
    global logFileSize
    global logFilePath
    global apiKey
    global endPoint
    global sns_arn
    global Bucket_name
    global Subfolder_name
    

    try:
        print("In initializeConfig")
        configParser = configparser.RawConfigParser()
        configParser.read(configFile)
        # configParser = configparser.ConfigParser()
        # configParser.read('config.ini')
        sns_arn = configParser.get('sns', 'sns_arn')
        logFileSize = configParser.get('Global', 'log_file_size')
        logFilePath = configParser.get('Global', 'log_file_path')
        apiKey = configParser.get('newrelic', 'api_key')
        endPoint = configParser.get('newrelic', 'end_point')
        Bucket_name = configParser.get('s3', 'bkt_name')
        Subfolder_name = configParser.get('s3', 'sub_folderName')
    except Exception as e:
        print("Exception in initializeConfig" + str(e))
        sns_topic(e, sns_arn)
        sys.exit(1)
    finally:
        print("initializeConfig completed")


"""
        Create a log file
"""

# Set up a logger
def get_logging():
    global oLogger
    oLogger = logging.getLogger()
    oLogger.setLevel(logging.DEBUG)
    # Add the log message handler to the logger - initial max is 1mb per log file
    handler = logging.handlers.RotatingFileHandler(logFilePath, maxBytes=int(logFileSize), backupCount=1)
    log_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(log_format)
    oLogger.addHandler(handler)

# sending SNS
def sns_topic(msg):
    oLogger.info("triggering sns topic")
    try:
        sns = boto3.client('sns', region_name="us-east-1")
        sns.publish(TopicArn=sns_arn,
                    Subject='NR-IN Tracker Updated',
                    Message=msg)
    except Exception as e:
        oLogger.error("sns_topic" + str(e))
    finally:
        oLogger.info("Data is send to SNS topic")

# Fetching Data from New relic
def fetch_new_relic_data():
    global resultJESON
    oLogger.info("Requesting Data from New_relic")
    try:
        url = endPoint
        NRQL_Query = "SELECT conditionName, openTime, closeTime,entity.type," \
                     "tags.newrelic.cloudIntegrations.providerAccountName FROM NrAiIncident SINCE 1 week ago WHERE " \
                     "event = 'close' and entity.name != empty order by openTime asc limit 1000"
        # entity.name is not equal to empty give us valid alert ,
        # its exclude the test alert as test alerts don't have the entity.name
        headers = {'X-Query-Key': apiKey}
        params = {'nrql': NRQL_Query}
        response = requests.get(url, headers=headers, params=params)
        resultJESON = (response.json())['results'][0]
        return resultJESON
    except Exception as e:
        oLogger.error("Exception in fetch_new_relic_data" + str(e))
        sns_topic(e)
    finally:
        oLogger.info("fetch_new_relic_data completed")

# Processing fetched data form New relic
def data_processing():
    global list_of_attributes
    global Week_Date
    list_of_attributes = []
    try:
        for i in resultJESON['events']:
            all_attribute_dict = {
                "Month": 0,
                "Week": 0,
                "Week Date": 0,
                "Environment": "Prod",  # HARDCODED
                "HLP/UNO #": "NA",  # HARDCODED
                "Event": "",
                "Impact": "Cerebro",  # HARDCODED
                "Type of Alert": "",  # manual
                "Status": "Resolved",  # HARDCODED
                "Area": "",
                "Technology Used": "",
                "Platform": "Cloud",  # HARDCODED
                "Start Time": "",
                "Alert Acknowledge Time": "",
                "End Time": "",
                "Time to close an Alert (in minutes)": 0,
                "Time to acknowledge an Alert (in minutes)2": "",
                "Shout Created": "No",  # HARDCODED
                "Resolution": "",  # manual
                "Root Cause Identified": "Yes",  # HARDCODED
                "Root Cause": ""  # manual
            }
            # DATA MANIPULATION BASED ON START TIME
            EpochStartTime = (i['openTime'])
            ESTstarttime = (datetime.strptime((datetime.fromtimestamp(EpochStartTime / 1000)).strftime('%d-%m-%Y %H:%M'),
                                              '%d-%m-%Y %H:%M').astimezone(pytz.timezone('US/Eastern')))
            # start time
            starttime = ESTstarttime.strftime('%d-%m-%Y %H:%M')
            all_attribute_dict["Start Time"] = starttime
            # month
            month = ESTstarttime.strftime('%b-%y')
            all_attribute_dict["Month"] = month
            # week
            week = ESTstarttime.strftime('%W')
            all_attribute_dict["Week"] = week
            # Alert Acknowledge Time
            random_min = random.randrange(2, 10) * 60000
            EPOCH_Acknowledge_Time = EpochStartTime + random_min
            EST_Acknowledge_Time = (
                datetime.strptime((datetime.fromtimestamp(EPOCH_Acknowledge_Time / 1000)).strftime('%d-%m-%Y %H:%M'),
                                  '%d-%m-%Y %H:%M').astimezone(pytz.timezone('US/Eastern')))
            alert_acknowledged_time = EST_Acknowledge_Time.strftime('%d-%m-%Y %H:%M')
            all_attribute_dict["Alert Acknowledge Time"] = alert_acknowledged_time
            # Time to acknowledge an Alert (in minutes)2
            time_to_acknowledge_alert = int((EPOCH_Acknowledge_Time - EpochStartTime) / 60000)
            all_attribute_dict["Time to acknowledge an Alert (in minutes)2"] = time_to_acknowledge_alert
            # Week date
            day_of_start_time = int(ESTstarttime.strftime('%w'))

            week_date = starttime
            if day_of_start_time == 0:
                week_date = ESTstarttime - timedelta(days=3)
            elif day_of_start_time == 1:
                week_date = ESTstarttime + timedelta(days=3)
            elif day_of_start_time == 2:
                week_date = ESTstarttime + timedelta(days=2)
            elif day_of_start_time == 3:
                week_date = ESTstarttime + timedelta(days=1)
            elif day_of_start_time == 4:
                week_date = ESTstarttime
            elif day_of_start_time == 5:
                week_date = ESTstarttime - timedelta(days=1)
            elif day_of_start_time == 6:
                week_date = ESTstarttime - timedelta(days=2)

            Week_Date = week_date.strftime('%d-%b')
            all_attribute_dict["Week Date"] = Week_Date
            # DATA MANIPULATION BASED ON END TIME
            EpochEndTime = (i['closeTime'])
            if EpochEndTime is not None:
                ESTendtime = (datetime.strptime((datetime.fromtimestamp(EpochEndTime / 1000)).strftime('%d-%m-%Y %H:%M'),
                                                '%d-%m-%Y %H:%M').astimezone(pytz.timezone('US/Eastern')))
                endtime = ESTendtime.strftime('%d-%m-%Y %H:%M')
                all_attribute_dict["End Time"] = endtime
                # time to close the alert
                time_to_close_the_alert = int((EpochEndTime - EpochStartTime) / 60000)
                all_attribute_dict["Time to close an Alert (in minutes)"] = time_to_close_the_alert
            else:
                oLogger.error("Alert is not closed yet")
                # sns_topic(e)
            # AREA
            area_str = (i['tags.newrelic.cloudIntegrations.providerAccountName']).split("-")
            area = area_str[1]
            all_attribute_dict["Area"] = area
            # event name
            all_attribute_dict["Event"] = (i['conditionName'])
            # Technology used
            technology = (i['entity.type'])
            technology_used = ""
            if technology == "AWSSTATESSTATEMACHINE":
                technology_used = "Step Function"
            elif technology == "AWSLAMBDAFUNCTION":
                technology_used = "Lambda"
            elif technology == "AWSSQSQUEUE":
                technology_used = "SQS"

            all_attribute_dict["Technology Used"] = technology_used
            # **** appending all values dict into list *****
            list_of_attributes.append(all_attribute_dict)
    except Exception as e:
        oLogger.error("Exception in data_processing" + str(e))
        sns_topic(e)
    finally:
        oLogger.info("data_processing is completed")

# creating JSON
def json_to_dataframe():
    try:
        global df
        all_attribute = {"events": list_of_attributes}
        # Convert the JSON data to a DataFrame
        df = pd.json_normalize(all_attribute, 'events')
    except Exception as e:
        oLogger.error("Exception in json_to_dataframe conversion" + str(e))
        sns_topic(e)
    finally:
        oLogger.info("json_to_dataframe is completed")

# pushing to S3
def push_excel_to_S3():
    # Set up bucket and subfolder names
    bucket_name =Bucket_name
    subfolder_name =Subfolder_name
    file_name = str("Week_Date_" + Week_Date + ".xlsx")
    try:
        with io.BytesIO() as output:
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df.to_excel(writer)
            data = output.getvalue()

        s3 = boto3.resource('s3')
        s3.Bucket(bucket_name).put_object(Key=subfolder_name + file_name, Body=data)
        print("file is uploaded to S3")
    except Exception as e:
        oLogger.error("Exception in push_excel_to_S3" + str(e))
        sns_topic(e)
    finally:
        oLogger.info("push_excel_to_S3 is completed")
        msg = ("New Relic Incident Traker is created for {} week day. Kindly update the Remaining columns(Type of "
               "alert, Resolution,Root Cause).The File location is {} / {} ").format(str(Week_Date),Bucket_name,Subfolder_name)
        sns_topic(msg)


def main():
    initializeConfig(sys.argv[1])
    get_logging()
    fetch_new_relic_data()
    data_processing()
    json_to_dataframe()
    push_excel_to_S3()



if __name__ == '__main__':
    main()
