#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# Output logstash events to the console from MySQL replication stream
#
# You can pipe it to logstash like this:
# python examples/logstash/mysql_to_logstash.py | java -jar logstash-1.1.13-flatjar.jar  agent -f examples/logstash/logstash-simple.conf

import json
import random
from dateutil.parser import parse
import sys

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)

MYSQL_SETTINGS = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "passwd": ""
}

import boto3
S3_SETTINGS = {
    "bucket_name": "hammer-test-abc"
}

MAX_DUMP_SIZE = 0

def main():
    stream = BinLogStreamReader(
        connection_settings=MYSQL_SETTINGS,
        server_id=3,
        only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent])

    s3 = boto3.resource('s3')

    for binlogevent in stream:
        dump = []

        for row in binlogevent.rows:
            event = {"schema": binlogevent.schema, "table": binlogevent.table}

            #if isinstance(binlogevent, DeleteRowsEvent):
                #event["action"] = "delete"
                #event = dict(event.items() + row["values"].items())
            
            if isinstance(binlogevent, WriteRowsEvent):
                event["action"] = "insert"
                event = dict(event.items())
                event["values"] = row["values"]

                if "created_at" in event["values"].keys():
                    dump.append( event )

            elif isinstance(binlogevent, UpdateRowsEvent):
                event["action"] = "update"
                event = dict(event.items() + row["after_values"].items())
            
            if len(dump) > MAX_DUMP_SIZE:
                s3.Bucket(S3_SETTINGS['bucket_name']).put_object(Key=event["table"] + "/"+ partition(event["values"]) + "/0" ".json", Body=str(event["values"]))

    stream.close()

#def format_dump(dump):
#    dump_body = {}
#    for event in dump:
#        if event["action"] == "insert":
            


##
# It's uneven.
# returns the partition in the day that this event belongs in.
def partition(event_values):
    created_at = event_values["created_at"]

    partition = 0
    if 0 <= created_at.minute < 15:
        partition = 0
    elif 15 <= created_at.minute < 30:
        partition = 1
    elif 30 <= created_at.minute < 45:
        partition = 2
    elif 45 <= created_at.minute < 60:
        partiition = 3

    return str(created_at.year) + "_" + str(created_at.month) + "_" + str(created_at.day) + "/" + str( partition )

if __name__ == "__main__":
    main()
    
