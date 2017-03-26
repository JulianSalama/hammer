#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# Update a redis server cache when an evenement is trigger
# in MySQL replication log
#


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

import random


def main():

    stream = BinLogStreamReader(
        connection_settings=MYSQL_SETTINGS, server_id=3, blocking=True, only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent])

    s3 = boto3.resource('s3')

    previous_events = []

    for binlogevent in stream:
        prefix = "%s:%s:" % (binlogevent.schema, binlogevent.table)

        for row in binlogevent.rows:
            if isinstance(binlogevent, DeleteRowsEvent):
                vals = row["values"]
                print vals
            elif isinstance(binlogevent, UpdateRowsEvent):
                vals = row["after_values"]
                print vals
            elif isinstance(binlogevent, WriteRowsEvent):
                s3.Bucket(S3_SETTINGS['bucket_name']).put_object(Key="awef" +str( random.random() )+ ".txt", Body=str(row["values"]))
                
    stream.close()


if __name__ == "__main__":
    main()
