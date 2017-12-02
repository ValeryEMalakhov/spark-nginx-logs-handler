#!/bin/bash

LOG_PATH=$1

#Move log-files in READY hdfs folder
/usr/local/hadoop/bin/hadoop fs -put $LOG_PATH/nginx_logs_archive/* /raw_data/nginx_logs/READY/

#Remove all old log-files
rm -r $LOG_PATH/nginx_logs_archive/*

