#!/bin/bash

LOG_PATH=$1

TIMESTAMP=$(date -u '+%s%N');
RAND=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1);
SHA=$(echo $RAND | sha256sum);

NEW_FILE_NAME=${TIMESTAMP}_${SHA%"  -"}.log

#Rename and move log file
mv $LOG_PATH/access.log $LOG_PATH/nginx_logs_archive/${NEW_FILE_NAME}

#Re-run logs
docker exec -i docker-nginx bash -c  'kill -USR1 `cat /var/run/nginx.pid`'

#Archive the log file
cd $LOG_PATH/nginx_logs_archive
gzip -rm ${NEW_FILE_NAME}

