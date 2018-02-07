#!/bin/bash

APP_PATH=$1
LOG_PATH=$2

if [ ! -e LOG_PATH ]; then
    mkdir $APP_PATH/output $APP_PATH/output/nginx_logs

    LOG_PATH="${LOG_PATH:-$APP_PATH/output/nginx_logs}"
fi

mkdir $LOG_PATH/nginx_logs_archive

#Started new docker-nginx container
docker run --name=docker-nginx -d -v $LOG_PATH:/var/log/nginx -p 80:80 nginx

#Create tmp folder for app
mkdir /tmp/snlh

docker exec -i docker-nginx mkdir /opt/nginx

docker cp $APP_PATH/injection/nginx/gifs docker-nginx:/usr/share/nginx/html/

#Creates a new nginx configuration file
echo $'
user  nginx;
worker_processes  1;

error_log  /var/log/nginx/error.log warn;
pid        /var/run/nginx.pid;


events {
    worker_connections  1024;
}


http {
    include       /etc/nginx/mime.types;
    default_type  application/octet-stream;

    map $request_filename $base_file_name {
       default "";
       ~/(?<captured_basename>[^/]*)$ $captured_basename;
    }

    log_format  retarget    \'$time_local\trt\t$request_id\t$cookie_USER\t\'
                            \'$host\t$remote_addr\t$http_user_agent\t$args\';

    log_format  impression  \'$time_local\timpr\t$request_id\t$cookie_USER\t\'
                            \'$host\t$remote_addr\t$http_user_agent\t$args\';

    log_format  click       \'$time_local\tclk\t$request_id\t$cookie_USER\t\'
                            \'$host\t$remote_addr\t$http_user_agent\t$args\';

    map $base_file_name $rt {
        default 0;
        ~rt 1;
    }

    map $base_file_name $impr {
        default 0;
        ~impr 1;
    }

    map $base_file_name $clk {
        default 0;
        ~clk 1;
    }

    access_log  /var/log/nginx/access.log  retarget     if=$rt;
    access_log  /var/log/nginx/access.log  impression   if=$impr;
    access_log  /var/log/nginx/access.log  click        if=$clk;

    sendfile        on;
    #tcp_nopush     on;

    keepalive_timeout  65;

    #gzip  on;

    include /etc/nginx/conf.d/*.conf;
}' >> /tmp/snlh/n.conf

#Change datetime
echo -e "8\23" | docker exec -i docker-nginx dpkg-reconfigure tzdata

#Pass the nginx configuration file to the container
docker cp /tmp/snlh/n.conf docker-nginx:/etc/nginx/nginx.conf

#Restart nginx with new configuration
docker exec -i docker-nginx nginx -s reload

#Remove app tmp folder
rm -rf /tmp/snlh

