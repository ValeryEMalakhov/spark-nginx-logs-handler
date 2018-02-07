# Spark nginx logs handler

Application for obtaining events logs from Docker-nginx, validating, processing and further storing them in Hadoop HDFS.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Prerequisites

What things you need to install the software and how to install them.

#### First workstation

On the first server, you must have:

* [Ubuntu 14.04 / 16.04](https://www.ubuntu.com/download)
* sudo user
* [Docker](https://www.docker.com/)
* [Git](https://git-scm.com/)

* [Apache Hadoop 2.7.4](http://www.apache.org/dyn/closer.cgi/hadoop/common/hadoop-2.7.4/hadoop-2.7.4-src.tar.gz)
* [Apache Spark 1.6.0 / 2.2.0](https://spark.apache.org/downloads.html)

First, you need to add the current user to the Docker group. This will avoid the need to use sudo every time

```
sudo groupadd docker

sudo gpasswd -a {USERNAME} docker
```

Log out and back in, then restart the Docker daemon

```
# Ubuntu
sudo service docker resart

# Arch
sudo systemctl docker restart
```

Then, you need to load the nginx container into the Docker

```
docker pull nginx
```

### Installing

#### First workstation (Step 1)

To get started, download the repository into the installation folder.

You can do this, for example, using git

```
cd $PATH_TO_APP

git clone https://github.com/ValeryEMalakhov/SparkNginxLogsHandler.git
```

The first entry point is *PATH_TO_APP/SparkNginxLogsHandler/injection/nginx/create_docker_nginx.sh*.

Since the system uses Docker *volumes*, the user can specify path to the directory where the logs will be written, or the default path (*$PATH_TO_APP/SparkNginxLogsHandler/output/nginx_logs*) will be used.

```
bash $PATH_TO_APP/SparkNginxLogsHandler/injection/nginx/create_docker_nginx.sh "$PATH_TO_APP/SparkNginxLogsHandler" "$SPECIFIC_LOGS_PATH"
```

The above command starts and configures the nginx container with name *docker-nginx*.

In order to ensure that the container is up and running

```
docker ps -a
```

To display the log file

```
cat "$SPECIFIC_LOGS_PATH"/access.log
```

#### First workstation (Step 2)

##### Presetting

Before running `crontab`, you need to configure the file *logs_rollout_crontab*, where you specify the path to the application and the path to the logs.

```
nano $PATH_TO_APP/SparkNginxLogsHandler/injection/cron/logs_rollout_crontab

```

In the file editor

```
APP_PATH=$PATH_TO_APP/SparkNginxLogsHandler
LOG_PATH=$SPECIFIC_LOGS_PATH
```

Also you can change the time for splitting the log file into parts into an archive (by default every minute)

```
* * * * * bash $APP_PATH/injection/cron/rename_and_move_logs.sh $LOG_PATH
```

And copying logs to hdfs (by default every hour)

```
0 * * * * bash $APP_PATH/injection/cron/put_local_logs_in_hdfs.sh $LOG_PATH
```

##### Starting

To start `crontab`, run the following command

```
crontab $APP_PATH/SparkNginxLogsHandler/injection/cron/logs_rollout_crontab
```

#### First workstation (Step 3)

To run this Spark application on the Yarn cluster, run the following command

```
spark-submit --master yarn-cluster $PATH_TO_APP/SparkNginxLogsHandler/enricher/target/scala-2.11/logsenricher_2.11*.jar 10
```

## Versioning

1.0.0.f4

## Authors

* **Valery Malakhov** - *Initial developer* - [ValeryEMalakhov](https://github.com/ValeryEMalakhov)

See also the list of [contributors](https://github.com/ValeryEMalakhov/SparkNginxLogsHandler/graphs/contributors) who participated in this project.

## License

**-**
