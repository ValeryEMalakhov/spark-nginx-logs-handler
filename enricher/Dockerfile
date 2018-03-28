# docker cont name - se_env
FROM teradatalabs/cdh5-hive
MAINTAINER VEM

USER root

# update all
RUN yum update
RUN java -version

# Set hadoop global env
ENV HADOOP_HOME usr/lib/hadoop
ENV HADOOP_CONF_DIR $HADOOP_HOME/etc/hadoop

# Install scala and sbt
## Scala
#RUN echo "SCALA_HOME=/usr/lib/scala/scala-2.12.4" > /etc/profile.d/scala.sh
#RUN echo 'export SCALA_HOME' >> /etc/profile.d/scala.sh
RUN wget http://www.scala-lang.org/files/archive/scala-2.12.4.tgz
RUN mkdir -p /usr/lib/scala
RUN cp scala-2.12.4.tgz /usr/lib/scala/
RUN cd /usr/lib/scala/
RUN tar xvf scala-2.12.4.tgz
RUN rm -f scala-2.12.4.tgz
RUN chown -R root:root /usr/lib/scala

RUN update-alternatives --install "/usr/bin/scala" "scala" "/usr/lib/scala/scala-2.12.4/bin/scala" 1
RUN update-alternatives --install "/usr/bin/scalac" "scalac" "/usr/lib/scala/scala-2.12.4/bin/scalac" 1
RUN update-alternatives --install "/usr/bin/scalap" "scalap" "/usr/lib/scala/scala-2.12.4/bin/scalap" 1
RUN update-alternatives --install "/usr/bin/scaladoc" "scaladoc" "/usr/lib/scala/scala-2.12.4/bin/scaladoc" 1
RUN update-alternatives --install "/usr/bin/fsc" "fsc" "/usr/lib/scala/scala-2.12.4/bin/fsc" 1

ENV SCALA_HOME /usr/lib/scala/scala-2.12.4
RUN scala -version

## SBT
#RUN echo 'SBT_HOME=/usr/lib/sbt/sbt-1.1.1' > /etc/profile.d/sbt.sh
#RUN echo 'export SBT_HOME' >> /etc/profile.d/sbt.sh
RUN mkdir -p /usr/lib/sbt
RUN wget https://github.com/sbt/sbt/releases/download/v1.1.1/sbt-1.1.1.tgz
RUN cp sbt-1.1.1.tgz /usr/lib/sbt/
RUN cd /usr/lib/sbt/
RUN tar xvf sbt-1.1.1.tgz
RUN rm -f sbt-1.1.1.tgz
RUN chown -R root:root /usr/lib/sbt

RUN update-alternatives --install "/usr/bin/sbt" "sbt" "/usr/lib/sbt/sbt-1.1.1/bin/sbt" 1

ENV SBT_HOME /usr/lib/sbt/sbt-1.1.1

# Install HBase
RUN wget http://apache.mirror.gtcomm.net/hbase/stable/hbase-1.2.6-bin.tar.gz
RUN tar -zxvf hbase-1.2.6-bin.tar.gz
RUN sudo mv hbase-1.2.6 /usr/lib/HBase/
RUN rm -f hbase-1.2.6-bin.tar.gz

ENV HBASE_HOME /usr/lib/Hbase

# Copy hadoop configuration
ADD ./hadoop_conf/hadoop/*  /usr/lib/hadoop/etc/hadoop/
ADD ./hadoop_conf/yarn/*  /usr/lib/hadoop-yarn/etc/hadoop/
ADD ./spark/* /usr/lib/spark

ENV SPARK_HOME /usr/lib/spark

# Add logs enriher entrypoint script
ADD ./script/run_logs_enricher_with_spark_submit.sh /root/le_startup.sh

# EntryPoint
CMD /root/le_startup.sh