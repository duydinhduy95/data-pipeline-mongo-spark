FROM apache/airflow:2.8.2
# setup things
USER root
# setup pkgs
RUN apt-get update && apt-get install -y wget gnupg openjdk-17-jdk

RUN mkdir /content
# mongosh
RUN wget -qO "/content/mongodb-mongosh_amd64.deb" "https://downloads.mongodb.com/compass/mongodb-mongosh_1.9.1_arm64.deb"
RUN dpkg -i "/content/mongodb-mongosh_amd64.deb"

# mongodb-database-tools
RUN wget -qO "/content/mongodb-database-tools.deb" "https://fastdl.mongodb.org/tools/db/mongodb-database-tools-ubuntu1804-arm64-100.9.3.deb"
RUN dpkg -i "/content/mongodb-database-tools.deb"

# JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64

# setup things
USER airflow
RUN pip install -U apache-airflow-providers-apache-spark pyspark findspark --user

# REMEMBER TO BUILD ME
# docker build -t my_airflow .
