# Download base image ubuntu 16.04
FROM ubuntu:20.04

# Set encoding
ENV LANG C.UTF-8
ENV DEBIAN_FRONTEND=noninteractive 

# Update Ubuntu Software repository
RUN apt-get update -y && apt-get upgrade -y

# Install essential packages
RUN apt-get install -y wget curl zip vim

# Install python-related packages and PIP3.
RUN apt-get install python3-pip -y

# Install kafka-needed java library
RUN apt-get install -y default-jre

# Install pip libraries
COPY data/requirements.txt /
RUN pip3 install --no-cache-dir -r requirements.txt

# Create needed folders
RUN mkdir /home/mp && mkdir /home/mp/Downloads && mkdir /home/mp/Downloads/kafka

#RUN wget https://downloads.apache.org/kafka/2.6.0/kafka_2.12-2.6.0.tgz -O /home/mp/Downloads/kafka/kafka.tgz
RUN wget https://archive.apache.org/dist/kafka/2.6.0/kafka_2.12-2.6.0.tgz -O /home/mp/Downloads/kafka/kafka.tgz
RUN tar -xvzf /home/mp/Downloads/kafka/kafka.tgz --strip 1 --directory /home/mp/Downloads/kafka/

#Install TwitterAPI
COPY src/local_twitterlib/ /twitter_lib/
RUN pip install /twitter_lib/ && rm -r /twitter_lib

# Create folder of integrated machine
RUN mkdir /home/mp/machines && mkdir /home/mp/machines/data && mkdir /home/mp/machines/src

# Copy configuration files
COPY data/config_group_id.csv /home/mp/machines/data/config_group_id.csv
COPY data/config_high2atomic.csv /home/mp/machines/data/config_high2atomic.csv
COPY data/config_high.csv /home/mp/machines/data/config_high.csv
COPY data/config_mapping.csv /home/mp/machines/data/config_mapping.csv
COPY data/config_servers.csv /home/mp/machines/data/config_servers.csv
COPY data/config_timeouts.csv /home/mp/machines/data/config_timeouts.csv
COPY data/config_topics.csv /home/mp/machines/data/config_topics.csv

# Copy files to specific folders
COPY src/common_functions.py /home/mp/machines/src/common_functions.py
COPY src/producer.py /home/mp/machines/src/producer.py
COPY src/consumer.py /home/mp/machines/src/consumer.py

# Copy Twitter api
COPY src/twitter_api.py /home/mp/machines/src/twitter_api.py

# Copy YouTube api
COPY src/youtube_api.py /home/mp/machines/src/youtube_api.py

# Copy Instagram api
COPY src/instagram_api.py /home/mp/machines/src/instagram_api.py
COPY src/instagram_coletor.py /home/mp/machines/src/instagram_coletor.py
COPY src/instagram_data_collection.py /home/mp/machines/src/instagram_data_collection.py
COPY src/instagram_data_handle.py /home/mp/machines/src/instagram_data_handle.py
# Copy local Instaloader library
RUN mkdir /home/mp/machines/src/local_instaloader
COPY src/local_instaloader/*.py /home/mp/machines/src/local_instaloader/

WORKDIR /home/mp/machines/src/

