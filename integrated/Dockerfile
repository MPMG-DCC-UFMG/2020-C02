# Download base image ubuntu 16.04
FROM ubuntu:16.04

# Set encoding
ENV LANG C.UTF-8

# Update Ubuntu Software repository
RUN apt-get update -y && apt-get upgrade -y

# Install essential packages
RUN apt-get install -y wget curl zip vim

# Install python-related packages
RUN apt-get install -y python3

# Install kafka-needed java library
RUN apt-get install -y default-jre

# Test python3
# RUN python3 -c 'print(1997)'

# Install pip3
COPY src/get-pip.py /
RUN python3 /get-pip.py && rm /get-pip.py

# Install pip libraries
COPY data/requirements.txt /
RUN pip3 install --no-cache-dir -r requirements.txt

# Check freeze
# RUN pip3 freeze

# Create needed folders
RUN mkdir /home/mp && mkdir /home/mp/Downloads && mkdir /home/mp/Downloads/kafka

# Install kafka-scripts
# COPY data/kafka.tgz /home/mp/Downloads/kafka/kafka.tgz
RUN wget https://downloads.apache.org/kafka/2.6.0/kafka_2.12-2.6.0.tgz -O /home/mp/Downloads/kafka/kafka.tgz
RUN tar -xvzf /home/mp/Downloads/kafka/kafka.tgz --strip 1 --directory /home/mp/Downloads/kafka/

# Create folder of integrated machine
RUN mkdir /home/mp/machines && mkdir /home/mp/machines/data && mkdir /home/mp/machines/src

# Copy configuration files
COPY data/config_group_id.csv /home/mp/machines/data/config_group_id.csv
COPY data/config_high2atomic.csv /home/mp/machines/data/config_high2atomic.csv
COPY data/config_high.csv /home/mp/machines/data/config_high.csv
COPY data/config_mapping.csv /home/mp/machines/data/config_mapping.csv
COPY data/config_topics.csv /home/mp/machines/data/config_topics.csv

# Copy files to specific folders
COPY src/common_functions.py /home/mp/machines/src/common_functions.py
COPY src/hybrid.py /home/mp/machines/src/hybrid.py
COPY src/producer.py /home/mp/machines/src/producer.py

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

# Assure it worked
# RUN pip3 freeze && find /home/mp/ -iname "*py"

WORKDIR /home/mp/machines/src/
