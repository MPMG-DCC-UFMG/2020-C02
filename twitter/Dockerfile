# Download base image ubuntu 16.04
FROM ubuntu:20.04

# Set encoding
ENV LANG C.UTF-8
ENV DEBIAN_FRONTEND=noninteractive 

# Update Ubuntu Software repository
RUN apt-get update -y && apt-get upgrade -y

# Install essential packages
RUN apt-get install -y wget curl zip

# Install kafka-needed java library
RUN apt-get install -y default-jre

# Install python-related packages and PIP3.
RUN apt-get install python3-pip -y

# Install pip libraries
COPY requirements.txt /
RUN pip3 install --no-cache-dir -r requirements.txt

# Install TwitterAPI
COPY util_libraries/TwitterAPI/ /twitter_lib/
RUN pip install /twitter_lib/ && rm -r /twitter_lib

# Check freeze
# RUN pip3 freeze

# Create needed folders
RUN mkdir /home/mp && mkdir /home/mp/coletor-twitter && mkdir /home/mp/coletor-twitter/crawler

# Copy files to specified folders
COPY coletor-twitter/main.py /home/mp/coletor-twitter/main.py
COPY coletor-twitter/crawler/api.py /home/mp/coletor-twitter/crawler/api.py

# Assure it worked
RUN pip3 freeze && find /home/mp/ -iname "*py"
