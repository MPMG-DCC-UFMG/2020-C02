# Download base image ubuntu 16.04
FROM ubuntu:16.04

# Set encoding
ENV LANG C.UTF-8

# Update Ubuntu Software repository
RUN apt-get update -y && apt-get upgrade -y

# Install essential packages
RUN apt-get install -y wget curl zip

# Install kafka-needed java library
RUN apt-get install -y default-jre


# Install python-related packages and PIP3.
RUN  apt install software-properties-common -y && \
 add-apt-repository ppa:deadsnakes/ppa -y && apt-get update -y && apt-get install python3.8-venv -y

# Make python 3.8 default version
RUN python3.8 -m venv /venv
ENV PATH=/venv/bin:$PATH

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
