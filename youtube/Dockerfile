# Download base image ubuntu 16.04
FROM ubuntu:16.04

# Set encoding
ENV LANG C.UTF-8

# Update Ubuntu Software repository
RUN apt-get update -y && apt-get upgrade -y

# Install essential packages
RUN apt-get install -y wget curl zip

# Install python-related packages
RUN apt-get install -y python3

# Test python3
# RUN python3 -c 'print(1997)'

# Install pip3
COPY get-pip.py /
RUN python3 /get-pip.py && rm /get-pip.py

# Install pip libraries
COPY requirements.txt /
RUN pip3 install --no-cache-dir -r requirements.txt

# Check freeze
# RUN pip3 freeze

# Create needed folders
RUN mkdir /home/mp && mkdir /home/mp/coletor-youtube && mkdir /home/mp/coletor-youtube/crawler

# Copy files to specified folders
COPY main.py /home/mp/coletor-youtube/main.py
COPY crawler/api.py /home/mp/coletor-youtube/crawler/api.py

# Assure it worked
RUN pip3 freeze && find /home/mp/ -iname "*py"
