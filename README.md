## Purpose of the Database
This database moves csv files into cassandra database

## Need to be installed
You can install these with pip or just run the installation_files.py
- cassandra-driver

## Set up Docker for Cassandra

Start a Cassandra node in Docker with  
$ docker pull cassandra  
$ docker run --name cassandra-container -p 9042:9042 -d cassandra:latest

To stop and remove the container after the exercise:  
$ docker stop cssandra-container  
$ docker rm cassandra-container

## How to use
- Run the etl.py to create database and insert data
- execute the queries in test.ipynb to select as you please