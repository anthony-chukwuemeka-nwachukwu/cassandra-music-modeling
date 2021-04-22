# checking your current working directory
import csv
import glob
import os
from cassandra.cluster import Cluster

from sql_query import ceate_keyspace, drop_table_queries, create_table_queries, insert_artist_song_length, \
    insert_artist_song_user, insert_first_last_song


def __get_paths_files(file_path):
    """
    Creating list of filepaths to process original event csv data files
    :param file_path:
    :return: list(file_path_list)
    """
    file_path_list = []
    # Create a for loop to create a list of files and collect each filepath
    for root, dirs, files in os.walk(file_path):
        file_path_list.extend(glob.glob(os.path.join(root, '*')))
    return file_path_list


def __get_data_rows_list(file_path):
    """
    Processing the files to create the data file csv that will be used for Apache Casssandra tables
    :param file_path:
    :return: List
    """

    # initiating an empty list of rows that will be generated from each file
    full_data_rows_list = []

    # for every filepath in the file path list
    for f in __get_paths_files(file_path):

        # reading csv file
        with open(f, 'r', encoding='utf8', newline='') as csvfile:
            # creating a csv reader object
            csvreader = csv.reader(csvfile)
            next(csvreader)

            # extracting each data row one by one and append it
            for line in csvreader:
                # print(line)
                full_data_rows_list.append(line)
    return full_data_rows_list


def create_new_event_datafile(output_file_path, file_path):
    """
    create a smaller event data csv file that will be used to insert
    data into the Apache Cassandra tables

    :param output_file_path:
    :param file_path:
    :return:
    """

    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

    with open(output_file_path, 'w', encoding='utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow(['artist', 'firstName', 'gender', 'itemInSession', 'lastName', 'length',
                         'level', 'location', 'sessionId', 'song', 'userId'])
        for row in __get_data_rows_list(file_path):
            if row[0] == '':
                continue
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))


def make_connection(data_base_name):
    cluster = Cluster(['127.0.0.1'])
    # To establish connection and begin executing queries, need a session
    session = cluster.connect()
    # Create keyspace
    session.execute(ceate_keyspace)
    # Set keyspace
    session.set_keyspace(data_base_name)

    # # To establish connection and begin executing queries, need a session
    # session = cluster.connect()
    return session, cluster


def create_drop_table(query, session):
    session.execute(query)


def insert_data_artist_song_length(file, query, session):
    with open(file, encoding='utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader)  # skip header
        for line in csvreader:
            session.execute(query, (int(line[8]), int(line[3]), line[0], line[9], float(line[5])))


def insert_data_artist_song_user(file, query, session):
    with open(file, encoding='utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader)  # skip header
        for line in csvreader:
            session.execute(query, (int(line[8]), int(line[10]), int(line[3]), line[0], line[9], line[1], line[4]))


def insert_data_first_last_song(file, query, session):
    with open(file, encoding='utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader)  # skip header
        for line in csvreader:
            session.execute(query, (int(line[10]), line[9], line[1], line[4]))


def main():
    """
    This procedure processes the process data for the song and log files.
    """

    # Get your current folder and subfolder event data
    output_file_path = 'event_datafile_new.csv'
    file_path = '.\event_data'
    create_new_event_datafile(output_file_path, file_path)

    # Make a connection to a Cassandra instance
    # (127.0.0.1)
    session, cluster = make_connection('event_database')
    print("Connection Made")

    # Drop and Create Tables
    # for q in drop_table_queries:
    #     create_drop_table(q, session)
    print("Dropped Tables")
    for q in create_table_queries:
        create_drop_table(q, session)
    print("Created Tables")

    # Insert
    new_file = 'event_datafile_new.csv'
    insert_data_artist_song_length(new_file, insert_artist_song_length, session)
    print("Inserted into artist_song_length")
    insert_data_artist_song_user(new_file, insert_artist_song_user, session)
    print("Inserted into artist_song_user")
    insert_data_first_last_song(new_file, insert_first_last_song, session)
    print("Inserted into first_last_song")

    # Close session and cluster connection
    session.shutdown()
    cluster.shutdown()
    print("Cluster and Session shutdown")


if __name__ == "__main__":
    main()
