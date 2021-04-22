# CREATE KEYSPACE

ceate_keyspace = """
        CREATE KEYSPACE IF NOT EXISTS event_database
        WITH REPLICATION = {
            'class': 'SimpleStrategy',
            'replication_factor': 1
        }
    """

# DROP TABLES

drop_artist_song_user = "DROP TABLE artist_song_user"
drop_artist_song_length = "DROP TABLE artist_song_length"
drop_first_last_song = "DROP TABLE first_last_song"

# CREATE TABLES

create_artist_song_length = ("""
    CREATE TABLE IF NOT EXISTS artist_song_length(
        sessionId int,
        itemInSession int,
        artist text,
        song text,
        length float,
        PRIMARY KEY (sessionId, itemInSession)
    );
""")

create_artist_song_user = ("""
    CREATE TABLE IF NOT EXISTS artist_song_user(
        sessionId int,
        userId int,
        itemInSession int,
        artist text,
        song text,
        firstName text,
        lastName text,
        PRIMARY KEY ((sessionId, userId), itemInSession)
    );
""")

create_first_last_song = ("""
    CREATE TABLE IF NOT EXISTS first_last_song(
        userId int,
        song text,
        firstName text,
        lastName text,
        PRIMARY KEY (song, userId)
    );
""")

# INSERT INTO TABLES

insert_artist_song_length = ("""
    INSERT INTO artist_song_length(
        sessionId,
        itemInSession,
        artist,
        song,
        length
    )
    VALUES(%s,%s,%s,%s,%s);
""")

insert_artist_song_user = ("""
    INSERT INTO artist_song_user(
        sessionId,
        userId,
        itemInSession,
        artist,
        song,
        firstName,
        lastName
    )
    VALUES(%s,%s,%s,%s,%s,%s,%s);
""")

insert_first_last_song = ("""
    INSERT INTO first_last_song(
        userId,
        song,
        firstName,
        lastName
)
    VALUES(%s,%s,%s,%s);
""")

# QUERY LISTS

create_table_queries = [create_artist_song_length, create_artist_song_user, create_first_last_song]
drop_table_queries = [drop_artist_song_user, drop_artist_song_length, drop_first_last_song]