import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import pdb


def process_song_file(cur, filepath):
    """
    Description: Inserts rows into songs and artists tables using data from the song file.
    
    Arguments:
        cur: cursor to the database
        filepath: path to the song data file in the local directory
    
    Returns:
        None
        
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert artist record
    artist_data = list(df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].values[0])
    cur.execute(artist_table_insert, artist_data)

    # insert song record
    song_data = list(df[["song_id", "title", "artist_id", "year", "duration"]].values[0])
    cur.execute(song_table_insert, song_data)
    

def process_log_file(cur, filepath):
    """
    Description: Inserts rows into users, time and songplays tables using the log file. Extracts the correct timestamps based on "NextSong" action, then calculates the values for the other columns of the time table. Finds the proper values for the songplays table by joining songs and artists tables. 
    
    Arguments:
        cur: cursor to the database
        filepath: path to the log data file in the local directory
    
    Returns:
        None
        
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page=="NextSong"]
    df.ts = pd.to_datetime(df.ts)

    # convert timestamp column to datetime
    t = df.ts
    
    # insert time data records
    time_data = (list(t.dt.time), list(t.dt.hour), list(t.dt.day), list(t.dt.isocalendar().week), list(t.dt.month), list(t.dt.year), list(t.dt.weekday))
    column_labels = ("start time", "hour", "day", "week", "month", "year", "weekday")
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df.loc[:, ["userId", "firstName", "lastName", "gender", "level"]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            #pdb.set_trace()
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Description: iterates over every file (song/log) and processes data using one of the two functions (process_song_file, process_log_file)
    
    Arguments:
        cur: cursor to the database
        conn: connection to the database
        filepath: path to the song/log data file in the local directory
        func: the function to use to process given data (process_song_file or process_log_file)
    
    Returns:
        None
        
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    Description: The main function that processes all the data by calling to the process_data function.
    
    Arguments:
        None
    
    Returns:
        None
        
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()