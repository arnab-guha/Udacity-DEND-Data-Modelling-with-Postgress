import os
import glob
import psycopg2
import pandas as pd
import datetime
from sql_queries import *


def process_song_file(cur, filepath):
    
    """ Process_song_file takes cursor and list of song files as arguments and process data for Songs and Artists table.
    
    It reads the song json files and stores them in a dataframe. Then it extracts the Songs and Artist related columns from the dataframe and executes SQL statements to insert data into Songs and Artists table.  
    """
    
    # open song file
    #df =
    df = pd.read_json(filepath, lines=True)

    # insert song record
    songcols = ['song_id','title','artist_id','year','duration']
    song_data = list(df[songcols].values.flatten())
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_col = ['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']
    artist_data=list(df[artist_col].values.flatten())
    cur.execute(artist_table_insert, artist_data)

def process_log_file(cur, filepath):
    
    """  Process_log_file takes cursor and list of log files as arguments and process data for Time, User and Songplays table. 
    
    It captures all json formatted log files and stores them in a dataframe. 
    It formats the TS column into proper TimeStamp information and captures the Time table columns from the same. It stores them in another dataframe and triggers a SQL statement to insert the dataframe into Time table. 
    
    It captures all user related columns from the main dataframe and stores them in a temporary dataframe. Then it triggers a SQL statement to store the user dataframe into Users table. 
    
    It triggers a SQL statement to capture the song_id, Artist_id and length from Songs and Artists table. It then generates proper Timestamp column from TS field. Finally it triggers a SQL statement to insert the necessary columns to Songsplay table. 
    """
    
    # open log file
    df = pd.read_json(filepath, lines=True)
    
    # filter by NextSong action
    df = df.loc[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.Series()
    for row in df['ts']:
        t=t.append(pd.Series(datetime.datetime.fromtimestamp(row / 1e3)))
    
    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('start_time', 'hour', 'day', 'week of year', 'month', 'year','weekday')
    time_df = pd.DataFrame(dict(zip(column_labels,time_data)),columns=column_labels).reset_index(drop=True).drop_duplicates()
    
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

    # insert user records
    for i, row in user_df.iterrows():
        if row[0]!='':
            cur.execute(user_table_insert, row)


    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record        
        start_time = pd.Series(datetime.datetime.fromtimestamp(row['ts'] / 1e3))[0]
        
        #songplay_data = (songplay_id,start_time,row['userId'],row['level'],songid,artistid,row['sessionId'],row['location'],row['userAgent'])
        songplay_data = (start_time,row['userId'],row['level'],songid,artistid,row['sessionId'],row['location'],row['userAgent'])
        cur.execute(songplay_table_insert, songplay_data)
    
    
def process_data(cur, conn, filepath, func):
    
    """ Process_data function use the cursor and connection to collect all the files and call functions to process Song and Log files.
   
   It collects all files in the available directory and store them in a list which it passes along with cursor to call the process_song_file and process_log_file functions On completion of the functions, it prints how many files are processed.
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
    """ Main function which calls the process_data function for processing Songs and Logs file. 
    
    It passes the cursor, connection, filepaths and the function names as arguments.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()