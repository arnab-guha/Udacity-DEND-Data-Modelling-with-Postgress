# Postgres DB & ETL Pipeline for Sparkify
#### *Creation of the datamodel in Postgres Database and the necessary ETL pipeline to support the same*


#### Purpose:
This data warehousing solution will help **Sparkify** to analyze their data in a more effective way which will in turn help them to make better business decisions. It will help them to understand user's taste in songs, their choice and listening pattern and utlizing those analytics, Sparkiy will be able to provide more quality service and gain their user base. 


#### Source File Information
There are 2 different types of Data that is available for the Sparkify music streaming application amd they are stored as JSON files. Following are the details regarding the same:

- Song Files: It has all Songs, Albums and Artist related details. Here is one sample row:
        {   "num_songs": 1, 
            "artist_id": "ARD7TVE1187B99BFB1", 
            "artist_latitude": null, 
            "artist_longitude": null, 
            "artist_location": "California - LA", 
            "artist_name": "Casual", 
            "song_id": "SOMZWCG12A8C13C480", 
            "title": "I Didn't Mean To", 
            "duration": 218.93179, 
            "year": 0
        }

- Log Files: It has the logs of the user's music listening activity on the app. Here is one sample row:
        {   "artist":null,
            "auth":"Logged In",
            "firstName":"Walter",
            "gender":"M",
            "itemInSession":0,
            "lastName":"Frye",
            "length":null,
            "level":"free",
            "location":"San Francisco-Oakland-Hayward, CA",
            "method":"GET",
            "page":"Home",
            "registration":1540919166796.0,
            "sessionId":38,
            "song":null,
            "status":200,
            "ts":1541105830796,
            "userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"39"
        }

#### Database Schema Design

To provide a platform of efficient data analysis, we have created a Star schema data model to store the above mentioned JSON files. The different dimension information that we can gather from the files are Scongs, Artists, Users and Time. Whereas the fact or measure that we get from the files are song listening activities of the log files. Hence we will create 4 dimenson tables **Songs, Artists, Users, Time** and 1 fact table **Songsplay**. The fact table will be connected to the dimension tables through foregin key constraints. 

We have created the tables in **PostgreSQL** database. Here are the Create Table Statement of the Dimension and fact tables:

**Songs:** *CREATE TABLE IF NOT EXISTS songs(song_id varchar NOT NULL PRIMARY KEY,title varchar,artist_id varchar,year int,duration NUMERIC(10,5));*

**Artists:** *CREATE TABLE IF NOT EXISTS artists(artist_id VARCHAR NOT NULL PRIMARY KEY,name VARCHAR,location VARCHAR,latitude DECIMAL,longitude DECIMAL);*

**Users:** *CREATE TABLE IF NOT EXISTS users(user_id INT PRIMARY KEY,first_name VARCHAR,last_name VARCHAR,gender VARCHAR,level VARCHAR);*

**Time:** *CREATE TABLE IF NOT EXISTS time(start_time TIMESTAMP,hour INT,day INT,week INT, month INT,year INT,weekday VARCHAR);*

**Songsplay:** *CREATE TABLE IF NOT EXISTS songplays(songplay_id BIGSERIAL PRIMARY KEY,start_time timestamp,user_id int REFERENCES users(user_id),level varchar,song_id varchar REFERENCES songs(song_id),artist_id varchar REFERENCES artists(artist_id),session_id int,location varchar,user_agent varchar);*

***Note. The SQL queries and the create table statements are stored in sql_queries.py and create_tables.py file which can be triggered using the following commands:***

- python sql_queries.py

- python create_tables.py


#### ETL Pipeline

The ETL process for the json files are handled in a Python framework using a main function and multiple other functions. Here are the Tree structure of the usage of different functions for the ETL process:

- Main
    - Process_data
        1. process_song_file - *inserts data into songs and artists table*
        2. process_log_file - *inserts data into users, time and sonsplay table*

##### Process_song_file function:
Process_song_file takes cursor and list of song files as arguments and process data for Songs and Artists table. 
- It reads the song json files and stores them in a dataframe. Then it extracts the Songs and Artist related columns from the dataframe and executes SQL statements to insert data into Songs and Artists table

##### Process_log_file function:
Process_log_file takes cursor and list of log files as arguments and process data for Time, User and Songplays table. 

- It captures all json formatted log files and stores them in a dataframe. It formats the TS column into proper TimeStamp information and captures the Time table columns from the same. It stores them in another dataframe and triggers a SQL statement to insert the dataframe into Time table. 
    
- It captures all user related columns from the main dataframe and stores them in a temporary dataframe. Then it triggers a SQL statement to store the user dataframe into Users table. 

- It triggers a SQL statement to capture the song_id, Artist_id and length from Songs and Artists table. It then generates proper Timestamp column from TS field. Finally it triggers a SQL statement to insert the necessary columns to Songsplay table.

***Note. The ETL process coded in the etl.py file which can be triggered using the following command:***

- python etl.py

Here are the sample rows from the tables once the data is inserted:

- Songplays Table:

![songplays_table](/assets/songplays.png)

- Songs Table:

![songplays_table](/assets/songs.png)

- Artists Table:

![songplays_table](/assets/artists.png)

- User Table:

![songplays_table](/assets/users.png)

- Time Table:

![songplays_table](/assets/time.png)


### Example Analytical Queries:

- ***songs and artist information from log table***

    %sql select a3.name as artist, a2.title as song from songplays a1 join songs a2 on a1.song_id = a2.song_id join artists a3 on a1.artist_id = a3.artist_id

    artist	song

    Elena	Setanta matins

- ***Total number of users present in log table***

    %sql select count(distinct user_id) from songplays

    96

- ***Songs played by Gender***

    %sql select a2.gender, count(a1.songplay_id) from songplays a1 join users a2 on a1.user_id = a2.user_id group by a2.gender

    M	1936

    F	4895
