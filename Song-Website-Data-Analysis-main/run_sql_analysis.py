import sqlite3
import pandas as pd
import os
from pathlib import Path

def create_database_and_tables():
    conn = sqlite3.connect('song_analysis.db')
    cursor = conn.cursor()
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS EVENTS (
        ARTIST_NAME VARCHAR(200), 
        USER_AUTHONTICATION VARCHAR(200), 
        USER_FIRST_NAME VARCHAR(200),
        USER_GENDER VARCHAR(5),
        NO_ITEMS_IN_SESSION NUMERIC(10),
        USER_LAST_NAME VARCHAR(200), 
        SONG_LENGTH_IN_SECONDS NUMERIC(26, 6), 
        SONG_LEVEL VARCHAR(200), 
        USER_LOCATION VARCHAR(200), 
        SONG_METHOD VARCHAR(200), 	
        SONG_PLAYED VARCHAR(200), 
        USER_REGESTRATION_TIME_IN_SECONDS NUMERIC(26, 6), 
        SESSION_ID NUMERIC(5), 
        SONG_NAME VARCHAR(200), 
        SONG_STATUS NUMERIC(5), 
        TIME_IN_SECONDS_OF_PLAYING_SONG NUMERIC(26, 6), 
        USER_AGENT VARCHAR(400), 
        USER_ID NUMERIC(10)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS SONGS (
        ARTIST_ID VARCHAR(100), 
        ARTIST_LATITUDE NUMERIC(20, 6),
        ARTIST_LOCATION VARCHAR(100),
        ARTIST_LONGTUDE NUMERIC(20, 6), 
        ARTIST_NAME VARCHAR(100), 
        SONG_DURATION_IN_SECONDS NUMERIC(20,6),
        ARTIST_NUM_OF_SONGS NUMERIC(5), 
        SONG_ID VARCHAR(100),
        SONG_NAME VARCHAR(100),
        SONG_REALASED_YEAR NUMERIC(5)
    )
    ''')
    
    conn.commit()
    return conn

def load_csv_data(conn):
    print("Loading CSV data into database...")
    
    events_df = pd.read_csv('Song-Website-Data-Analysis-main/Data/events.csv')
    events_df.columns = [
        'ARTIST_NAME', 'USER_AUTHONTICATION', 'USER_FIRST_NAME', 'USER_GENDER',
        'NO_ITEMS_IN_SESSION', 'USER_LAST_NAME', 'SONG_LENGTH_IN_SECONDS', 'SONG_LEVEL',
        'USER_LOCATION', 'SONG_METHOD', 'SONG_PLAYED', 'USER_REGESTRATION_TIME_IN_SECONDS',
        'SESSION_ID', 'SONG_NAME', 'SONG_STATUS', 'TIME_IN_SECONDS_OF_PLAYING_SONG',
        'USER_AGENT', 'USER_ID'
    ]
    events_df.to_sql('EVENTS', conn, if_exists='replace', index=False)
    
    songs_df = pd.read_csv('Song-Website-Data-Analysis-main/Data/songs.csv')
    songs_df.columns = [
        'ARTIST_ID', 'ARTIST_LATITUDE', 'ARTIST_LOCATION', 'ARTIST_LONGTUDE',
        'ARTIST_NAME', 'SONG_DURATION_IN_SECONDS', 'ARTIST_NUM_OF_SONGS', 'SONG_ID',
        'SONG_NAME', 'SONG_REALASED_YEAR'
    ]
    songs_df.to_sql('SONGS', conn, if_exists='replace', index=False)
    
    print("Data loaded successfully!")
    return conn

def execute_query(conn, query, query_name):
    print(f"\n{'='*60}")
    print(f"QUERY: {query_name}")
    print(f"{'='*60}")
    
    try:
        result = pd.read_sql_query(query, conn)
        print(f"Results ({len(result)} rows):")
        print(result.head(10))
        if len(result) > 10:
            print(f"... and {len(result) - 10} more rows")
        return result
    except Exception as e:
        print(f"Error executing query: {e}")
        return None

def main():
    print("Song Website Data Analysis - SQL Implementation")
    print("=" * 60)
    
    conn = create_database_and_tables()
    conn = load_csv_data(conn)
    queries = {
        "Query 1 - Artists by User Count": """
        SELECT DISTINCT * FROM
        (SELECT E.ARTIST_NAME, S.ARTIST_ID, ARTIST_LOCATION, ARTIST_LATITUDE, ARTIST_LONGTUDE
        , COUNT(E.USER_ID) OVER(PARTITION BY E.ARTIST_NAME) USERS_NUMBER
        FROM EVENTS E, SONGS S
        WHERE E.ARTIST_NAME = S.ARTIST_NAME) USER_SUB_Q 
        ORDER BY USERS_NUMBER DESC
        """,
        
        "Query 2 - Most Played Songs": """
        SELECT * FROM
        (
         SELECT SONG_ID, S.SONG_NAME, S.ARTIST_ID, E.ARTIST_NAME, E.SONG_LENGTH_IN_SECONDS, 
        COUNT(USER_ID) OVER(PARTITION BY SONG_ID) USERS_NUMBER
        FROM EVENTS E, SONGS S
        WHERE E.SONG_NAME = S.SONG_NAME AND SONG_PLAYED = 'NextSong'
        	) SUB_QUERY ORDER BY USERS_NUMBER DESC
        """,
        
        "Query 3 - Most Played Songs with Rankings": """
        SELECT * , DENSE_RANK() OVER(ORDER BY USERS_NUMBER DESC) as RANK FROM
        (SELECT DISTINCT SONG_NAME, ARTIST_NAME, SONG_LEVEL, COUNT(USER_ID) OVER (PARTITION BY SONG_NAME) USERS_NUMBER
        FROM EVENTS
        WHERE SONG_PLAYED = 'NextSong') SUB_QUERY_1
        WHERE SONG_NAME IS NOT NULL
        """,
        
        "Query 4 - Song Rankings by Session": """
        SELECT * From
        (
        SELECT  DISTINCT SESSION_ID, SONG_NAME, USERS_NUMBER
        , ARTIST_NAME, SONG_LEVEL, DENSE_RANK()  OVER(PARTITION BY SESSION_ID ORDER BY USERS_NUMBER DESC) SONG_RANK FROM
        (SELECT SESSION_ID, SONG_NAME, COUNT(USER_ID) OVER(PARTITION BY SESSION_ID, SONG_NAME) USERS_NUMBER, ARTIST_NAME, SONG_LEVEL
        FROM EVENTS
        WHERE SONG_NAME IS NOT NULL AND SONG_PLAYED = 'NextSong'
         ) SUB_QUERY) SUB_QUERY_2
         GROUP BY SESSION_ID, SONG_NAME, SONG_LEVEL, USERS_NUMBER, ARTIST_NAME, SONG_RANK
         ORDER BY SESSION_ID, SONG_RANK
        """,
        
        "Query 5 - Artists by Song Count": """
        SELECT *, DENSE_RANK() OVER(ORDER BY SONGS_NUMBER DESC) as RANK FROM 
        (
        SELECT DISTINCT ARTIST_NAME, SONG_NAME, COUNT(SONG_NAME) OVER(PARTITION BY ARTIST_NAME) SONGS_NUMBER
        FROM EVENTS
        WHERE SONG_NAME IS NOT NULL) SUB_QUERY
        """,
        
        "Query 6 - User Duration Analysis": """
        SELECT USER_ID, DENSE_RANK() OVER(ORDER BY USER_DURATION_IN_SECONDS DESC) as RANK,
        USER_FIRST_NAME, USER_LAST_NAME, USER_DURATION_IN_SECONDS 
        FROM 
        (
        SELECT DISTINCT USER_ID, USER_FIRST_NAME, USER_LAST_NAME, 
        SUM(SONG_LENGTH_IN_SECONDS) OVER(PARTITION BY USER_ID) USER_DURATION_IN_SECONDS
        FROM EVENTS
        WHERE SONG_NAME IS NOT NULL AND SONG_PLAYED = 'NextSong') SUB_QUERY
        """
    }
    
    results = {}
    for query_name, query in queries.items():
        results[query_name] = execute_query(conn, query, query_name)
    print(f"\n{'='*60}")
    print("SAVING RESULTS TO CSV FILES")
    print(f"{'='*60}")
    
    for query_name, result in results.items():
        if result is not None:
            filename = f"results_{query_name.lower().replace(' ', '_').replace('-', '_')}.csv"
            result.to_csv(filename, index=False)
            print(f"Saved: {filename}")
    
    conn.close()
    print(f"\n{'='*60}")
    print("ANALYSIS COMPLETE!")
    print("All results have been saved to CSV files.")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()
