import glob
import os
from typing import Callable, List

import pandas as pd
import psycopg2
from numpy import float64, int64
from psycopg2.errors import UniqueViolation
from psycopg2.extensions import connection, cursor

from sql_queries import (
    artist_table_insert,
    song_select,
    song_table_insert,
    songplay_table_insert,
    time_table_insert,
    user_table_insert,
)

# Necessary for df
# pylint: disable=C0103


def fix_numpy_types(row: List) -> List:
    """
    Check types and convert numpy types to standard Python types.

    Args:
        row (List): List to inspect. It must be a list without nested data structures.

    Returns:
        List: Fixed row
    """
    fixed = []
    for elem in row:
        if isinstance(elem, int64):
            fixed.append(int(elem))
        elif isinstance(elem, float64):
            fixed.append(float(elem))
        else:
            fixed.append(elem)

    return fixed


def process_song_file(cur, conn, filepath):
    """
    Process a song dataset's file.

    Args:
        cur (cursor): PostgreSQL cursor
        conn (connection): PostgreSQL connection
        filepath (str): path of the file
    """
    # open song file
    df = pd.read_json(filepath, lines=True)
    df = df.iloc[0]

    # insert song record
    song_data = df[["song_id", "title", "artist_id", "year", "duration"]].values.tolist()
    song_data = fix_numpy_types(song_data)
    try:
        cur.execute(song_table_insert, song_data)
    except UniqueViolation:
        # Can be also managed with conflit policy on SQL query
        print(f"[SONGS] Skipped {song_data[0]} beacuse is already in db.")
    conn.commit()

    # insert artist record
    artist_data = df[
        ["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]
    ].values.tolist()
    artist_data = fix_numpy_types(artist_data)
    try:
        cur.execute(artist_table_insert, artist_data)
    except UniqueViolation:
        print(f"[ARTISTS] Skipped {artist_data[0]} beacuse is already in db.")
    conn.commit()


def process_log_file(cur: cursor, conn: connection, filepath: str):
    """
    Process a log dataset's file.

    Args:
        cur (cursor): PostgreSQL cursor
        conn (connection): PostgreSQL connection
        filepath (str): path of the file
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[df["page"] == "NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df["ts"], unit="ms")
    df["ts"] = t

    # insert time data records
    time_data = [t, t.dt.hour, t.dt.day, t.dt.isocalendar().week, t.dt.month, t.dt.year, t.dt.weekday]
    column_labels = ["Timestamp", "Hour", "Day", "Week of the year", "Month", "Year", "Day of the week"]
    time_df = pd.DataFrame(
        list(zip(time_data[0], time_data[1], time_data[2], time_data[3], time_data[4], time_data[5], time_data[6])),
        columns=column_labels,
    )

    skipped = 0
    for _, row in time_df.iterrows():
        try:
            cur.execute(time_table_insert, list(row))
        except UniqueViolation:
            skipped += 1
        conn.commit()

    if skipped > 0:
        print(f"[TIMESTAMP] Skipped {skipped} items beacuse are already in db.")

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    # insert user records
    skipped = 0
    for _, row in user_df.iterrows():
        try:
            cur.execute(user_table_insert, row)
        except UniqueViolation:
            skipped += 1
        conn.commit()

    if skipped > 0:
        print(f"[USERS] Skipped {skipped} beacuse are already in db.")

    # insert songplay records
    for _, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)
        conn.commit()


def process_data(cur: cursor, conn: connection, filepath: str, func: Callable[[cursor, connection, str], None]):
    """
    Process data from a specific directory. For each file found, process it with the function passed as parameter.

    Args:
        cur (cursor): PostgreSQL cursor
        conn (connection): PostgreSQL connection
        filepath (str): filepath from where to start searching files
        func (Calleble): function to process data
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print(f"{num_files} files found in {filepath}")

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, conn, datafile)
        print(f"{i}/{num_files} files processed. Processed {datafile}")


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    print("Processing songs and artists")
    process_data(cur, conn, filepath="data/song_data", func=process_song_file)
    print("Processig users, time and songs play")
    process_data(cur, conn, filepath="data/log_data", func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
