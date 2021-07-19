import glob
import logging
import os
from typing import List

import pandas as pd
import psycopg2
from numpy import float64, int64
from psycopg2.errors import UniqueViolation

from sql_queries import (
    artist_table_insert,
    song_table_insert,
    songplay_table_insert,
    time_table_insert,
    user_table_insert,
)

log = logging.getLogger("ETL")

# Necessary for df
# pylint: disable=C0103


def fix_types(row: List) -> List:
    """
    Check types and convert numpy types to standard Python types.

    Args:
        row (List): List to inspect

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
    # open song file
    df = pd.read_json(filepath, lines=True)
    df = df.iloc[0]

    # insert song record
    song_data = df[["song_id", "title", "artist_id", "year", "duration"]].values.tolist()
    song_data = fix_types(song_data)
    try:
        cur.execute(song_table_insert, song_data)
    except UniqueViolation:
        print(f"Skipped {song_data[0]} beacuse is already in db.")
    conn.commit()

    # insert artist record
    artist_data = df[
        ["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]
    ].values.tolist()
    artist_data = fix_types(artist_data)
    try:
        cur.execute(artist_table_insert, artist_data)
    except UniqueViolation:
        print(f"Skipped {artist_data[0]} beacuse is already in db.")
    conn.commit()


# def process_log_file(cur, filepath):
#     # open log file
#     df =

#     # filter by NextSong action
#     df =

#     # convert timestamp column to datetime
#     t =

#     # insert time data records
#     time_data =
#     column_labels =
#     time_df =

#     for i, row in time_df.iterrows():
#         cur.execute(time_table_insert, list(row))

#     # load user table
#     user_df =

#     # insert user records
#     for i, row in user_df.iterrows():
#         cur.execute(user_table_insert, row)

#     # insert songplay records
#     for index, row in df.iterrows():

#         # get songid and artistid from song and artist tables
#         cur.execute(song_select, (row.song, row.artist, row.length))
#         results = cur.fetchone()

#         if results:
#             songid, artistid = results
#         else:
#             songid, artistid = None, None

#         # insert songplay record
#         songplay_data =
#         cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
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

    process_data(cur, conn, filepath="data/song_data", func=process_song_file)
    # process_data(cur, conn, filepath="data/log_data", func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
