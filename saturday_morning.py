import sqlite3
import os
from random import choices
import pandas as pd
from datetime import date


# When run, this script will generate a sqlite database in the working directory, as well as a .vlc playlist file.

# Set your directory parameters here
cartoon_dir = r'E:\TV Shows'
commercial_dir = r'E:\Bryce'
# Set the desired output directory for your playlist and sqlite database
os.setcwd = r'C:\Users\Erik\Documents\GitHub\saturday_morning'
# Specify desired file extensions
file_extensions = ['avi', 'mpg', 'mp4']
# Specify number of episodes per playlist and commercials per intermission
num_episodes = 6
num_commercials = 2

# Initialize connection
conn = sqlite3.connect('cartoons.db')
c = conn.cursor()
c.execute('''SELECT name FROM sqlite_master WHERE TYPE = 'table';''')

tables = c.fetchall()
tables = [i[0] for i in tables]

if tables is None:
    tables = []


def crawl_dirs(directory, table, cursor, connection):
    '''Crawl through media directories and add filepaths not found in the existing database'''
    existing_qry = cursor.execute(f'''SELECT filepath FROM {table}''')
    existing_files = existing_qry.fetchall()
    existing_files = [i[0] for i in existing_files]
    for root, dirs, files in os.walk(directory, topdown=False):
        for name in files:
            if name.rsplit('.', maxsplit=1)[1] in file_extensions:
                filepath = os.path.join(root, name)
                if filepath not in existing_files:
                    last_played = 'NULL'
                    play_count = 0
                    cursor.execute(f'''INSERT INTO {table} VALUES (:last_played, :filepath, :play_count)''',
                                   {'last_played': last_played,
                                    'filepath': filepath,
                                    'play_count': play_count})
    connection.commit()


# Construct tables if not already found in database
if 'cartoons' not in tables:
    c.execute('''CREATE TABLE cartoons
                 (last_accessed date, filepath text, play_count real)''')
    conn.commit()
if 'commercials' not in tables:
    c.execute('''CREATE TABLE commercials
                 (last_accessed date, filepath text, play_count real)''')
    conn.commit()

# Crawl directories and look for new files
crawl_dirs(cartoon_dir, 'cartoons', c, conn)
crawl_dirs(commercial_dir, 'commercials', c, conn)

with open('playlist.vlc', 'w') as playlist:

    def construct_playlist(playlist):
        '''Main function to construct playlist'''
        playlist.write('[playlist]\n')
        playlist.write(
            f'NumberOfEntries={num_episodes * (num_commercials + 1)}\n')
        playlist_lines = []

        def add_to_playlist(table, playlist_lines, i):
            '''Generates individual lines to be appended'''
            i += 1
            min_play_qry = c.execute(f"SELECT MIN(play_count) FROM {table}")
            min_play_count = min_play_qry.fetchone()[0]

            least_played = c.execute(f'''SELECT * FROM {table} WHERE play_count = :min_play''',
                                     {'min_play': min_play_count})
            least_df = pd.DataFrame(least_played.fetchall(),
                                    columns=['last_played', 'filepath', 'play_count'])

            chosen_index = choices(least_df.index)
            chosen_row = pd.Series(
                least_df.iloc[chosen_index].to_dict('records')[0])
            filepath = chosen_row.loc['filepath']
            play_count = int(chosen_row.loc['play_count']) + 1

            sql = f'''UPDATE {table}
                      SET last_accessed = :today ,
                          play_count = :play_count
                      WHERE filepath = :filepath;'''
            c.execute(sql, {'today': date.today(),
                            'play_count': play_count, 'filepath': filepath})
            conn.commit()
            playlist_lines += [f'''File{i}={filepath}\n''']
            return playlist_lines, i

        for i in [i*(num_commercials + 1) for i in range(num_episodes)]:
            playlist_lines, i = add_to_playlist('cartoons', playlist_lines, i)
            for j in range(num_commercials):
                playlist_lines, i = add_to_playlist(
                    'commercials', playlist_lines, i)

        playlist.writelines(playlist_lines)
        playlist.close()

    construct_playlist(playlist)

conn.close()
