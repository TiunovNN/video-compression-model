import logging
import os
import sqlite3
import csv
from io import StringIO

import click
import requests


@click.command()
@click.option('--csv-input', type=click.STRING, required=True)
@click.option('--db-name', type=click.STRING, default='upload_progress.db')
def main(csv_input: str, db_name: str):
    con = sqlite3.connect(db_name)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute('CREATE TABLE IF NOT EXISTS upload_progress (src TEXT PRIMARY KEY, dst TEXT, success BOOLEAN)')
    con.commit()
    with requests.get(csv_input) as response:
        response.raise_for_status()
        reader = csv.DictReader(StringIO(response.text), delimiter=';')
        items = (
            (
                f'https://storage.googleapis.com/ugc-dataset/original_videos/{row["category"]}/{row["resolution"]}P/{row["vid"]}.mkv',
                f'{row["category"]}/{row["resolution"]}P/{row["vid"]}.mkv',
            )
            for row in reader
        )
        cur.executemany('INSERT INTO upload_progress (src, dst) VALUES (?, ?)', items)
        con.commit()


if __name__ == '__main__':
    main()
