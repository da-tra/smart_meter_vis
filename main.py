import requests
import sqlite3
import pyarrow as pa
from pathlib import Path
from pprint import pprint



def importdb(db):
     conn = sqlite3.connect(db)
     c = conn.cursor()
     c.execute("SELECT name FROM sqlite_master WHERE type='table';")
     for table in c.fetchall():
         yield list(c.execute('SELECT * from ?;', (table[0],)))

pprint(importdb(db="./usage_data.db"))
