from libraries.PostgreSQL import pg_time
from libraries.SQLite import sqlite_time
from libraries.DuckDB import duck_time
from libraries.Pandas import pand_time
from libraries.SQLAlchemy import alch_time

print("Select a library\n"
      "1) PostgreSQL\n"
      "2) SQLite\n"
      "3) DuckDB\n"
      "4) Pandas\n"
      "5) SQLAlchemy\n"
      "To end the program press 6")

number = int(input())

while number != 6:
    match number:
        case 1:
            pg_time()
        case 2:
            sqlite_time()
        case 3:
            duck_time()
        case 4:
            pand_time()
        case 5:
            alch_time()
    number = int(input())
