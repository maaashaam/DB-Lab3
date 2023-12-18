from sqlalchemy import create_engine, URL
from sqlalchemy.sql import text
import statistics
import time


host = "localhost"
user = "postgres"
password = "1234"
database = "postgres"
port = "5432"

query = [
    "SELECT vendorid, count(*) FROM taxi GROUP BY 1;",
    "SELECT passenger_count, avg(total_amount) FROM taxi GROUP BY 1;",
    "SELECT passenger_count, extract(year from tpep_pickup_datetime), count(*) FROM taxi GROUP BY 1, 2;",
    "SELECT passenger_count, extract(year from tpep_pickup_datetime), round(trip_distance), count(*) FROM taxi GROUP BY 1, 2, 3 ORDER BY 2, 4 desc;"
]


def alch_time():
    try:
        url = URL.create(
            "postgresql",
            username=user,
            password=password,
            host=host,
            port=port,
            database=database
        )

        engine = create_engine(url)
        connection = engine.connect()

        result = []

        for i in range(10):
            start = time.time()
            connection.execute(text(query[0]))
            finish = time.time()
            result.append(finish - start)
        print("1:", statistics.median(result) * 1000, "ms")
        result.clear()

        for i in range(10):
            start = time.time()
            connection.execute(text(query[1]))
            finish = time.time()
            result.append(finish - start)
        print("2:", statistics.median(result) * 1000, "ms")
        result.clear()

        for i in range(10):
            start = time.time()
            connection.execute(text(query[2]))
            finish = time.time()
            result.append(finish - start)
        print("3:", statistics.median(result) * 1000, "ms")
        result.clear()

        for i in range(10):
            start = time.time()
            connection.execute(text(query[3]))
            finish = time.time()
            result.append(finish - start)
        print("4:", statistics.median(result) * 1000, "ms")
        result.clear()

        connection.close()

    except create_engine.Error as e:
        print("Error SQLAlchemy", e)
