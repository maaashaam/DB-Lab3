import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import URL
import time
import statistics


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


def pand_time():
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

        result = []

        for i in range(10):
            start = time.time()
            pd.read_sql(query[0], con=engine)
            finish = time.time()
            result.append(finish - start)
        print("1:", statistics.median(result) * 1000, "ms")
        result.clear()

        for i in range(10):
            start = time.time()
            pd.read_sql(query[1], con=engine)
            finish = time.time()
            result.append(finish - start)
        print("2:", statistics.median(result) * 1000, "ms")
        result.clear()

        for i in range(10):
            start = time.time()
            pd.read_sql(query[2], con=engine)
            finish = time.time()
            result.append(finish - start)
        print("3:", statistics.median(result) * 1000, "ms")
        result.clear()

        for i in range(10):
            start = time.time()
            pd.read_sql(query[3], con=engine)
            finish = time.time()
            result.append(finish - start)
        print("4:", statistics.median(result) * 1000, "ms")
        result.clear()

        engine.dispose()

    except create_engine.Error as e:
        print('Error Pandas', e)
