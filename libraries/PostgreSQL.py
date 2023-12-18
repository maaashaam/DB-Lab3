import psycopg2
import time
import statistics


query = [
    "SELECT vendorid, count(*) FROM taxi GROUP BY 1;",
    "SELECT passenger_count, avg(fare_amount) FROM taxi GROUP BY 1;",
    "SELECT passenger_count, extract(year from tpep_pickup_datetime), count(*) FROM taxi GROUP BY 1, 2;",
    "SELECT passenger_count, extract(year from tpep_pickup_datetime), round(trip_distance), count(*) FROM taxi GROUP BY 1, 2, 3 ORDER BY 2, 4 desc;"
]


def pg_time():

    inf = {
        'host': 'localhost',
        'user': 'postgres',
        'password': '1234',
        'database': 'postgres',
        'port': '5432'
    }

    try:
        connection = psycopg2.connect(**inf)
        cursor = connection.cursor()

        result = []
        for i in range(10):
            start = time.time()
            cursor.execute(query[0])
            finish = time.time()
            result.append(finish - start)
        print("1:", statistics.median(result) * 1000, "ms")
        result.clear()

        for i in range(10):
            start = time.time()
            cursor.execute(query[1])
            finish = time.time()
            result.append(finish - start)
        print("2:", statistics.median(result) * 1000, "ms")
        result.clear()

        for i in range(10):
            start = time.time()
            cursor.execute(query[2])
            finish = time.time()
            result.append(finish - start)
        print("3:", statistics.median(result) * 1000, "ms")
        result.clear()

        for i in range(10):
            start = time.time()
            cursor.execute(query[3])
            finish = time.time()
            result.append(finish - start)
        print("4:", statistics.median(result) * 1000, "ms")
        result.clear()

        cursor.close()
        connection.close()

    except psycopg2.Error as e:
        print('Error PostgreSQL', e)
