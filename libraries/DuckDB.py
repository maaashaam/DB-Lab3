import duckdb
import time
import statistics


def duck_time():
    try:
        connection = duckdb.connect()

        table = "data_wo_unnamed.csv"

        result = []
        for i in range(10):
            start = time.time()
            connection.execute(f"SELECT VendorID, count(*) FROM {table} GROUP BY 1;")
            finish = time.time()
            result.append(finish - start)
        print("1:", statistics.median(result) * 1000, "ms")
        result.clear()

        for i in range(10):
            start = time.time()
            connection.execute(f"SELECT passenger_count, avg(fare_amount) FROM {table} GROUP BY 1;")
            finish = time.time()
            result.append(finish - start)
        print("2:", statistics.median(result) * 1000, "ms")
        result.clear()

        for i in range(10):
            start = time.time()
            connection.execute(f"SELECT passenger_count, extract(year from tpep_pickup_datetime), count(*) FROM {table} GROUP BY 1, 2;")
            finish = time.time()
            result.append(finish - start)
        print("3:", statistics.median(result) * 1000, "ms")
        result.clear()

        for i in range(10):
            start = time.time()
            connection.execute(f"SELECT passenger_count, extract(year from tpep_pickup_datetime), round(trip_distance), count(*) FROM {table} GROUP BY 1, 2, 3 ORDER BY 2, 4 desc;")
            finish = time.time()
            result.append(finish - start)
        print("4:", statistics.median(result) * 1000, "ms")
        result.clear()

        connection.close()

    except duckdb.Error as e:
        print("Error DuckDB", e)
