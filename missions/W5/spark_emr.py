from pyspark.sql import SparkSession
from pyspark import SparkContext
from datetime import datetime

if __name__ == "__main__":
    if SparkContext._active_spark_context:
        SparkContext._active_spark_context.stop()

    spark = SparkSession.builder \
            .appName("NYC Taxi Analysis") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.executor.cores", "4") \
            .config("spark.driver.cores", "4") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .getOrCreate()
    
    df = spark.read.parquet("/data/yellow_tripdata_2024-11.parquet")
    # df = spark.read.parquet("s3://aws-seoul-softeerbucket/yellow_tripdata_2024-11.parquet")
    rdd = df.rdd

    # 누락된 데이터 제거
    rdd = rdd.filter(lambda row : all(value is not None for value in row.asDict().values()))

    # 요금이 0이상 필터링
    rdd = rdd.filter(lambda row : row['fare_amount'] > 0)

    # 특정 컬럼만 선택하기
    selected_rdd = rdd.map(lambda row : (row['fare_amount'], row['tpep_pickup_datetime'].strftime('%Y-%m-%d'), row['trip_distance']))

    # 총 수익 계산
    total_revenue = selected_rdd.map(lambda row : row[0]).reduce(lambda x,y : x+y)

    # 총 여행 수 계산
    total_trips = selected_rdd.count()

    daily_rdd = selected_rdd.map(lambda x : (x[1], (x[0],1))).reduceByKey(lambda a, b:(a[0]+b[0],a[1]+b[1]))
    average_trip_distance = selected_rdd.map(lambda x : float(x[2])).mean()

    print("Total Number of Trips : {total}".format(total=total_trips))
    print("Total Revenue : {total}".format(total=total_revenue))
    print("Average trip distance : {total}".format(total = average_trip_distance))
    print("날짜 / the total revenue per day / Number of Trips per day")
    print(daily_rdd.take(10))

    daily_df = daily_rdd.map(lambda x: (x[0], x[1][0], x[1][1])).toDF(["date", "daily_revenue", "trip_count"])

    # CSV 파일로 저장 (파티션 1개로 통합)
    # daily_df.coalesce(1) \
    #     .write \
    #     .mode("overwrite") \
    #     .option("header", "true") \
    #     .csv("s3://aws-seoul-softeerbucket/output/result.csv")

    daily_df.coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv("result.csv")

    spark.stop()