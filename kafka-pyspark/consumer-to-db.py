import mysql.connector
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum, avg, window, expr, date_format, round
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pymongo import MongoClient


def create_mysql_table():
    connection = mysql.connector.connect(
        host="localhost",
        user="bangmuchlis",
        password="{BangMuchlis123!}",
        database="sensor_data",
        auth_plugin='mysql_native_password'
    )

    cursor = connection.cursor()

    create_table_query_1m = """
    CREATE TABLE IF NOT EXISTS summary_data_1m (
        id INT AUTO_INCREMENT PRIMARY KEY,
        timestamp DATETIME,
        city VARCHAR(255),
        total_watt INT,
        total_volt INT,
        total_amphere FLOAT,
        avg_watt FLOAT,
        avg_volt FLOAT,
        avg_amphere FLOAT
    )
    """

    create_table_query_1h = """
    CREATE TABLE IF NOT EXISTS summary_data_1h (
        id INT AUTO_INCREMENT PRIMARY KEY,
        timestamp DATETIME,
        city VARCHAR(255),
        total_watt INT,
        total_volt INT,
        total_amphere FLOAT,
        avg_watt FLOAT,
        avg_volt FLOAT,
        avg_amphere FLOAT
    )
    """

    create_table_query_1d = """
    CREATE TABLE IF NOT EXISTS summary_data_1d (
        id INT AUTO_INCREMENT PRIMARY KEY,
        timestamp DATETIME,
        city VARCHAR(255),
        total_watt INT,
        total_volt INT,
        total_amphere FLOAT,
        avg_watt FLOAT,
        avg_volt FLOAT,
        avg_amphere FLOAT
    )
    """

    cursor.execute(create_table_query_1m)
    cursor.execute(create_table_query_1h)
    cursor.execute(create_table_query_1d)


    connection.commit()
    connection.close()


def save_to_mysql(batch_df, batch_id, table_name):
    try:

        connection = mysql.connector.connect(
            host="localhost",
            user="bangmuchlis",
            password="{BangMuchlis123!}",
            database="sensor_data",
            auth_plugin='mysql_native_password'
        )

        cursor = connection.cursor()


        for row in batch_df.collect():
            timestamp = row['timestamp']
            city = row['city']
            total_watt = row['total_watt']
            total_volt = row['total_volt']
            total_amphere = row['total_amphere']
            avg_watt = row['avg_watt']
            avg_volt = row['avg_volt']
            avg_amphere = row['avg_amphere']


            insert_query = f"""
            INSERT INTO {table_name} (timestamp, city, total_watt, total_volt, total_amphere, avg_watt, avg_volt, avg_amphere)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """

            cursor.execute(insert_query, (timestamp, city, total_watt, total_volt, total_amphere, avg_watt, avg_volt, avg_amphere))

        connection.commit()
        connection.close()
        print(f"Data successfully written to MySQL table: {table_name}")

    except Exception as e:
        print(f"An error occurred while writing data to MySQL table {table_name}: {str(e)}")


def save_to_mongodb(batch_df, batch_id, collection_name):
    try:

        client = MongoClient("mongodb://localhost:27017/")
        db = client["sensor_data"]
        collection = db[collection_name]


        for row in batch_df.collect():
            document = row.asDict()  


            collection.insert_one(document)

        client.close()
        print(f"Data successfully written to MongoDB collection: {collection_name}")

    except Exception as e:
        print(f"An error occurred while writing data to MongoDB collection {collection_name}: {str(e)}")


def start_streaming(spark, topic_name, bootstrap_servers):
    schema = StructType([
        StructField("date_time", StringType()),
        StructField("id", StringType()),
        StructField("city", StringType()),
        StructField("P (watt)", IntegerType()),
        StructField("V (volt)", IntegerType()),
        StructField("I (amphere)", FloatType())
    ])

    stream_df = (spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", ",".join(bootstrap_servers))
                .option("subscribe", topic_name)
                .option("startingOffsets", "latest")
                .load())

    parsed_stream_df = stream_df.selectExpr("CAST(value AS STRING) as json") \
                                .select(from_json(col("json"), schema).alias("data")) \
                                .select(("data.*"))

    parsed_stream_df = parsed_stream_df.withColumn("date_time", expr("to_timestamp(date_time)"))

    # Define windowed dataframes
    windowed_df_per_minute = parsed_stream_df \
        .withWatermark("date_time", "1 minute") \
        .groupBy(window(col("date_time"), "1 minute").alias("timestamp"), col("city")) \
        .agg(
            sum("P (watt)").alias("total_watt"),
            sum("V (volt)").alias("total_volt"),
            round(sum("I (amphere)"), 2).alias("total_amphere"),
            round(avg("P (watt)"), 2).alias("avg_watt"),
            round(avg("V (volt)"), 2).alias("avg_volt"),
            round(avg("I (amphere)"), 2).alias("avg_amphere")
        ) \
        .withColumn("timestamp", date_format(col("timestamp.start"), "yyyy-MM-dd HH:mm:ss"))

    windowed_df_per_hour = parsed_stream_df \
        .withWatermark("date_time", "1 hour") \
        .groupBy(window(col("date_time"), "1 hour").alias("timestamp"), col("city")) \
        .agg(
            sum("P (watt)").alias("total_watt"),
            sum("V (volt)").alias("total_volt"),
            round(sum("I (amphere)"), 2).alias("total_amphere"),
            round(avg("P (watt)"), 2).alias("avg_watt"),
            round(avg("V (volt)"), 2).alias("avg_volt"),
            round(avg("I (amphere)"), 2).alias("avg_amphere")
        ) \
        .withColumn("timestamp", date_format(col("timestamp.start"), "yyyy-MM-dd HH:mm:ss"))

    windowed_df_per_day = parsed_stream_df \
        .withWatermark("date_time", "1 day") \
        .groupBy(window(col("date_time"), "1 day", "1 day", "0 day").alias("timestamp"), col("city")) \
        .agg(
            sum("P (watt)").alias("total_watt"),
            sum("V (volt)").alias("total_volt"),
            round(sum("I (amphere)"), 2).alias("total_amphere"),
            round(avg("P (watt)"), 2).alias("avg_watt"),
            round(avg("V (volt)"), 2).alias("avg_volt"),
            round(avg("I (amphere)"), 2).alias("avg_amphere")
        ) \
        .withColumn("timestamp", date_format(col("timestamp.start"), "yyyy-MM-dd HH:mm:ss"))

    # Write raw data to MongoDB
    query_mongodb = (parsed_stream_df.writeStream
                    .foreachBatch(lambda df, id: save_to_mongodb(df, id, "raw_data"))
                    .outputMode('append')
                    .start())

    # Write windowed data to MySQL
    query_minute_mysql = (windowed_df_per_minute.writeStream
                        .foreachBatch(lambda df, id: save_to_mysql(df, id, "summary_data_1m"))
                        .outputMode('update')
                        .trigger(processingTime='1 minute')
                        .start())
    


    query_hour_mysql = (windowed_df_per_hour.writeStream
                        .foreachBatch(lambda df, id: save_to_mysql(df, id, "summary_data_1h"))
                        .outputMode('update')
                        .trigger(processingTime='1 hour')
                        .start())

    query_day_mysql = (windowed_df_per_day.writeStream
                    .foreachBatch(lambda df, id: save_to_mysql(df, id, "summary_data_1d"))
                    .outputMode('update')
                    .trigger(processingTime='1 day')
                    .start())

    query_mongodb.awaitTermination()
    query_minute_mysql.awaitTermination()
    query_hour_mysql.awaitTermination()
    query_day_mysql.awaitTermination()

if __name__ == "__main__":

    spark_conn = SparkSession.builder.appName("KafkaStreamConsumer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .getOrCreate()

    create_mysql_table()

    topic_name = 'test_topic'
    bootstrap_servers = ['localhost:9092']

    spark_conn.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")

    try:
        start_streaming(spark_conn, topic_name, bootstrap_servers)
    except Exception as e:
        print("An error occurred while streaming:", str(e))
