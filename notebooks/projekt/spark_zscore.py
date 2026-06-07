import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, BooleanType,
)
from pyspark.sql.functions import (
    col, from_json, window, avg, stddev, max as _max,
    round as _round, when, lit, to_json, struct,
)
import sqlite3

if pyspark.__version__.startswith("4"):
    KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0-preview2"
else:
    KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"

os.environ["PYSPARK_SUBMIT_ARGS"] = f"--packages {KAFKA_PACKAGE} pyspark-shell"

spark = (
    SparkSession.builder
    .appName("spark_zscore")
    .config("spark.jars.packages", KAFKA_PACKAGE)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

DB_PATH = "rta.db"

kline_schema = StructType([
    StructField("symbol",           StringType()),
    StructField("start_time",       LongType()),
    StructField("open_price",       StringType()),
    StructField("high_price",       StringType()),
    StructField("low_price",        StringType()),
    StructField("close_price",      StringType()),
    StructField("volume",           StringType()),
    StructField("taker_buy_volume", StringType()),
    StructField("is_closed",        BooleanType()),
])

def save_to_db(df, table):
    pdf = df.toPandas()
    if pdf.empty:
        return
    conn = sqlite3.connect(DB_PATH, timeout=30)
    try:
        pdf.to_sql(table, conn, if_exists="append", index=False)
    finally:
        conn.close()

def process_batch(df, batch_id):
    print(f"Batch ID: {batch_id}")
    df.show(truncate=False)

    save_to_db(df, "volume_zscore_history")

    alerts = (
        df
        .filter(col("is_anomaly") == True)
        .select(
            to_json(
                struct(
                    "symbol",
                    col("window_start").cast("string"),
                    col("window_end").cast("string"),
                    "avg_volume", "max_volume", "zscore", "ratio",
                    lit("VOLUME_SPIKE").alias("alert_type"),
                )
            ).alias("value")
        )
    )

    if not alerts.rdd.isEmpty():
        (alerts.write
            .format("kafka")
            .option("kafka.bootstrap.servers", "broker:9092")
            .option("topic", "alerts")
            .save())
        print("  -> alerty wyslane do tematu 'alerts'")

kafka_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "klines")
    .load()
)

df = (
    kafka_raw
    .select(from_json(col("value").cast("string"), kline_schema).alias("tx"))
    .select("tx.*")
    .filter(col("is_closed") == True)
    .withColumn("volume", col("volume").cast(DoubleType()))
    .withColumn("start_ts", (col("start_time") / 1000).cast("timestamp"))
)

agg = (
    df
    .withWatermark("start_ts", "2 minutes")
    .groupBy(window("start_ts", "10 minutes", "1 minute"), "symbol")
    .agg(
        _round(avg("volume"), 2).alias("avg_volume"),
        _round(stddev("volume"), 2).alias("std_volume"),
        _max("volume").alias("max_volume"),
    )
)

scored = (
    agg
    .withColumn(
        "zscore",
        when(col("std_volume") > 0,
             _round((col("max_volume") - col("avg_volume")) / col("std_volume"), 2))
        .otherwise(lit(0.0))
    )
    .withColumn("ratio", _round(col("max_volume") / col("avg_volume"), 2))
    .withColumn("is_anomaly", col("zscore") > 2.5)
    .select(
        col("symbol"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("avg_volume"),
        col("max_volume"),
        col("std_volume"),
        col("zscore"),
        col("ratio"),
        col("is_anomaly"),
    )
)

query = (
    scored.writeStream
    .outputMode("update")
    .foreachBatch(process_batch)
    .option("checkpointLocation", "/tmp/checkpoints/zscore")
    .start()
)

query.awaitTermination()