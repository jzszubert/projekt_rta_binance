import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark.sql.functions import col, from_json, window, sum as _sum, round as _round, expr, lit
import sqlite3

if pyspark.__version__.startswith("4"):
    KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0-preview2"
else:
    KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"

os.environ["PYSPARK_SUBMIT_ARGS"] = f"--packages {KAFKA_PACKAGE} pyspark-shell"

spark = (
    SparkSession.builder
    .appName("spark_vwap")
    .config("spark.jars.packages", KAFKA_PACKAGE)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

DB_PATH = "rta.db"

trade_schema = StructType([
    StructField("symbol",       StringType()),
    StructField("trade_time",   LongType()),
    StructField("price",        StringType()),
    StructField("quantity",     StringType()),
    StructField("market_maker", StringType()),
    StructField("trade_id",     LongType()),
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
    save_to_db(df, "vwap_history")

kafka_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "trades")
    .load()
)

df = (
    kafka_raw
    .select(from_json(col("value").cast("string"), trade_schema).alias("tx"))
    .select("tx.*")
    .withColumn("price",    col("price").cast(DoubleType()))
    .withColumn("quantity", col("quantity").cast(DoubleType()))
    .withColumn("trade_ts", (col("trade_time") / 1000).cast("timestamp"))
)

def vwap_window(duration, etykieta):
    return (
        df
        .withWatermark("trade_ts", "30 seconds")
        .groupBy(window("trade_ts", duration), "symbol")
        .agg(
            _round(_sum(expr("price * quantity")) / _sum("quantity"), 2).alias("vwap"),
            _round(_sum("quantity"), 4).alias("total_volume"),
        )
        .select(
            col("symbol"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            lit(etykieta).alias("window_len"),
            col("vwap"),
            col("total_volume"),
        )
    )

q1 = (
    vwap_window("1 minute", "1 min").writeStream
    .outputMode("update")
    .foreachBatch(process_batch)
    .option("checkpointLocation", "/tmp/checkpoints/vwap_1min")
    .start()
)

q5 = (
    vwap_window("5 minutes", "5 min").writeStream
    .outputMode("update")
    .foreachBatch(process_batch)
    .option("checkpointLocation", "/tmp/checkpoints/vwap_5min")
    .start()
)

spark.streams.awaitAnyTermination()