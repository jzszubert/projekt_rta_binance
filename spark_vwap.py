## spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 spark_vwap.py

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark.sql.functions import (
    col, from_json, window,
    sum as _sum, round as _round, expr
)

KAFKA_BROKER = "broker:9092"
KAFKA_TOPIC  = "trades"

# schemat zgodny z FIELD_MAPPING['@trade'] z producer_binance.py
trade_schema = StructType([
    StructField("symbol",       StringType()),
    StructField("trade_time",   LongType()),
    StructField("price",        StringType()),
    StructField("quantity",     StringType()),
    StructField("market_maker", StringType()),
    StructField("trade_id",     LongType()),
])

spark = (
    SparkSession.builder
    .appName("spark_vwap")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


def process_batch(df, batch_id):
    print(f"Batch ID: {batch_id}")
    df.show(truncate=False)


# Krok 1: surowe bajty z Kafki
kafka_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

# Krok 2: bajty → JSON → płaskie kolumny (tak jak w Lab 4)
df = (
    kafka_raw
    .select(from_json(col("value").cast("string"), trade_schema).alias("tx"))
    .select("tx.*")
    .withColumn("price",    col("price").cast(DoubleType()))
    .withColumn("quantity", col("quantity").cast(DoubleType()))
    .withColumn("trade_ts", (col("trade_time") / 1000).cast("timestamp"))
)

# Krok 3: VWAP w oknie tumbling 1 minuta
# VWAP = suma(cena * wolumen) / suma(wolumen)
vwap = (
    df
    .withWatermark("trade_ts", "30 seconds")
    .groupBy(window("trade_ts", "1 minute"), "symbol")
    .agg(
        _round(_sum(expr("price * quantity")) / _sum("quantity"), 2).alias("vwap"),
        _round(_sum("quantity"), 4).alias("total_volume"),
    )
    .select(
        col("symbol"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("vwap"),
        col("total_volume"),
    )
)

query = (
    vwap.writeStream
    .format("console")
    .outputMode("update")
    .foreachBatch(process_batch)
    .option("truncate", False)
    .option("checkpointLocation", "/tmp/checkpoints/vwap")
    .start()
)

query.awaitTermination()
