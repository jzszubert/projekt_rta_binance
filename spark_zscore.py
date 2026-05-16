## spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 spark_zscore.py

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, BooleanType
from pyspark.sql.functions import (
    col, from_json, window,
    avg, stddev,
    round as _round, to_json, struct, lit, when
)

KAFKA_BROKER       = "broker:9092"
KAFKA_TOPIC_IN     = "klines"
KAFKA_TOPIC_ALERTS = "alerts"
ZSCORE_THRESHOLD   = 2.5

# schemat zgodny z FIELD_MAPPING['@kline_1m'] z producer_binance.py
kline_schema = StructType([
    StructField("symbol",          StringType()),
    StructField("start_time",      LongType()),
    StructField("open_price",      StringType()),
    StructField("high_price",      StringType()),
    StructField("low_price",       StringType()),
    StructField("close_price",     StringType()),
    StructField("volume",          StringType()),
    StructField("taker_buy_volume", StringType()),
    StructField("is_closed",       BooleanType()),
])

spark = (
    SparkSession.builder
    .appName("spark_zscore")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


def process_batch(df, batch_id):
    print(f"Batch ID: {batch_id}")
    df.show(truncate=False)

    # transakcje z anomalią wysyłamy do tematu alerts (tak jak w Lab 4, Część 5)
    alerts = (
        df
        .filter(col("is_anomaly") == True)
        .select(
            to_json(
                struct(
                    "symbol", "window_start", "window_end",
                    "avg_volume", "zscore",
                    lit("VOLUME_SPIKE").alias("alert_type"),
                )
            ).alias("value")
        )
    )

    if not alerts.rdd.isEmpty():
        (
            alerts.write
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BROKER)
            .option("topic", KAFKA_TOPIC_ALERTS)
            .save()
        )
        print(f"  -> alerty wyslane do tematu '{KAFKA_TOPIC_ALERTS}'")


# Krok 1: surowe bajty z Kafki
kafka_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", KAFKA_TOPIC_IN)
    .option("startingOffsets", "latest")
    .load()
)

# Krok 2: bajty → JSON → płaskie kolumny
df = (
    kafka_raw
    .select(from_json(col("value").cast("string"), kline_schema).alias("tx"))
    .select("tx.*")
    .filter(col("is_closed") == True)           # tylko zamknięte świece
    .withColumn("volume",   col("volume").cast(DoubleType()))
    .withColumn("start_ts", (col("start_time") / 1000).cast("timestamp"))
)

# Krok 3: średnia i odchylenie wolumenu w oknie 5 minut (sliding 1 min)
agg = (
    df
    .withWatermark("start_ts", "2 minutes")
    .groupBy(window("start_ts", "5 minutes", "1 minute"), "symbol")
    .agg(
        _round(avg("volume"),    4).alias("avg_volume"),
        _round(stddev("volume"), 4).alias("std_volume"),
    )
    .select(
        col("symbol"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("avg_volume"),
        col("std_volume"),
    )
)

# Krok 4: z-score i oznaczenie anomalii
# z-score = avg_volume / std_volume  — uproszczona miara skoku względem okna
# (std_volume blisko zera = wolumen stabilny; duże std = wysoki rozrzut = anomalia)
agg = (
    agg
    .withColumn(
        "zscore",
        when(col("std_volume") > 0, _round(col("avg_volume") / col("std_volume"), 4))
        .otherwise(lit(0.0))
    )
    .withColumn(
        "is_anomaly",
        col("zscore") > lit(ZSCORE_THRESHOLD)
    )
)

query = (
    agg.writeStream
    .format("console")
    .outputMode("update")
    .foreachBatch(process_batch)
    .option("truncate", False)
    .option("checkpointLocation", "/tmp/checkpoints/zscore")
    .start()
)

query.awaitTermination()
