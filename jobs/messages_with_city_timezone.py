from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import Window

spark = SparkSession.builder \
    .appName("Message to Nearest City)") \
    .master("local[*]") \
    .getOrCreate()

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# Пути
EVENTS_PATH = "/user/master/data/geo/events"
GEO_PATH    = "/user/data_engineer/data/geo/geo.csv"
OUTPUT_PATH = "/user/data_engineer/data/geo/message_with_city_and_timezone"

# 1. Города (без изменений)
cities_raw = spark.read \
    .option("header", "false") \
    .option("delimiter", ";") \
    .csv(GEO_PATH)

cities = cities_raw.toDF("id", "city", "lat_str", "lng_str") \
    .withColumn("id", F.col("id").cast("integer")) \
    .withColumn("lat", F.regexp_replace("lat_str", ",", ".").cast("double")) \
    .withColumn("lng", F.regexp_replace("lng_str", ",", ".").cast("double")) \
    .drop("lat_str", "lng_str") \
    .filter(F.col("lat").isNotNull() & F.col("lng").isNotNull())

tz_dict = {
    "Sydney": "Australia/Sydney",
    "Melbourne": "Australia/Melbourne",
    "Brisbane": "Australia/Brisbane",
    "Perth": "Australia/Perth",
    "Adelaide": "Australia/Adelaide",
    "Gold Coast": "Australia/Brisbane",
    "Cranbourne": "Australia/Melbourne",
    "Canberra": "Australia/Sydney",
    "Newcastle": "Australia/Sydney",
    "Wollongong": "Australia/Sydney",
    "Geelong": "Australia/Melbourne",
    "Hobart": "Australia/Hobart",
    "Townsville": "Australia/Brisbane",
    "Ipswich": "Australia/Brisbane",
    "Cairns": "Australia/Brisbane",
    "Toowoomba": "Australia/Brisbane",
    "Darwin": "Australia/Darwin",
    "Ballarat": "Australia/Melbourne",
    "Bendigo": "Australia/Melbourne",
    "Launceston": "Australia/Hobart",
    "Mackay": "Australia/Brisbane",
    "Rockhampton": "Australia/Brisbane",
    "Maitland": "Australia/Sydney",
    "Bunbury": "Australia/Perth"
}

tz_udf = F.udf(lambda city: tz_dict.get(city, "Australia/Sydney"), "string")
cities = cities.withColumn("timezone", tz_udf(F.col("city")))

events = spark.read.parquet(EVENTS_PATH)

raw_messages = events.filter(
    (F.col("event_type") == "message") &
    F.col("event.message_from").isNotNull() &
    F.col("lat").isNotNull() &
    F.col("lon").isNotNull()
).select(
    F.col("event.message_id").cast("string").alias("message_id"),
    F.col("event.message_from").cast("string").alias("user_id"),
    F.coalesce(
        # Приоритет 1: актуальное время 2022
        F.to_timestamp(F.col("event.datetime"), "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(F.col("event.datetime"), "yyyy-MM-dd HH:mm:ss.SSSSSSSSS"),
        F.to_timestamp(F.substring(F.col("event.datetime"), 1, 19), "yyyy-MM-dd HH:mm:ss"),        
        # Приоритет 2: fallback на message_ts 2021
        F.to_timestamp(F.col("event.message_ts"), "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(F.col("event.message_ts"), "yyyy-MM-dd HH:mm:ss.SSSSSSSSS"),
        F.to_timestamp(F.substring(F.col("event.message_ts"), 1, 19), "yyyy-MM-dd HH:mm:ss")
    ).alias("event_time"),
    
    F.col("lat").alias("lat_msg"),
    F.col("lon").alias("lon_msg"),
    F.col("event.message_to").cast("string").alias("message_to"),
    F.col("event.message").alias("message_text")
)

total_count = raw_messages.count()
null_time_count = raw_messages.filter(F.col("event_time").isNull()).count()

messages = raw_messages.filter(F.col("event_time").isNotNull())

# 3. Кросс-джоин + расстояние
cities_hint = cities.hint("broadcast")
df = messages.crossJoin(cities_hint)

R = 6371.0
df = df.withColumn("dlat", F.radians(F.col("lat_msg") - F.col("lat")))
df = df.withColumn("dlon", F.radians(F.col("lon_msg") - F.col("lng")))
df = df.withColumn("a",
    F.pow(F.sin(F.col("dlat") / 2), 2) +
    F.cos(F.radians(F.col("lat_msg"))) * F.cos(F.radians(F.col("lat"))) *
    F.pow(F.sin(F.col("dlon") / 2), 2)
)
df = df.withColumn("c", 2 * F.atan2(F.sqrt(F.col("a")), F.sqrt(1 - F.col("a"))))
df = df.withColumn("distance_km", R * F.col("c"))

window_nearest = Window.partitionBy("message_id").orderBy("distance_km")
nearest_city = df.withColumn("rn", F.row_number().over(window_nearest)) \
    .filter(F.col("rn") == 1) \
    .drop("rn", "a", "c", "dlat", "dlon", "distance_km") \
    .select(
        "message_id",
        "user_id",
        "event_time",
        "lat_msg",
        "lon_msg",
        F.col("id").alias("city_id"),
        F.col("city").alias("city_name"),
        "timezone",
        "message_to",
        "message_text"
    ) \
    .orderBy("event_time", "message_id")

# Сохраняем
nearest_city.write.mode("overwrite").parquet(OUTPUT_PATH)
print(f"Сохранено: {OUTPUT_PATH}")

spark.stop()