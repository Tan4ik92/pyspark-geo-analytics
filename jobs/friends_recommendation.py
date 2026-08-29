# friends_recommendation.py
# Витрина рекомендаций друзей (шаг 4 проекта)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

def run_friends_recommendation():
    spark = SparkSession.builder \
        .appName("Friends Recommendation") \
        .master("yarn") \
        .getOrCreate()

    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    spark.conf.set("spark.sql.shuffle.partitions", "100")

    # Пути — используем готовые таблицы
    EVENTS_PATH = "/user/master/data/geo/events"  # реакции и подписки из сырых событий
    MSG_CITY_PATH = "/user/data_engineer/data/geo/message_with_city_and_timezone"
    OUTPUT_PATH = "/user/data_engineer/data/analytics/friends_recommendation"

    # 1. Последний город + координаты пользователя (из готовой таблицы)
    window_last = Window.partitionBy("user_id").orderBy(F.desc("event_time"))
    user_last = spark.read.parquet(MSG_CITY_PATH) \
        .withColumn("rn", F.row_number().over(window_last)) \
        .filter(F.col("rn") == 1) \
        .select(
            "user_id",
            F.col("city_id").alias("zone_id"),
            "lat_msg",
            "lon_msg"
        )

    # 2. Подписки (из сырых событий)
    subscriptions = spark.read.parquet(EVENTS_PATH) \
        .filter(F.col("event_type") == "subscription") \
        .select(
            F.col("event.subscription_user").cast("string").alias("user_id"),
            F.col("event.subscription_channel").cast("string").alias("channel_id")
        ) \
        .filter(F.col("user_id").isNotNull() & F.col("channel_id").isNotNull()) \
        .distinct()

    # 3. Пары пользователей на одном канале (user_left < user_right)
    channel_pairs = subscriptions.alias("s1") \
        .join(
            subscriptions.alias("s2"),
            (F.col("s1.channel_id") == F.col("s2.channel_id")) &
            (F.col("s1.user_id") < F.col("s2.user_id"))
        ) \
        .select(
            F.col("s1.user_id").alias("user_left"),
            F.col("s2.user_id").alias("user_right")
        )

    # 4. Исключаем тех, кто уже переписывался (тоже из сырых событий)
    messages = spark.read.parquet(EVENTS_PATH) \
        .filter(F.col("event_type") == "message") \
        .select(
            F.col("event.message_from").cast("string").alias("from_user"),
            F.col("event.message_to").cast("string").alias("to_user")
        ) \
        .filter(F.col("from_user").isNotNull() & F.col("to_user").isNotNull())

    communicated = messages.select(
        F.least("from_user", "to_user").alias("user_left"),
        F.greatest("from_user", "to_user").alias("user_right")
    ).distinct()

    candidates = channel_pairs.join(
        communicated,
        ["user_left", "user_right"],
        "left_anti"
    )

    # 5. Добавляем координаты (из готовой таблицы)
    candidates_geo = candidates \
        .join(user_last.alias("u1"), F.col("user_left") == F.col("u1.user_id")) \
        .join(user_last.alias("u2"), F.col("user_right") == F.col("u2.user_id"))

    # 6. Расчёт расстояния (Haversine)
    R = 6371.0
    candidates_filtered = candidates_geo \
        .withColumn("dlat", F.radians(F.col("u1.lat_msg") - F.col("u2.lat_msg"))) \
        .withColumn("dlon", F.radians(F.col("u1.lon_msg") - F.col("u2.lon_msg"))) \
        .withColumn(
            "a",
            F.pow(F.sin(F.col("dlat") / 2), 2) +
            F.cos(F.radians(F.col("u1.lat_msg"))) *
            F.cos(F.radians(F.col("u2.lat_msg"))) *
            F.pow(F.sin(F.col("dlon") / 2), 2)
        ) \
        .withColumn("c", 2 * F.atan2(F.sqrt(F.col("a")), F.sqrt(1 - F.col("a")))) \
        .withColumn("distance_km", R * F.col("c")) \
        .filter(F.col("distance_km") <= 1.0) \
        .select(
            "user_left",
            "user_right",
            F.col("u1.zone_id").alias("zone_id")
        )

    # 7. Финальная витрина
    result = candidates_filtered \
        .select(
            "user_left",
            "user_right",
            F.current_timestamp().alias("processed_dttm"),
            "zone_id",
            F.from_utc_timestamp(F.current_timestamp(), "Australia/Sydney").alias("local_time")
        ) \
        .distinct()

    # Сохранение
    result.write.mode("overwrite").parquet(OUTPUT_PATH)

    spark.stop()

if __name__ == "__main__":
    run_friends_recommendation()