from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

def run_zones_datamart():
    spark = SparkSession.builder \
        .appName("Zones Datamart") \
        .master("yarn") \
        .getOrCreate()

    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    spark.conf.set("spark.sql.shuffle.partitions", "100")

    # Пути — используем готовые таблицы
    MSG_CITY_PATH = "/user/data_engineer/data/geo/message_with_city_and_timezone" 
    OUTPUT_PATH = "/user/data_engineer/data/analytics/zones_datamart"

    # Чтение готовых данных
    msg_city = spark.read.parquet(MSG_CITY_PATH)  # сообщения + города + timezone

    # Последний город пользователя (для reaction и subscription)
    window_last = Window.partitionBy("user_id").orderBy(F.desc("event_time"))
    last_city = msg_city \
        .withColumn("rn", F.row_number().over(window_last)) \
        .filter(F.col("rn") == 1) \
        .select("user_id", F.col("city_id").alias("zone_id"))

    # Регистрации — первое сообщение пользователя
    window_first = Window.partitionBy("user_id").orderBy("event_time")
    registrations = msg_city \
        .withColumn("rn", F.row_number().over(window_first)) \
        .filter(F.col("rn") == 1) \
        .select(
            "user_id",
            "event_time",
            F.lit("registration").alias("event_type"),
            F.col("city_id").alias("zone_id")
        )

    # Сообщения — своя зона
    messages = msg_city.select(
        "user_id",
        "event_time",
        F.lit("message").alias("event_type"),
        F.col("city_id").alias("zone_id")
    )

    # Реакции — присоединяем последний город с broadcast
    events = spark.read.parquet("/user/master/data/geo/events")
    reactions = events.filter(F.col("event_type") == "reaction") \
        .select(
            F.col("event.reaction_from").cast("string").alias("user_id"),
            F.to_timestamp(F.col("event.datetime")).alias("event_time"),
            F.lit("reaction").alias("event_type")
        ).filter(F.col("user_id").isNotNull() & F.col("event_time").isNotNull()) \
        .join(F.broadcast(last_city), "user_id", "left") \
        .select("user_id", "event_time", "event_type", "zone_id") \
        .filter(F.col("zone_id").isNotNull())

    # Подписки — то же самое
    subscriptions = events.filter(F.col("event_type") == "subscription") \
        .select(
            F.col("event.subscription_user").cast("string").alias("user_id"),
            F.to_timestamp(F.col("event.datetime")).alias("event_time"),
            F.lit("subscription").alias("event_type")
        ).filter(F.col("user_id").isNotNull() & F.col("event_time").isNotNull()) \
        .join(F.broadcast(last_city), "user_id", "left") \
        .select("user_id", "event_time", "event_type", "zone_id") \
        .filter(F.col("zone_id").isNotNull())

    # Объединяем все события
    all_events = messages.unionByName(registrations) \
                         .unionByName(reactions) \
                         .unionByName(subscriptions)

    # Группировка
    vitrina = all_events.withColumn("month", F.month("event_time")) \
                        .withColumn("week", F.weekofyear("event_time")) \
                        .groupBy("month", "week", "zone_id") \
                        .agg(
                            F.sum(F.when(F.col("event_type") == "message", 1).otherwise(0)).alias("week_message"),
                            F.sum(F.when(F.col("event_type") == "reaction", 1).otherwise(0)).alias("week_reaction"),
                            F.sum(F.when(F.col("event_type") == "subscription", 1).otherwise(0)).alias("week_subscription"),
                            F.sum(F.when(F.col("event_type") == "registration", 1).otherwise(0)).alias("week_user")
                        )

    monthly = all_events.withColumn("month", F.month("event_time")) \
                        .groupBy("month", "zone_id") \
                        .agg(
                            F.sum(F.when(F.col("event_type") == "message", 1).otherwise(0)).alias("month_message"),
                            F.sum(F.when(F.col("event_type") == "reaction", 1).otherwise(0)).alias("month_reaction"),
                            F.sum(F.when(F.col("event_type") == "subscription", 1).otherwise(0)).alias("month_subscription"),
                            F.sum(F.when(F.col("event_type") == "registration", 1).otherwise(0)).alias("month_user")
                        )

    result = vitrina.join(monthly, ["month", "zone_id"], "left")

    result = result.select(
        "month", "week", "zone_id",
        "week_message", "week_reaction", "week_subscription", "week_user",
        "month_message", "month_reaction", "month_subscription", "month_user"
    )

    result.write.mode("overwrite").parquet(OUTPUT_PATH)

    spark.stop()

if __name__ == "__main__":
    run_zones_datamart()