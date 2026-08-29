from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import Window

def run_users_datamart():
    spark = SparkSession.builder \
        .appName("Users Datamart") \
        .master("yarn") \
        .getOrCreate()

    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    spark.conf.set("spark.sql.shuffle.partitions", "100")

    # Пути
    MSG_CITY_PATH = "/user/data_engineer/data/geo/message_with_city_and_timezone"
    OUTPUT_PATH = "/user/data_engineer/data/analytics/users_datamart"

    # 1. Читаем готовую таблицу сообщений с городами и таймзонами
    messages_with_city = spark.read.parquet(MSG_CITY_PATH)
    print(f"Прочитано сообщений с городами: {messages_with_city.count()}")

    # 2. Витрина пользователей

    # Последнее сообщение + act_city + local_time в одном проходе
    w_last = Window.partitionBy("user_id").orderBy(F.desc("event_time"))

    last_message_df = messages_with_city.withColumn("rn", F.row_number().over(w_last)) \
        .filter(F.col("rn") == 1) \
        .select(
            "user_id",
            F.col("city_id").alias("act_city"),
            F.col("city_name").alias("act_city_name"),
            "event_time",
            "timezone"
        )

    act_local_df = last_message_df.withColumn(
        "local_time",
        F.from_utc_timestamp(F.col("event_time"), F.col("timezone"))
    ).select(
        "user_id",
        "act_city",
        "act_city_name",
        "local_time"
    )

    # travel — последовательность смен городов (без подряд идущих повторов)
    w_order = Window.partitionBy("user_id").orderBy("event_time")

    travel_seq = messages_with_city \
        .withColumn("prev_city", F.lag("city_name").over(w_order)) \
        .filter(
            (F.col("prev_city").isNull()) | 
            (F.col("city_name") != F.col("prev_city"))
        ) \
        .select("user_id", "city_name")

    travel_agg = travel_seq.groupBy("user_id") \
        .agg(
            F.count("*").alias("travel_count"),               # кол-во смен + 1
            F.collect_list("city_name").alias("travel_array") # последовательность смен
        )

    # home_city — город с ≥27 дней подряд
    daily = messages_with_city.groupBy(
        "user_id",
        F.to_date("event_time").alias("day"),
        "city_id", "city_name"
    ).agg(F.count("*").alias("msg_per_day"))

    w_day = Window.partitionBy("user_id").orderBy("day")
    daily = daily.withColumn("prev_city",   F.lag("city_id").over(w_day)) \
                 .withColumn("city_changed", F.when(F.col("city_id") != F.col("prev_city"), 1).otherwise(0)) \
                 .withColumn("seq_id",       F.sum("city_changed").over(w_day))

    sequences = daily.groupBy("user_id", "seq_id", "city_id", "city_name") \
        .agg(
            F.count("*").alias("days_count"),
            F.min("day").alias("start_day"),
            F.max("day").alias("end_day")
        ) \
        .filter(F.col("days_count") >= 27)

    w_last_home = Window.partitionBy("user_id").orderBy(F.desc("end_day"))
    home_city_df = sequences.withColumn("rn", F.row_number().over(w_last_home)) \
        .filter(F.col("rn") == 1) \
        .select(
            "user_id",
            F.col("city_id").alias("home_city_id"),
            F.col("city_name").alias("home_city_name")
        )

    # Финальная витрина
    user_datamart = act_local_df \
        .join(travel_agg, "user_id", "left") \
        .join(home_city_df, "user_id", "left_outer") \
        .select(
            "user_id",
            "act_city",
            "act_city_name",
            "local_time",
            "travel_count",
            "travel_array",
            "home_city_id",
            "home_city_name"
        )

    user_datamart.write.mode("overwrite").parquet(OUTPUT_PATH)

    spark.stop()

if __name__ == "__main__":
    run_users_datamart()