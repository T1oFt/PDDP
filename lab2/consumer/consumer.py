import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, avg, count, sum, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType
from pyspark.sql.functions import udf

# --- Конфигурация ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "instagram-users")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "postgres")
# Итоговая таблица, куда попадут агрегированные данные активных пользователей
POSTGRES_FINAL_TABLE = os.getenv("POSTGRES_TABLE_FINAL", "instagram_active_users_aggregated")

# --- Схема данных ---
schema = StructType([
    StructField("user_id", IntegerType(), False),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("country", StringType(), True),
    StructField("urban_rural", StringType(), True),
    StructField("income_level", StringType(), True),
    StructField("employment_status", StringType(), True),
    StructField("education_level", StringType(), True),
    StructField("relationship_status", StringType(), True),
    StructField("has_children", StringType(), True),
    StructField("exercise_hours_per_week", DoubleType(), True),
    StructField("sleep_hours_per_night", DoubleType(), True),
    StructField("diet_quality", StringType(), True),
    StructField("smoking", StringType(), True),
    StructField("alcohol_frequency", StringType(), True),
    StructField("perceived_stress_score", IntegerType(), True),
    StructField("self_reported_happiness", IntegerType(), True),
    StructField("body_mass_index", DoubleType(), True),
    StructField("blood_pressure_systolic", IntegerType(), True),
    StructField("blood_pressure_diastolic", IntegerType(), True),
    StructField("daily_steps_count", IntegerType(), True),
    StructField("weekly_work_hours", DoubleType(), True),
    StructField("hobbies_count", IntegerType(), True),
    StructField("social_events_per_month", IntegerType(), True),
    StructField("books_read_per_year", DoubleType(), True),
    StructField("volunteer_hours_per_month", DoubleType(), True),
    StructField("travel_frequency_per_year", DoubleType(), True),
    StructField("daily_active_minutes_instagram", IntegerType(), True),
    StructField("sessions_per_day", IntegerType(), True),
    StructField("posts_created_per_week", IntegerType(), True),
    StructField("reels_watched_per_day", IntegerType(), True),
    StructField("stories_viewed_per_day", IntegerType(), True),
    StructField("likes_given_per_day", IntegerType(), True),
    StructField("comments_written_per_day", IntegerType(), True),
    StructField("dms_sent_per_week", IntegerType(), True),
    StructField("dms_received_per_week", IntegerType(), True),
    StructField("ads_viewed_per_day", IntegerType(), True),
    StructField("ads_clicked_per_day", IntegerType(), True),
    StructField("time_on_feed_per_day", IntegerType(), True),
    StructField("time_on_explore_per_day", IntegerType(), True),
    StructField("time_on_messages_per_day", IntegerType(), True),
    StructField("time_on_reels_per_day", IntegerType(), True),
    StructField("followers_count", IntegerType(), True),
    StructField("following_count", IntegerType(), True),
    StructField("uses_premium_features", StringType(), True),
    StructField("notification_response_rate", DoubleType(), True),
    StructField("account_creation_year", IntegerType(), True),
    StructField("last_login_date", StringType(), True),
    StructField("average_session_length_minutes", DoubleType(), True),
    StructField("content_type_preference", StringType(), True),
    StructField("preferred_content_theme", StringType(), True),
    StructField("privacy_setting_level", StringType(), True),
    StructField("two_factor_auth_enabled", StringType(), True),
    StructField("biometric_login_used", StringType(), True),
    StructField("linked_accounts_count", IntegerType(), True),
    StructField("subscription_status", StringType(), True),
    StructField("user_engagement_score", DoubleType(), True),
])

def is_active_user(daily_active_minutes, sessions_per_day, followers_count):
    """
    Возвращает True, если пользователь активен:
    - Daily active minutes > 60 OR
    - Sessions per day > 5 OR
    - Followers count > 1000
    """
    if daily_active_minutes is None or sessions_per_day is None:
        return False
    
    f_count = followers_count if followers_count is not None else 0

    if (daily_active_minutes > 60 or 
        sessions_per_day > 5 or 
        f_count > 1000):
        return True
    return False

is_active_user_udf = udf(is_active_user, BooleanType())

def categorize_engagement(daily_active_minutes, sessions_per_day, followers_count, engagement_score):
    if daily_active_minutes is None or sessions_per_day is None:
        return "Unknown"
    
    f_count = followers_count if followers_count is not None else 0
    e_score = engagement_score if engagement_score is not None else 0

    if (daily_active_minutes > 200 or sessions_per_day > 10 or f_count > 5000 or e_score > 7):
        return "Super User"
    elif (daily_active_minutes > 100 or sessions_per_day > 5 or e_score > 5):
        return "Active User"
    elif daily_active_minutes > 30 or sessions_per_day > 3:
        return "Moderate User"
    else:
        return "Casual User"

categorize_engagement_udf = udf(categorize_engagement, StringType())

def create_spark_session():
    spark = SparkSession.builder \
        .appName("InstagramAggregationPipeline") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def read_from_kafka(spark):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

def parse_messages(df):
    return df.select(from_json(col("value").cast("string"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("processed_at", current_timestamp())

def aggregate_active_users(df):
    """
    Основная логика:
    1. Применяем UDF фильтрации.
    2. Фильтруем DataFrame.
    3. Добавляем Watermark.
    4. Агрегируем по country и content_type_preference.
    """

    df_with_flag = df.withColumn(
        "is_active", 
        is_active_user_udf(
            col("daily_active_minutes_instagram"),
            col("sessions_per_day"),
            col("followers_count")
        )
    )
    
    df_filtered = df_with_flag.filter(col("is_active") == True)
    
    print("[Pipeline] Users filtered by activity UDF.")

    df_with_watermark = df_filtered.withWatermark("processed_at", "10 minutes")

    result_df = df_with_watermark.groupBy(
        col("country"),
        col("content_type_preference")
    ).agg(
        count("user_id").alias("active_user_count"),
        avg("daily_active_minutes_instagram").alias("avg_daily_active_minutes"),
        avg("user_engagement_score").alias("avg_engagement_score"),
        sum("followers_count").alias("total_followers"),
        avg("time_on_reels_per_day").alias("avg_reels_time"),
        avg("posts_created_per_week").alias("avg_posts_per_week"),
    )
    
    return result_df

def write_final_to_postgres(spark, df_stream, table_name):
    """
    df_stream: Потоковый DataFrame (для получения схемы и запуска стрима)
    table_name: Имя таблицы в PostgreSQL
    """
    jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    properties = {
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver"
    }

    print(f"[Sink] Creating table '{table_name}' if not exists...")
    
    schema = df_stream.schema
    
    empty_static_df = spark.createDataFrame([], schema)

    try:
        empty_static_df.write \
            .jdbc(url=jdbc_url, table=table_name, mode="ignore", properties=properties)
        print(f"[Sink] Table '{table_name}' ready (created or already exists).")
    except Exception as e:
        print(f"[Sink] Warning: Could not create table automatically: {e}")
        print("[Sink] Please ensure the table exists in PostgreSQL manually.")

    def write_batch(batch_df, batch_id):
        batch_count = batch_df.count()
        if batch_count > 0:
            print(f"[Sink] Writing batch {batch_id} with {batch_count} aggregated rows")
            batch_df.write \
                .jdbc(url=jdbc_url, table=table_name, mode="append", properties=properties)
            print(f"[Sink] Batch {batch_id} written successfully")
        else:
            print(f"[Sink] Batch {batch_id} is empty, skipping")

    query = df_stream.writeStream \
        .foreachBatch(write_batch) \
        .outputMode("update") \
        .trigger(processingTime="15 seconds") \
        .queryName("FinalAggregationStream") \
        .start()
    
    return query

def main():
    print("=" * 60)
    print("Instagram Pipeline: Filter (UDF) -> Aggregate -> Postgres")
    print("=" * 60)
    
    spark = create_spark_session()
    
    # 1. Чтение
    kafka_df = read_from_kafka(spark)
    parsed_df = parse_messages(kafka_df)
    
    print("\n--- Schema before filtering ---")
    parsed_df.printSchema()

    # 2. Логика: Фильтрация + Агрегация
    final_df = aggregate_active_users(parsed_df)
    
    print("\n--- Schema after aggregation ---")
    final_df.printSchema()

    # 3. Запись в одну итоговую таблицу
    query = write_final_to_postgres(spark, final_df, POSTGRES_FINAL_TABLE)
    
    print("=" * 60)
    print(f"Streaming started. Writing to table: {POSTGRES_FINAL_TABLE}")
    print("=" * 60)
    
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()