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

# === UDF: Wellness Score ===
def calculate_wellness(bmi, stress, sleep):
    """
    Рассчитывает интегральный показатель благополучия.
    Возвращает: (wellness_score: 0-100, risk_category: str)
    """
    if None in (bmi, stress, sleep):
        return None, "Unknown"
    
    # Компоненты (чем ближе к оптимуму — тем выше скор)
    bmi_score = max(0, 10 - abs(bmi - 22))           # оптимум BMI ~22
    stress_score = max(0, 10 - stress / 4)           # меньше стресс = лучше
    sleep_score = max(0, 10 - abs(sleep - 7.5) * 2)  # оптимум сна ~7.5ч
    
    wellness = (bmi_score + stress_score + sleep_score) / 3 * 10
    wellness = round(min(100, max(0, wellness)), 2)
    
    if wellness >= 70:
        category = "High Wellness"
    elif wellness >= 40:
        category = "Medium Wellness"
    else:
        category = "Low Wellness"
    
    return wellness, category

wellness_udf = udf(
    calculate_wellness, 
    StructType([
        StructField("wellness_score", DoubleType(), True),
        StructField("risk_category", StringType(), True)
    ])
)

# === Схема агрегированных данных (для Postgres) ===
aggregated_schema = StructType([
    StructField("country", StringType(), True),
    StructField("risk_category", StringType(), True),
    StructField("avg_daily_active_minutes", DoubleType(), True),
    StructField("avg_engagement_score", DoubleType(), True),
    StructField("avg_wellness_score", DoubleType(), True),
    StructField("user_count", IntegerType(), True),
    StructField("avg_bmi", DoubleType(), True),
    StructField("avg_stress_score", DoubleType(), True),
    StructField("processed_at", StringType(), True),
])


def create_spark_session():
    spark = SparkSession.builder \
        .appName("InstagramAggregationPipeline") \
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


def filter_and_enrich(df):
    """Фильтрация + применение UDF"""
    # Фильтр: только пользователи с высоким стрессом
    filtered = df.filter(col("perceived_stress_score") > 10)
    
    # Применяем UDF
    with_wellness = filtered.withColumn(
        "wellness_data",
        wellness_udf(
            col("body_mass_index"), 
            col("perceived_stress_score"), 
            col("sleep_hours_per_night")
        )
    ).select(
        "*",
        col("wellness_data.wellness_score").alias("wellness_score"),
        col("wellness_data.risk_category").alias("risk_category")
    )
    return with_wellness


def aggregate_data(df):
    """Агрегация по стране и категории риска"""
    return df.groupBy("country", "risk_category").agg(
        avg("daily_active_minutes_instagram").alias("avg_daily_active_minutes"),
        avg("user_engagement_score").alias("avg_engagement_score"),
        avg("wellness_score").alias("avg_wellness_score"),
        count("user_id").alias("user_count"),
        avg("body_mass_index").alias("avg_bmi"),
        avg("perceived_stress_score").alias("avg_stress_score")
    ).withColumn(
        "processed_at", 
        current_timestamp().cast("string")
    )

def write_to_postgres(spark, df_stream, table_name):
    jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    properties = {
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver"
    }

    # Инициализация таблицы (если не существует)
    empty_df = spark.createDataFrame([], aggregated_schema)
    try:
        empty_df.write.jdbc(url=jdbc_url, table=table_name, mode="ignore", properties=properties)
        print(f"[Sink] Table '{table_name}' ready")
    except Exception as e:
        print(f"[Sink] Warning: {e}")

    def write_batch(batch_df, batch_id):
        cnt = batch_df.count()
        if cnt > 0:
            print(f"[Sink] Batch {batch_id}: writing {cnt} rows")
            batch_df.write.jdbc(url=jdbc_url, table=table_name, mode="append", properties=properties)
        else:
            print(f"[Sink] Batch {batch_id}: empty")

    return df_stream.writeStream \
        .foreachBatch(write_batch) \
        .outputMode("update") \
        .trigger(processingTime="10 seconds") \
        .queryName("AggregationSink") \
        .start()


def main():
    print("=" * 60)
    print("Instagram Pipeline: Filter (UDF) -> Aggregate -> Postgres")
    print("=" * 60)
    
    spark = create_spark_session()
    
    # 1. Чтение
    df = read_from_kafka(spark)
    df = parse_messages(df)
    df = filter_and_enrich(df)
    df = aggregate_data(df)
    
    print("\n--- Schema after aggregation ---")
    df.printSchema()

    # 3. Запись в одну итоговую таблицу
    query = write_to_postgres(spark, df, POSTGRES_FINAL_TABLE)
    
    print("=" * 60)
    print(f"Streaming started. Writing to table: {POSTGRES_FINAL_TABLE}")
    print("=" * 60)
    
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()