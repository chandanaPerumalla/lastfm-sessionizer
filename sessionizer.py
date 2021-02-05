from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, unix_timestamp, when, concat_ws, collect_list


class Sessionizer:

    def __init__(self, file_path):
        self.file_path = file_path

    def generate_sessions(self):
        spark = SparkSession.builder.appName('lastfm-sessionizer').master('local[*]').getOrCreate()

        user_track_schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("musician_id", StringType(), True),
            StructField("musician_name", StringType(), True),
            StructField("track_id", StringType(), True),
            StructField("track_name", StringType(), True)])

        track_df = spark.read.option("delimiter", "\t").schema(user_track_schema).csv(self.file_path)

        partition_window = Window.partitionBy("user_id").orderBy("timestamp")

        lag_diff = unix_timestamp("timestamp") - lag(unix_timestamp("timestamp"), 1, 0).over(partition_window)

        session_indicator = when(lag_diff >= 20 * 60 * 1000, 1).otherwise(0)

        track_df_with_lag = track_df.select("*", session_indicator.alias("session_indicator"))

        session = sum("session_indicator").over(partition_window)

        track_df_sessionized = track_df_with_lag.select("*", session.alias("session_id")).drop("session_indicator")

        grouped_df = track_df_sessionized \
            .groupBy("user_id", "session_id").agg(min("timestamp").alias("min_timestamp"),
                                                  max("timestamp").alias("max_timestamp"),
                                                  concat_ws(",", collect_list("track_name")).alias("tracks_played"))

        track_df_with_session_length = grouped_df.drop("session_id").select("*", unix_timestamp(
            "max_timestamp") - unix_timestamp("min_timestamp").alias("sessionLength")).orderBy("sessionLength".desc)

        track_df_with_session_length.drop("sessionLength").limit(10).show(truncate=False)


sessionizer = Sessionizer("lastfm-dataset-1K/userid-timestamp-artid-artname-traid-traname.tsv")
sessionizer.generate_sessions()
