from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, broadcast

# ------------------------------------------
# 1️⃣ Spark session setup
# ------------------------------------------
spark = SparkSession.builder.appName("SparkGameAnalytics").getOrCreate()

# Disable Spark’s automatic broadcast join to manually control it
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# Base path to data files
base_path = "/home/iceberg/data"

# ------------------------------------------
# 2️⃣ Load CSVs with headers
# ------------------------------------------
# (Note: For a production-quality pipeline, you would define explicit schemas.)

match_details = spark.read.option("header", "true").csv(f"{base_path}/match_details.csv")
matches = spark.read.option("header", "true").csv(f"{base_path}/matches.csv") \
    .withColumn("completion_date", expr("DATE_TRUNC('day', completion_date)"))
medals_matches_players = spark.read.option("header", "true").csv(f"{base_path}/medals_matches_players.csv")
medals = spark.read.option("header", "true").csv(f"{base_path}/medals.csv")
maps = spark.read.option("header", "true").csv(f"{base_path}/maps.csv")

# ------------------------------------------
# 3️⃣ Rename key columns to avoid ambiguity
# ------------------------------------------
match_details = match_details.withColumnRenamed("player_gamertag", "player_gamertag_md") \
                             .withColumnRenamed("player_total_kills", "kills")

medals_matches_players = medals_matches_players.withColumnRenamed("player_gamertag", "player_gamertag_mmp")

matches = matches.withColumnRenamed("mapid", "mapid_matches")
maps = maps.withColumnRenamed("mapid", "mapid_maps")

# ------------------------------------------
# 4️⃣ Repartition by match_id (preparation for joins)
# ------------------------------------------
match_details = match_details.repartition(16, "match_id")
matches = matches.repartition(16, "match_id")
medals_matches_players = medals_matches_players.repartition(16, "match_id")

# ------------------------------------------
# 5️⃣ Alias tables for cleaner joins
# ------------------------------------------
md = match_details.alias("md")
m = matches.alias("m")
mmp = medals_matches_players.alias("mmp")
med = medals.alias("med")
mp = maps.alias("mp")

# ------------------------------------------
# 6️⃣ Explicit broadcast joins (Query 2)
# ------------------------------------------
# Broadcasting `medals` and `maps` since they’re small dimension tables.
df = md.join(m, on="match_id", how="left") \
       .join(
           mmp,
           (md.match_id == mmp.match_id) &
           (col("player_gamertag_md") == col("player_gamertag_mmp")),
           "left"
       ) \
       .join(broadcast(med), on="medal_id", how="left") \
       .join(broadcast(mp), m.mapid_matches == mp.mapid_maps, how="left") \
       .select(
           col("md.match_id"),
           col("md.player_gamertag_md"),
           col("md.kills"),
           col("m.playlist_id"),
           col("mp.mapid_maps").alias("mapid"),
           col("mp.name").alias("map_name"),
           col("med.name").alias("medal_name")
       )

# ------------------------------------------
# 7️⃣ Bucket joins (Query 3)
# ------------------------------------------
# Bucket each main table on `match_id` for optimized joins.
# Note: Using only bucketBy() because sortBy() can cause IllegalArgumentException in some setups.

spark.conf.set("spark.sql.sources.bucketing.enabled", "true")

match_details.write.mode("overwrite") \
    .bucketBy(16, "match_id") \
    .saveAsTable("hw.match_details_b16")

matches.write.mode("overwrite") \
    .bucketBy(16, "match_id") \
    .saveAsTable("hw.matches_b16")

medals_matches_players.write.mode("overwrite") \
    .bucketBy(16, "match_id") \
    .saveAsTable("hw.mmp_b16")

# Load the bucketed tables back
md_b = spark.table("hw.match_details_b16")
m_b = spark.table("hw.matches_b16")
mmp_b = spark.table("hw.mmp_b16")

# Join the bucketed tables — this should avoid shuffles
bucket_join_df = md_b.join(m_b, "match_id", "inner") \
                     .join(mmp_b, "match_id", "inner")

# Show the physical plan to confirm bucketed join behavior
bucket_join_df.explain("formatted")

# ------------------------------------------
# 8️⃣ Aggregations (Query 4)
# ------------------------------------------

# Query 4a: Which player averages the most kills per game?
df_kills = md.withColumn("kills", col("kills").cast("int"))
query_4a = df_kills.groupBy("player_gamertag_md") \
    .agg(expr("sum(kills) / count(distinct match_id) as avg_kills_per_game")) \
    .orderBy(col("avg_kills_per_game").desc())
query_4a.show(5)

# Query 4b: Which playlist gets played the most?
query_4b = matches.groupBy("playlist_id") \
    .agg(expr("count(distinct match_id) as total_matches")) \
    .orderBy(col("total_matches").desc())
query_4b.show(5)

# Query 4c: Which map gets played the most?
maps_join = matches.select("match_id", "mapid_matches") \
    .join(maps, col("mapid_matches") == col("mapid_maps"), "left")
query_4c = maps_join.groupBy(col("mapid_maps").alias("mapid"), col("name").alias("map_name")) \
    .agg(expr("count(distinct match_id) as total_plays")) \
    .orderBy(col("total_plays").desc())
query_4c.show(5)

# Query 4d: Which map do players get the most "Killing Spree" medals on?
spree_ids = medals.filter(col("name") == "Killing Spree").select("medal_id")
mmp_spree = medals_matches_players.join(spree_ids, "medal_id", "inner")
spree_maps = mmp_spree.join(matches.select("match_id", "mapid_matches"), "match_id", "left") \
                      .join(maps, col("mapid_matches") == col("mapid_maps"), "left")
query_4d = spree_maps.groupBy(col("mapid_maps").alias("mapid"), col("name").alias("map_name")) \
    .count().orderBy(col("count").desc())
query_4d.show(5)

# ------------------------------------------
# 9️⃣ Optional: Partitioning + sortWithinPartitions (Query 5)
# ------------------------------------------
# Experiment with different partition/sort combinations to compare file sizes.
# This section is left as a lab exercise — see instructor notes.

# Example (uncomment and adjust path):
# df_a = df.repartition("playlist_id").sortWithinPartitions("mapid")
# df_a.write.mode("overwrite").partitionBy("playlist_id").parquet("/tmp/out/ver_a")

# df_b = df.repartition("mapid").sortWithinPartitions("playlist_id")
# df_b.write.mode("overwrite").partitionBy("mapid").parquet("/tmp/out/ver_b")

# df_c = df.repartition("playlist_id", "mapid").sortWithinPartitions("mapid", "playlist_id")
# df_c.write.mode("overwrite").partitionBy("playlist_id", "mapid").parquet("/tmp/out/ver_c")

# Then compare file sizes to identify the most storage-efficient setup.
