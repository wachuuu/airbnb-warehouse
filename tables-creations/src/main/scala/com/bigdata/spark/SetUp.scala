package com.bigdata.spark

import org.apache.spark.sql.SparkSession

object SetUp {
  val JOB_NAME = "CREATE_TABLES"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName(JOB_NAME).master("local[*]").getOrCreate()

    // Drop tables if they exist
    spark.sql("DROP TABLE IF EXISTS dim_room_type")
    spark.sql("DROP TABLE IF EXISTS dim_neighbourhood")
    spark.sql("DROP TABLE IF EXISTS dim_date")
    spark.sql("DROP TABLE IF EXISTS dim_host")
    spark.sql("DROP TABLE IF EXISTS tof_reviews")

    // Create new tables
    spark.sql(
      """CREATE TABLE dim_room_type (
   `room_type` string)
  ROW FORMAT SERDE
   'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
  STORED AS INPUTFORMAT
   'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
  OUTPUTFORMAT
   'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

    spark.sql(
      """CREATE TABLE dim_neighbourhood (
   `name` string,
   `group` string,
   `city` string)
  ROW FORMAT SERDE
   'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
  STORED AS INPUTFORMAT
   'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
  OUTPUTFORMAT
   'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

    spark.sql(
      """CREATE TABLE dim_date (
   `date` date,
   `month` string,
   `year` int)
  ROW FORMAT SERDE
   'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
  STORED AS INPUTFORMAT
   'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
  OUTPUTFORMAT
   'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

    spark.sql(
      """CREATE TABLE dim_host (
   `id` string,
   `name` string)
  ROW FORMAT SERDE
   'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
  STORED AS INPUTFORMAT
   'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
  OUTPUTFORMAT
   'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

    spark.sql(
      """CREATE TABLE tof_reviews (
   `id` int,
   `neighbourhood` string,
   `date` date,
   `host_id` string,
   `room_type` string,
   `available_listings_count` int,
   `not_available_listings_count` int,
   `prices_sum` double,
   `ratings_sum` double,
   `declared_reviews_count` int)
  ROW FORMAT SERDE
   'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
  STORED AS INPUTFORMAT
   'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
  OUTPUTFORMAT
   'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

  }
}
