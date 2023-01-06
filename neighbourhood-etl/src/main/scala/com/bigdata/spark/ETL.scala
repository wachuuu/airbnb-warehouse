package com.bigdata.spark

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Dataset, SparkSession}

case class Listing (
                     id: String,
                     name: String,
                     host_id: String,
                     host_name: String,
                     neighbourhood_group: String,
                     neighbourhood: String,
                     latitude: Double,
                     longitude: Double,
                     room_type: String,
                     price: Int,
                     minimum_nights: Int,
                     number_of_reviews: Int,
                     last_review: String,
                     reviews_per_month: Option[Double],
                     calculated_host_listings_count: Int,
                     availability_365: Int,
                   )

case class Neighbourhood (
                            neighbourhood: String,
                            neighbourhood_group: String,
                            city: String
                         )

object ETL {
  val JOB_NAME = "ETL_NEIGHBOURHOOD"
  val DESTINATION_TABLE = "dim_neighbourhood"

  def main(args: Array[String]): Unit = {

    val sourceDir: String = args(0)
    val spark = SparkSession.builder().appName(JOB_NAME).master("local[*]").getOrCreate()

    import spark.implicits._

    val berlinListingsDS: Dataset[Listing] = spark.read.format("org.apache.spark.csv")
      .option("header", value = true)
      .option("inferSchema", value = true)
      .option("multiline", value = true)
      .option("quote", "\"")
      .option("escape", "\"")
      .option("dateFormat", "yyyy-MM-dd")
      .csv(s"$sourceDir/listing/BerlinListings.csv")
      .as[Listing]

    val madridListingsDS: Dataset[Listing] = spark.read.format("org.apache.spark.csv")
      .option("header", value = true)
      .option("inferSchema", value = true)
      .option("multiline", value = true)
      .option("quote", "\"")
      .option("escape", "\"")
      .csv(s"$sourceDir/listing/MadridListings.csv")
      .as[Listing]

    val parisListingsDS: Dataset[Listing] = spark.read.format("org.apache.spark.csv")
      .option("header", value = true)
      .option("inferSchema", value = true)
      .option("multiline", value = true)
      .option("quote", "\"")
      .option("escape", "\"")
      .csv(s"$sourceDir/listing/ParisListings.csv")
      .as[Listing]

    val berlinNeighbourhood = berlinListingsDS
      .select("neighbourhood", "neighbourhood_group")
      .withColumn("city", lit("BERLIN"))
      .dropDuplicates("neighbourhood")
      .as[Neighbourhood]

    val madridNeighbourhood = madridListingsDS
      .select("neighbourhood", "neighbourhood_group")
      .withColumn("city", lit("MADRID"))
      .dropDuplicates("neighbourhood")
      .as[Neighbourhood]

    val parisNeighbourhood = parisListingsDS
      .select("neighbourhood", "neighbourhood_group")
      .withColumn("city", lit("PARIS"))
      .dropDuplicates("neighbourhood")
      .as[Neighbourhood]

    val allNeighbourhood = berlinNeighbourhood
      .union(madridNeighbourhood)
      .union(parisNeighbourhood)
      .dropDuplicates("neighbourhood")
      .orderBy("city")

    allNeighbourhood.write.insertInto(DESTINATION_TABLE)
  }
}
