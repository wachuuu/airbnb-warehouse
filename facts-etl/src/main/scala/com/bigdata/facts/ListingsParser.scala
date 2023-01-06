package com.bigdata.facts

import org.apache.spark.sql.{Dataset, SparkSession}

case class Listing(
                    id: String,
                    name: String,
                    host_id: Int,
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

object ListingsParser {

  def parseListings(dataPath: String, spark: SparkSession): Dataset[Listing] = {

    val berlinListing = parseListing(s"$dataPath/listing/BerlinListings.csv", spark)
    val parisListing = parseListing(s"$dataPath/listing/ParisListings.csv", spark)
    val madridListing = parseListing(s"$dataPath/listing/MadridListings.csv", spark)

    berlinListing
      .union(parisListing)
      .union(madridListing)
    //    parseListing(s"$dataPath/ParisListings.csv", spark)
  }

  private def parseListing(listingPath: String, spark: SparkSession): Dataset[Listing] = {
    import spark.implicits._
    spark.read.format("org.apache.spark.csv")
      .option("header", value = true)
      .option("inferSchema", value = true)
      // new lines in name column ("a \n b")
      .option("multiline", value = true)
      // double quotes inside quoted text ("a b ""X"" a, bc")
      .option("quote", "\"")
      .option("escape", "\"")
      .csv(listingPath)
      .as[Listing]
  }
}
