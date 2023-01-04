package com.bigdata.spark
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

object ETL {

  val JOB_NAME = "ETL_ROOM_TYPE"
  val DESTINATION_TABLE = "dim_room_type"
  val format = new java.text.SimpleDateFormat("yyyy-MM-dd")

  def main(args: Array[String]): Unit = {

    val sourceDir: String = args(0)
    val spark = SparkSession.builder().appName(JOB_NAME).master("local[*]").getOrCreate()

    import spark.implicits._

    val berlinListingsDS: Dataset[Listing] = spark.read.format("org.apache.spark.csv")
      .option("header", value = true)
      .option("inferSchema", value = true)
      // new lines in name column ("a \n b")
      .option("multiline", value = true)
      // double quotes inside quoted text ("a b ""X"" a, bc")
      .option("quote", "\"")
      .option("escape", "\"")
      .option("dateFormat", "yyyy-MM-dd")
      .csv(s"$sourceDir/listing/BerlinListings.csv")
      .as[Listing]

    val parisListingsDS: Dataset[Listing] = spark.read.format("org.apache.spark.csv")
      .option("header", value = true)
      .option("inferSchema", value = true)
      // new lines in name column ("a \n b")
      .option("multiline", value = true)
      // double quotes inside quoted text ("a b ""X"" a, bc")
      .option("quote", "\"")
      .option("escape", "\"")
      .csv(s"$sourceDir/listing/ParisListings.csv")
      .as[Listing]

    val madridListingsDS: Dataset[Listing] = spark.read.format("org.apache.spark.csv")
      .option("header", value = true)
      .option("inferSchema", value = true)
      // new lines in name column ("a \n b")
      .option("multiline", value = true)
      // double quotes inside quoted text ("a b ""X"" a, bc")
      .option("quote", "\"")
      .option("escape", "\"")
      .csv(s"$sourceDir/listing/MadridListings.csv")
      .as[Listing]

    val berlinRoomTypes = berlinListingsDS.map(_.room_type).distinct()
    val parisRoomTypes = parisListingsDS.map(_.room_type).distinct()
    val madridRoomTypes = madridListingsDS.map(_.room_type).distinct()

    val allRoomTypes = berlinRoomTypes
      .union(parisRoomTypes)
      .union(madridRoomTypes)
      .distinct()

    allRoomTypes.write.insertInto(DESTINATION_TABLE)
  }
}
