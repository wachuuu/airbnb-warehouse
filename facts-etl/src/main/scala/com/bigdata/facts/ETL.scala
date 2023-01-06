package com.bigdata.facts

import CalendarParser.parseCalendars
import DataGrouper.joinAndGroupDatasets
import ListingsParser.parseListings
import ReviewsInputParser.parseFiles
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

import java.time.LocalDate
import scala.io.Source


case class ReviewEntry(
                        id: Int,
                        neighbourhood: String,
                        date: LocalDate,
                        host_id: Int,
                        room_type: String,
                        available_listings_count: BigInt,
                        not_available_listings_count: BigInt,
                        prices_sum: Double,
                        ratings_sum: Double,
                        declared_reviews_count: BigInt,
                      )

object ETL {

  val JOB_NAME = "ETL_FACTS_TABLE"
  val TABLE_NAME = "tof_reviews"

  val POSITIVE_WORDS_FILE_NAMES: List[String] = List(
    "positive_words_de.txt",
    "positive_words_es.txt",
    "positive_words_fr.txt",
    "positive_words_en.txt",
  )

  val NEGATIVE_WORDS_FILE_NAMES: List[String] = List(
    "negative_words_de.txt",
    "negative_words_es.txt",
    "negative_words_fr.txt",
    "negative_words_en.txt",
  )

  implicit val myObjEncoder: Encoder[InputReview] = org.apache.spark.sql.Encoders.kryo[InputReview]

  def main(args: Array[String]): Unit = {
    val dataPath: String = args(0)
    val spark = SparkSession.builder().appName(JOB_NAME).master("local[*]").getOrCreate()

    val positiveWords = parsePositiveWordFiles()
    val negativeWords = parseNegativeWordFiles()

    val ratedReviewsDs: Dataset[RatedReview] = parseFiles(dataPath, spark, positiveWords, negativeWords)
    val listingsDs: Dataset[Listing] = parseListings(dataPath, spark)
    val calendarsDs: Dataset[CalendarEntry] = parseCalendars(dataPath, spark)

    val reviewsListingsCalendarEntriesDs: Dataset[ReviewEntry] =
      joinAndGroupDatasets(ratedReviewsDs, listingsDs, calendarsDs, spark)

    reviewsListingsCalendarEntriesDs.write.insertInto(TABLE_NAME)
  }

  def parsePositiveWordFiles(): Set[String] = {
    POSITIVE_WORDS_FILE_NAMES
      .flatMap(filename => Source.fromResource(filename).getLines())
      .toSet
  }

  def parseNegativeWordFiles(): Set[String] = {
    NEGATIVE_WORDS_FILE_NAMES
      .flatMap(filename => Source.fromResource(filename).getLines())
      .toSet
  }
}
