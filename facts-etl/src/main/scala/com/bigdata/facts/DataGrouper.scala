package com.bigdata.facts

import org.apache.spark.sql.functions.{col, count, monotonically_increasing_id, sum, when}
import org.apache.spark.sql.{Dataset, SparkSession}

import java.time.LocalDate
import java.time.format.DateTimeFormatter


case class JoinedRow(
                      neighbourhood: String,
                      date: LocalDate,
                      hostId: Int,
                      room_type: String,
                      available: Boolean,
                      price: Option[Double],
                      rating: Double,
                      number_of_reviews: Int,
                    )


case class ReviewSubEntry(
                           id: String,
                           neighbourhood: String,
                           date: String,
                           host_id: String,
                           room_type: String,
                           available_listings_count: BigInt,
                           not_available_listings_count: BigInt,
                           prices_sum: Option[Double],
                           ratings_sum: Double,
                           declared_reviews_count: BigInt,
                         )

object DataGrouper {

  val FORMAT: DateTimeFormatter = DateTimeFormatter.ISO_DATE

  def joinAndGroupDatasets(ratedReviewsDs: Dataset[RatedReview],
                           listingsDs: Dataset[Listing],
                           calendarsDs: Dataset[CalendarEntry],
                           sparkSession: SparkSession): Dataset[ReviewEntry] = {
    import sparkSession.implicits._

    val reviewsAndListingsDs: Dataset[(RatedReview, Listing)] = ratedReviewsDs
      .joinWith(listingsDs, ratedReviewsDs("listingId") === listingsDs("id"))
    val reviewsListingsCalendarEntriesDs: Dataset[JoinedRow] = reviewsAndListingsDs
      .joinWith(calendarsDs, (
        reviewsAndListingsDs("_1.listingId") === calendarsDs("listingId"))
        and
        (reviewsAndListingsDs("_1.date") === calendarsDs("date")),
        "left"
      ).map(entry => JoinedRow(
      neighbourhood = entry._1._2.neighbourhood,
      date = entry._1._1.date,
      hostId = entry._1._2.host_id,
      room_type = entry._1._2.room_type,
      // assuming that if no entry was found in *.Calendar.csv, then in the day of review the apartment was available.
      available = if (entry._2 == null) true else entry._2.available,
      price = if (entry._2 == null) Option(entry._1._2.price.toDouble) else entry._2.price,
      rating = entry._1._1.rating,
      number_of_reviews = entry._1._2.number_of_reviews
    ))
    reviewsListingsCalendarEntriesDs
      .groupBy(
        reviewsListingsCalendarEntriesDs("neighbourhood"),
        reviewsListingsCalendarEntriesDs("date"),
        reviewsListingsCalendarEntriesDs("hostId").as("host_id"),
        reviewsListingsCalendarEntriesDs("room_type")
      ).agg(
      count(when(col("available") === true, 1)).as("available_listings_count"),
      count(when(col("available") =!= true, 1)).as("not_available_listings_count"),
      // we want to keep the null, to know that for this review, no information was found in *Calendar.csv file
      sum(col("price")).as("prices_sum"),
      //        sum(when(col("price").isNull, 0).otherwise(col("price"))).as("prices_sum"),
      sum(col("rating")).as("ratings_sum"),
      sum(col("number_of_reviews")).as("declared_reviews_count")
    ).withColumn("id", monotonically_increasing_id())
      .as[ReviewSubEntry]
      .map(entry => ReviewEntry(id = entry.id,
        neighbourhood = entry.neighbourhood,
        date = LocalDate.parse(entry.date, FORMAT),
        host_id = entry.host_id,
        room_type = entry.room_type,
        available_listings_count = entry.available_listings_count,
        not_available_listings_count = entry.not_available_listings_count,
        prices_sum = entry.prices_sum,
        ratings_sum = entry.ratings_sum,
        declared_reviews_count = entry.declared_reviews_count))
  }

}
