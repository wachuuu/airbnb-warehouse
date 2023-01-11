package com.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

import java.time.LocalDate
import java.time.format.DateTimeFormatter

case class CalendarInputEntry(
                               listing_id: Int,
                               date: String,
                               available: String,
                               price: Option[String],
                             )

case class Calendar(
                     listing_id: Int,
                     date: LocalDate,
                     available: Boolean,
                     price: Option[Double]
                   )

case class DateSet(
                    date: LocalDate,
                    day: Int,
                    month: Int,
                    year: Int,
                  )


case class InputReview(listingId: Int, date: LocalDate, comment: String)

object ETL {

  val JOB_NAME = "ETL_DATE"
  val DESTINATION_TABLE = "dim_date"
  val PATTERN: scala.util.matching.Regex = """.{6} \d+ for listing (\d+) .+(\d{4}-\d+-\d+):((?!")(.+)|"[^"]+")""".r
  val FORMAT: DateTimeFormatter = DateTimeFormatter.ISO_DATE

  def main(args: Array[String]): Unit = {

    val sourceDir: String = args(0)
    val spark = SparkSession.builder().appName(JOB_NAME).master("local[*]").getOrCreate()

    val calendarDates = getCalendarDates(sourceDir, spark)
    val reviewDates = getReviewDates(sourceDir, spark)

    val allDates = calendarDates
      .union(reviewDates)
      .distinct()
      .sort("year", "month", "day")

    allDates.write.insertInto(DESTINATION_TABLE)
  }

  def getCalendarDate(sourceDir: String, file: String, spark: SparkSession): Dataset[DateSet] = {
    import spark.implicits._

    spark.read.format("org.apache.spark.csv")
      .option("header", value = true)
      .option("inferSchema", value = true)
      .option("multiline", value = true)
      .option("quote", "\"")
      .option("escape", "\"")
      .csv(s"$sourceDir/calendar/$file")
      .as[CalendarInputEntry]
      .map(entry => Calendar(
        listing_id = entry.listing_id,
        date = LocalDate.parse(entry.date, FORMAT),
        available = entry.available == "t",
        price = entry.price.map(_.replaceAll("[$,]", "").toDouble)
      ))
      .map(entry =>
        DateSet(date = entry.date,
          day = entry.date.getDayOfMonth,
          month = entry.date.getMonthValue,
          year = entry.date.getYear))
  }

  def getCalendarDates(sourceDir: String, spark: SparkSession): Dataset[DateSet] = {
    val berlinDatesDS = getCalendarDate(sourceDir, "BerlinCalendar.csv", spark)
    val madridDatesDS = getCalendarDate(sourceDir, "MadridCalendar.csv", spark)
    val parisDatesDS = getCalendarDate(sourceDir, "ParisCalendar.csv", spark)

    berlinDatesDS
      .union(madridDatesDS)
      .union(parisDatesDS)
      .distinct()
      .orderBy("year", "month", "day")
  }

  def getReview(sourceDir: String, file: String, spark: SparkSession): RDD[InputReview] = {
    spark.sparkContext
      .wholeTextFiles(s"$sourceDir/review/$file")
      .flatMap { case (_, txt) => parseReviewFile(txt) }
  }

  def getReviewDates(sourceDir: String, spark: SparkSession): Dataset[DateSet] = {
    import spark.implicits._

    val berlinReviewsRdd: RDD[InputReview] = getReview(sourceDir, "BerlinReviews.txt", spark)
    val mardidReviewsRdd: RDD[InputReview] = getReview(sourceDir, "MadridReviews.txt", spark)
    val parisReviewsRdd: RDD[InputReview] = getReview(sourceDir, "ParisReviews.txt", spark)

    val allDatesRdd = berlinReviewsRdd
      .union(mardidReviewsRdd)
      .union(parisReviewsRdd)

    spark.createDataset(allDatesRdd)
      .map(entry =>
        DateSet(date = entry.date,
          day = entry.date.getDayOfMonth,
          month = entry.date.getMonthValue,
          year = entry.date.getYear))
  }

  private def parseReviewFile(file: String): Iterator[InputReview] = {
    PATTERN.findAllMatchIn(file)
      .map(m => InputReview(m.group(1).toInt, LocalDate.parse(m.group(2), FORMAT), m.group(3)))
  }
}
