package com.bigdata.facts

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

import java.time.LocalDate
import java.time.format.DateTimeFormatter

case class InputReview(listingId: Int, date: LocalDate, comment: String)

case class RatedReview(listingId: Int, date: LocalDate, rating: Double)

object ReviewsInputParser {

  val formatter: DateTimeFormatter = DateTimeFormatter.ISO_DATE
  val PATTERN: scala.util.matching.Regex = """.{6} \d+ for listing (\d+) .+(\d{4}-\d+-\d+):((?!")(.+)|"[^"]+")""".r

  def parseFiles(dataPath: String, spark: SparkSession, positiveWords: Set[String], negativeWords: Set[String]): Dataset[RatedReview] = {
    import spark.implicits._
    val berlinReviewsRdd: RDD[InputReview] = parseReviewFilesToRdd(s"$dataPath/review/BerlinReviews.txt", spark)
    val madridReviewsRdd: RDD[InputReview] = parseReviewFilesToRdd(s"$dataPath/review/MadridReviews.txt", spark)
    val parisReviewsRdd: RDD[InputReview] = parseReviewFilesToRdd(s"$dataPath/review/ParisReviews.txt", spark)

    val reviewsRdd = berlinReviewsRdd
      .union(madridReviewsRdd)
      .union(parisReviewsRdd)

    spark.createDataset(rateReviews(reviewsRdd,
      positiveWords,
      negativeWords))
    //    spark.createDataset(rateReviews(parseReviewFileToRdd(s"$dataPath/ParisReviews.txt", "MADRID", spark),
    //      positiveWords,
    //      negativeWords))
  }

  private def rateReviews(reviewsRdd: RDD[InputReview],
                          positiveWordSet: Set[String],
                          negativeWordSet: Set[String]): RDD[RatedReview] = {
    reviewsRdd.map(entry => {
      RatedReview(
        listingId = entry.listingId,
        date = entry.date,
        rating = rateComment(entry.comment, positiveWordSet, negativeWordSet))
    })
  }

  private def rateComment(comment: String,
                          positiveWordSet: Set[String],
                          negativeWordSet: Set[String]): Double = {
    val (positiveCount: Int, negativeCount: Int) = comment
      .replaceAll("""[".?!,]""", "")
      .split(" ")
      .foldLeft((0, 0))((counts, word) => {
        var positiveContains = 0
        var negativeContains = 0
        if (positiveWordSet.contains(word.toLowerCase)) {
          positiveContains += 1
        }
        if (negativeWordSet.contains(word.toLowerCase)) {
          negativeContains += 1
        }
        (counts._1 + positiveContains, counts._2 + negativeContains)
      })

    calculateRating(positiveCount, negativeCount)
  }

  /**
   * Returns normalised (in 0-5 scale) predicted rating out of counts of:
   *
   * @param positiveCount - positive count of words in a comment
   * @param negativeCount - negative count of words in a comment
   * @return normalised (in 0-5 scale) rating
   */
  private def calculateRating(positiveCount: Int, negativeCount: Int): Double = {
    if ((positiveCount == 0) && (negativeCount == 0)) {
      return 2.5
    }
    (((positiveCount - negativeCount) / (positiveCount + negativeCount).toDouble) + 1) * 2.5
  }

  private def parseReviewFilesToRdd(pathToFile: String,sparkSession: SparkSession): RDD[InputReview] = {
    sparkSession.sparkContext
      .wholeTextFiles(pathToFile)
      .flatMap { case (_, txt) => parseReviewFile(txt) }
  }

  private def parseReviewFile(file: String): Iterator[InputReview] = {
    PATTERN.findAllMatchIn(file)
      .map(m => InputReview(m.group(1).toInt, LocalDate.parse(m.group(2), formatter), m.group(3)))
  }
}
