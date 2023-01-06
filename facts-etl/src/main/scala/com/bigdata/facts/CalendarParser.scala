package com.bigdata.facts

import org.apache.spark.sql.{Dataset, SparkSession}

import java.time.LocalDate
import java.time.format.DateTimeFormatter


case class CalendarInputEntry(
                               listing_Id: Int,
                               date: String,
                               available: String,
                               price: Option[String],
                             )

case class CalendarEntry(
                          listingId: Int,
                          date: LocalDate,
                          available: Boolean,
                          price: Option[Double],
                        )

object CalendarParser {

  val formatter: DateTimeFormatter = DateTimeFormatter.ISO_DATE

  def parseCalendars(dataPath: String, spark: SparkSession): Dataset[CalendarEntry] = {

    val berlinCalendar = parseCalendarFiles(s"$dataPath/calendar/BerlinCalendar.csv", spark)
    val parisCalendar = parseCalendarFiles(s"$dataPath/calendar/ParisCalendar.csv", spark)
    val madridCalendar = parseCalendarFiles(s"$dataPath/calendar/MadridCalendar.csv", spark)

    berlinCalendar
      .union(parisCalendar)
      .union(madridCalendar)
    //    parseCalendarFiles(s"$dataPath/ParisCalendar.csv", spark)
  }

  private def parseCalendarFiles(calendarPath: String, spark: SparkSession): Dataset[CalendarEntry] = {
    import spark.implicits._
    spark.read.format("org.apache.spark.csv")
      .option("header", value = true)
      .option("inferSchema", value = true)
      // new lines in name column ("a \n b")
      .option("multiline", value = true)
      // double quotes inside quoted text ("a b ""X"" a, bc")
      .option("quote", "\"")
      .option("escape", "\"")
      .csv(calendarPath)
      .as[CalendarInputEntry]
      .map(entry => CalendarEntry(
        listingId = entry.listing_Id,
        date = LocalDate.parse(entry.date, formatter),
        available = entry.available == "t",
        price = entry.price.map(_.replaceAll("[$,]", "").toDouble)
      ))
  }
}
