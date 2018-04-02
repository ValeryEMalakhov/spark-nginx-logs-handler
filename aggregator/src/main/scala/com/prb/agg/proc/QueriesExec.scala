package com.prb.agg.proc

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.slf4j.Logger

abstract class QueriesExec {

  val log: Logger
  val sqlContext: HiveContext

  def executeDFQueries(data: DataFrame): Unit = {

    log.warn("Show all data in table")
    data.show(50, false)

    log.warn("Show the filtered data")
    data
      .filter(data("eventType") === "clk")
      .show(50, false)

    log.warn("Show the number of users in each age category who are interested in a particular product section")
    data
      .filter(data("eventType") === "clk")
      .groupBy("section", "ageCategory")
      .agg(
        count("userCookie")
          .as("Users interested in")
      )
      .orderBy("ageCategory")
      .show(50, false)

    log.warn("Show the number of users interested in a certain advertisement")
    data
      .filter(data("eventType") === "clk")
      .groupBy("AdId")
      .agg(
        count("userCookie")
          .as("Users interested in")
      )
      .orderBy("AdId")
      .show(50, false)

    log.warn("Shows the prospective age of the participant")
    val ageData = data
      .groupBy("userCookie")
      .agg(
        max("ageCategory")
          .as("minAge")
      )
    ageData.show(50, false)

    log.warn("Show the most popular age category")
    ageData
      .groupBy("minAge")
      .agg(
        count("userCookie")
          .as("userNumb")
      )
      .orderBy("userNumb")
      .show(1, false)

  }

  def executeSQLQueries(data: DataFrame): Unit = {

    data.registerTempTable("Ads_Logs")

    sqlContext
      .sql("SELECT * FROM Ads_Logs WHERE eventType like 'clk'")
      .show(false)
  }
}
