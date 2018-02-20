package com.prb.dnhs.constants

import org.apache.spark.sql.SparkSession

trait TestSparkSession {

  private val appName = "LogsEnricherTest"
  private val sparkMaster = "local[2]"

  lazy val sparkSession: SparkSession =
    SparkSession
      .builder()
      .appName(appName)
      .master(sparkMaster)
      .getOrCreate()
}

