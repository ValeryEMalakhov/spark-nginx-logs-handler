package com.prb.dnhs.constants

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

trait TestSparkSession {

  private val appName = "LogsEnricherTest"
  private val sparkMaster = "local[2]"

  // SparkSession for Spark 1.6.* and older
  lazy val sparkConfig: SparkConf =
    new SparkConf()
      .setAppName(appName)
      .setMaster(sparkMaster)

  lazy val sparkContext: SparkContext =
    new SparkContext(sparkConfig)

  lazy val SQLContext: SQLContext =
    new SQLContext(sparkContext)

//  lazy val sparkSession: SparkSession =
//    SparkSession
//      .builder()
//      .appName(appName)
//      .master(sparkMaster)
//      .getOrCreate()
}

