package com.prb.dnhs

import com.typesafe.config._

import org.apache.spark._
import org.apache.spark.sql._

/**
  * The DriverContext object contains a number of parameters
  * that enable to work with Spark.
  */
object DriverContext {

  private val config: Config = ConfigFactory.load("application.conf")

//  private val runStatus = "remote"
  private val runStatus = "local"

  val pathToFile: String = config.getString(s"hdfs.$runStatus.node") + config.getString(s"hdfs.$runStatus.files")

  // create Spark config with default settings
  private val sparkConf: SparkConf = new SparkConf()
    .setAppName(config.getString("spark.name"))
    .setMaster(config.getString(s"spark.$runStatus.master"))

  // create Spark context with Spark configuration
  val sc: SparkContext = new SparkContext(sparkConf)

  // since cloudera used Spark 1.6.0, we use SQLContext instead of SparkSession.
  val sqlContext = new SQLContext(sc)
}
