package com.prb.dnhs

import com.prb.dnhs.helpers.ConfigHelper
import com.typesafe.config._
import org.apache.spark._
import org.apache.spark.sql._

/**
  * The DriverContext object contains a number of parameters
  * that enable to work with Spark.
  */
object DriverContext extends ConfigHelper {

  //TODO: maybe add that variable in scopt param
//   private val runStatus = "def"
  private val runStatus = "debug"

  val pathToFile: String = config.getString(s"hdfs.$runStatus.node") + config.getString(s"hdfs.$runStatus.files")

  // create Spark config with default settings
  private val sparkConf: SparkConf =
    if (runStatus == "debug") {
      new SparkConf()
        .setAppName(config.getString("spark.name"))
        .setMaster(config.getString(s"spark.$runStatus.master"))
    } else {
      new SparkConf()
        .setAppName(config.getString("spark.name"))
    }

  // create Spark context with Spark configuration
  val sc: SparkContext = new SparkContext(sparkConf)

  // since Cloudera used Spark 1.6.0, we use SQLContext instead of SparkSession.
  val sqlContext = new SQLContext(sc)
}
