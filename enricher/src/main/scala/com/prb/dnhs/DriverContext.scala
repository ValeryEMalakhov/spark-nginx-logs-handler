package com.prb.dnhs

import com.typesafe.config._
import org.apache.log4j._

import org.apache.spark._
import org.apache.spark.sql._

/**
  * The DriverContext object contains a number of parameters
  * that enable to work with Spark.
  */
object DriverContext {

  private val config: Config = ConfigFactory.load("application.conf")

    private val runStatus = "def"
//  private val runStatus = "debug"

  val pathToFile: String = config.getString(s"hdfs.$runStatus.node") + config.getString(s"hdfs.$runStatus.files")

  //  create Spark config with default settings
  private val sparkConf: SparkConf =
    if (runStatus == "debug") {
      new SparkConf()
        .setAppName(config.getString("spark.name"))
        .setMaster(config.getString(s"spark.$runStatus.master"))
    } else {
      new SparkConf()
        .setAppName(config.getString("spark.name"))
    }

  //  create Spark context with Spark configuration
  val sc: SparkContext = new SparkContext(sparkConf)

  //  since Cloudera used Spark 1.6.0, we use SQLContext instead of SparkSession.
  val sqlContext = new SQLContext(sc)

  /**
    * Block of global values
    */

  lazy val log: Logger = LogManager.getRootLogger

  lazy val core: DataFrame = sqlContext.read.parquet("src/main/resources/schemas/core.parquet")

  lazy val rt: DataFrame = sqlContext.read.parquet("src/main/resources/schemas/rtNotNullable.parquet")
  lazy val impr: DataFrame = sqlContext.read.parquet("src/main/resources/schemas/imprNotNullable.parquet")
  lazy val clk: DataFrame = sqlContext.read.parquet("src/main/resources/schemas/clkNotNullable.parquet")

  //  get merged schema without duplicated columns
  lazy val mergedSchema: DataFrame =
    rt.join(right = impr, usingColumns = rt.columns)
      .join(right = clk, usingColumns = (rt.columns ++ impr.columns).distinct)

  lazy val mutablePartOfSchema: Array[String] = mergedSchema.columns.drop(core.columns.length)
}
