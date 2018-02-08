package com.prb.dnhs.constants

import org.apache.spark.sql.SparkSession

trait TestSparkSession {

  ///////////////////////////////////////////////////////////////////////////
  // Test constants
  ///////////////////////////////////////////////////////////////////////////

  private val appName = "LogsEnricher"
  private val sparkMaster = "local[2]"

  // private val warehouseLocation = new File("spark-warehouse").getAbsolutePath

  ///////////////////////////////////////////////////////////////////////////
  // Test spark config
  ///////////////////////////////////////////////////////////////////////////

  lazy val spark: SparkSession = SparkSession
    .builder()
    .appName(appName)
    .master(sparkMaster)
    .config("spark.driver.memory", 471859200)
    // .config("spark.sql.warehouse.dir", warehouseLocation)
    // .enableHiveSupport()
    .getOrCreate()
}

