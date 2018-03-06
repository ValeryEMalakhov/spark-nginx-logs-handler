package com.prb.dnhs

import java.io.File

import org.apache.spark.sql.SparkSession

object DriverContextIT extends DriverContext {

  override lazy val dcSparkSession: SparkSession =
    SparkSession
      .builder()
      .appName("LogsEnricherIT")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", new File("ITest/hive").getAbsolutePath)
      .enableHiveSupport()
      .getOrCreate()
}
