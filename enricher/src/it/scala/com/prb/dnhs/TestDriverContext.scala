package com.prb.dnhs

import java.io.File

import org.apache.spark.sql.SparkSession

object TestDriverContext extends DriverContext {

  override lazy val dcSparkSession: SparkSession =
    SparkSession
      .builder()
      .appName("LogsEnricherIT")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", new File("ITest").getAbsolutePath)
      .enableHiveSupport()
      .getOrCreate()
}
