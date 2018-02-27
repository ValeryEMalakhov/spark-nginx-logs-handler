package com.prb.dnhs.constants

import com.prb.dnhs.DriverContext
import org.apache.spark.sql.SparkSession

object TestDriverContext extends DriverContext {

  override lazy val dcSparkSession: SparkSession =
    SparkSession
      .builder()
      .appName("LogsEnricherIT")
      .master("local[*]")
      //.config("spark.sql.warehouse.dir", warehouseLocation)
      .config("hive.metastore.uris", config.getString("hive.address"))
      .enableHiveSupport()
      .getOrCreate()
}
