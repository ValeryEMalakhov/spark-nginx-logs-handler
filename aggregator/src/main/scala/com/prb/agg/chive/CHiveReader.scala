package com.prb.agg.chive

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

abstract class CHiveReader {

  val sqlContext: HiveContext

  def read(tableName: String): DataFrame = {

    sqlContext.sql(s"SELECT * FROM $tableName")
  }
}
