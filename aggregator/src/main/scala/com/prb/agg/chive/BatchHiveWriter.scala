package com.prb.agg.chive

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import org.slf4j.Logger

abstract class BatchHiveWriter {

  val sqlContext: HiveContext
  val warehouse: String
  val tableName: String
  val log: Logger

  def write(data: DataFrame): Unit = {

    val batchId =
      data
        .select("batchId")
        .orderBy(asc("batchId"))
        .head

    if (!sqlContext.tableNames.contains(tableName))
      sqlContext.sql(
        s"CREATE TABLE IF NOT EXISTS $tableName(batchId BigInt) " +
          s"STORED AS PARQUET"
      )

    sqlContext.sql(
      s"INSERT INTO TABLE $tableName VALUES " +
        s"($batchId(0)"
    )
  }
}
