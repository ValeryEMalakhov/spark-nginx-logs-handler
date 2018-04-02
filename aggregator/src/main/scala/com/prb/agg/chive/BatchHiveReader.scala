package com.prb.agg.chive

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

abstract class BatchHiveReader {

  val sqlContext: HiveContext

  val batchTableName: String
  val logTableName: String

  def read(): DataFrame = {

    if (sqlContext.tableNames.contains(batchTableName)) {
      val edgeBatch =
        sqlContext
          .sql(s"SELECT max(batchId) FROM $batchTableName")
          .head

      sqlContext
        .sql(s"SELECT * FROM $logTableName WHERE batchId > ${edgeBatch(0)}")
    } else {
      sqlContext
        .sql(s"SELECT * FROM $logTableName")
    }
  }
}
