package com.prb.dnhs.recorders

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}
import org.slf4j.Logger

abstract class HiveRecorderImpl extends DataRecorder[RDD[Row]] {

  val log: Logger
  val hiveContext: HiveContext
  val sqlContext: SQLContext
  val dataTableName: String
  val dataFrameGenericSchema: StructType
  val batchId: String

  override def save(logRow: RDD[Row]): Unit = {

    val logDF = hiveContext.createDataFrame(logRow, dataFrameGenericSchema)

    createTableIfNotExists(batchId)

    /**
      * The way in which a column with the value of the current PARTITION is added to the data first,
      * and after that the data is saved by the spark itself.
      */
    logDF
      .withColumn("batchId", lit(batchId))
      .write
      .partitionBy("batchId")
      .format("parquet")
      .insertInto(dataTableName)
  }

  // will be commented out, if there is no need to create a table
  private def createTableIfNotExists(batchId: String): Unit = {
    if (!hiveContext.tableNames.contains(dataTableName)) {
      // create new table if not exists
      hiveContext
        .createDataFrame(
          hiveContext.sparkContext.emptyRDD[Row],
          dataFrameGenericSchema
        )
        .withColumn("batchId", lit(batchId))
        .write
        .format("parquet")
        .format("parquet")
        .partitionBy("batchId")
        .saveAsTable(dataTableName)
    }
  }
}
