package com.prb.dnhs.recorders

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}
import org.slf4j.Logger

abstract class HiveRecorderImpl extends DataRecorder[RDD[Row]] {

  val log: Logger
  val sparkSession: SparkSession
  val dataTableName: String
  val dataFrameGenericSchema: StructType
  val batchId: String

  override def save(logRow: RDD[Row], path: String): Unit = {

    val logDF = sparkSession.createDataFrame(logRow, dataFrameGenericSchema)

    createTableIfNotExists(batchId)

    /**
      * The way in which a column with the value of the current PARTITION is added to the data first,
      * and after that the data is saved by the spark itself.
      */
    logDF
      .withColumn("batchId", lit(batchId))
      .write
      .format("parquet")
      .insertInto(dataTableName)
  }

  // will be commented out, if there is no need to create a table
  private def createTableIfNotExists(batchId: String): Unit = {
    if (!sparkSession.catalog.tableExists(dataTableName)) {
      // create new table if not exists
      sparkSession
        .createDataFrame(
          sparkSession.sparkContext.emptyRDD[Row],
          dataFrameGenericSchema
        )
        .withColumn("batchId", lit(batchId))
        .write
        .format("parquet")
        .partitionBy("batchId")
        .saveAsTable(dataTableName)
    }
  }
}
