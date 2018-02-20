package com.prb.dnhs.recorders

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}

abstract class HiveRecorderImpl extends DataRecorder[RDD[Row]] {

  val sparkSession: SparkSession
  val hiveTableName: String
  val dataFrameGenericSchema: StructType
  val batchId: Long

  override def save(logRow: RDD[Row], path: String): Unit = {

    val logDF = sparkSession.createDataFrame(logRow, dataFrameGenericSchema)

    createTableIfNotExists()

    /**
      * The way in which a column with the value of the current PARTITION is added to the data first,
      * and after that the data is saved by the spark itself.
      */
    logDF
      .withColumn("batchId", lit(batchId))
      .write
      .format("parquet")
      .insertInto(hiveTableName)
  }

  // will be commented out, if there is no need to create a table
  private def createTableIfNotExists(): Unit = {
    if (!sparkSession.catalog.tableExists(hiveTableName)) {
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
        .saveAsTable(hiveTableName)
    }
  }
}
