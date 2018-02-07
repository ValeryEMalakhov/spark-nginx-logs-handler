package com.prb.dnhs.recorders

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}

abstract class HiveRecorderImpl extends DataRecorder[RDD[Row]] {

  val spark: SparkSession
  val tableName: String
  val schema: StructType
  val batchId: Long

  override def save(logRow: RDD[Row], path: String): Unit = {

    val logDF = spark.createDataFrame(logRow, schema)

    createTableIfNotExists()

    /**
      * The way in which a column with the value of the current PARTITION is added to the data first,
      * and after that the data is saved by the spark itself.
      */
    logDF
      .withColumn("batchId", lit(batchId))
      .write
      .format("parquet")
      .insertInto(tableName)
  }

  // will be commented out, if there is no need to create a table
  private def createTableIfNotExists(): Unit = {
    if (!spark.catalog.tableExists(tableName)) {
      // create new table if not exists
      spark
        .createDataFrame(
          spark.sparkContext.emptyRDD[Row],
          schema
        )
        .withColumn("batchId", lit(batchId))
        .write
        .format("parquet")
        .partitionBy("batchId")
        .saveAsTable(tableName)
    }
  }
}
