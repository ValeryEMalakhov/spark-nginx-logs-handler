package com.prb.dnhs.recorders

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row
import org.slf4j.Logger

abstract class HiveRecorderImpl extends DataRecorder[RDD[Row]] {

  val log: Logger
  val hiveContext: HiveContext
  val warehouse: String
  val dataTableName: String
  val dataFrameGenericSchema: StructType
  val batchId: String

  override def save(logRow: RDD[Row]): Unit = {

    createTableIfNotExists(batchId)

    // If absolutely empty batches should be recorded - comment on this check
    if (!(logRow.collect sameElements Array.empty[Row])) {

      val logDF = hiveContext.createDataFrame(logRow, dataFrameGenericSchema)

      /**
        * Spark 1.6.*
        * The way in which the data is first written to the warehouse-directory,
        * and then new PARTITION is added to the table.
        */

      logDF.write.parquet(s"$warehouse/bathcId=$batchId")

      hiveContext.sql(
        s"ALTER TABLE $dataTableName " +
          s"ADD IF NOT EXISTS PARTITION(batchId=$batchId) " +
          s"location \'$warehouse\'")

      /**
        * Spark 2.*
        * The way in which a column with the value of the current PARTITION is added to the data first,
        * and after that the data is saved by the spark itself.
        */
      /*
          logDF
            .withColumn("batchId", lit(batchId))
            .write
            .format("parquet")
            .insertInto(dataTableName)
      */
    }
  }

  // will be commented out, if there is no need to create a table
  private def createTableIfNotExists(batchId: String): Unit = {
    if (!hiveContext.tableNames.contains(dataTableName)) {
      // create new table if not exists

      // Spark 1.6.*
      val tableSchema =
        dataFrameGenericSchema.fields.map { f =>
          s"${f.name} ${f.dataType.simpleString}"
        }.mkString(", ")

      hiveContext.sql(
        s"CREATE TABLE IF NOT EXISTS $dataTableName($tableSchema) " +
          s"PARTITIONED BY (batchId BIGINT) " +
          s"STORED AS PARQUET"
      )

      // Spark 2.*
      /*
          hiveContext
            .createDataFrame(
              hiveContext.sparkContext.emptyRDD[Row],
              dataFrameGenericSchema
            )
            .withColumn("batchId", lit(batchId))
            .write
            .format("parquet")
            .partitionBy("batchId")
            .saveAsTable(dataTableName)
      */
    }
  }
}
