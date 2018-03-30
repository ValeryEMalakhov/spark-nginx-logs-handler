package com.prb.agg.chive

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.slf4j.Logger

abstract class LogHiveWriter {
  val sqlContext: HiveContext
  val warehouse: String

  val tableName: String
  val batchId: String
  val log: Logger

  def write(data: DataFrame): Unit = {

    createTableIfNotExists(data)

    data.write.parquet(s"$warehouse/bathcId=$batchId")

    sqlContext.sql(
      s"ALTER TABLE $tableName " +
        s"ADD IF NOT EXISTS PARTITION(batchId=$batchId) " +
        s"location \'${warehouse.replaceAll("\\\\", "/")}/bathcId=$batchId\'"
    )
  }

  private def createTableIfNotExists(data: DataFrame): Unit = {
    if (!sqlContext.tableNames.contains(tableName)) {
      // create new table if not exists

      // Spark 1.6.*
      val tableSchema =
        data.schema.fields.map { f =>
          s"${f.name} ${f.dataType.simpleString}"
        }.mkString(", ")

      sqlContext.sql(
        s"CREATE TABLE IF NOT EXISTS $tableName($tableSchema) " +
          s"PARTITIONED BY (batchId BIGINT) " +
          s"STORED AS PARQUET"
      )
    }
  }
}
