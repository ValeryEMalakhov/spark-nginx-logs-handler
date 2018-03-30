package com.prb.agg.chive

import com.prb.agg.entities.Ads
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.StructType
import org.slf4j.Logger

abstract class AdsHiveWriter {

  val sqlContext: HiveContext
  val warehouse: String

  val tableName: String
  // val tableSchema: StructType
  val log: Logger

  def write(data: RDD[Ads]): Unit = {

    import sqlContext.implicits._

    val dataDF: DataFrame = data.toDF

    dataDF.show(false)

    if(!sqlContext.tableNames.contains(tableName))
      dataDF.write.format("parquet").saveAsTable(tableName)
    else dataDF.write.format("parquet").insertInto(tableName)
  }

  /*
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
            s"STORED AS PARQUET"
        )
      }
    }
  */
}
