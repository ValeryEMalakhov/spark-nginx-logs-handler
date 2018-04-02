package com.prb.agg.sim.chive

import com.prb.agg.entities.Ads
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.slf4j.Logger

abstract class AdsHiveWriter {

  val sqlContext: HiveContext
  val warehouse: String

  val tableName: String
  val log: Logger

  def write(data: RDD[Ads]): Unit = {

    import sqlContext.implicits._

    val dataDF: DataFrame = data.toDF

    dataDF.show(false)

    if(!sqlContext.tableNames.contains(tableName))
      dataDF.write.format("parquet").saveAsTable(tableName)
    else dataDF.write.format("parquet").insertInto(tableName)
  }
}
