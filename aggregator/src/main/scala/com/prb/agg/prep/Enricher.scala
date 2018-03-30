package com.prb.agg.prep

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._

abstract class Enricher {

  val sqlContext: HiveContext

  def prepare(data: RDD[String]): DataFrame = {

    val parsedData = data.map{ row =>
      val r = row.split(',')
      val list = if (r(7) != "null") r(7).split('|').toSeq else null
      val adsID = if (r(8) != "null") r(8).toInt else null

      Row(r(0), r(1), r(2), r(3), r(4), r(5), r(6), list, adsID)
    }

    val schema = StructType(
      StructField("logDateTime", StringType, false) ::
        StructField("eventType", StringType, false) ::
        StructField("requesrId", StringType, false) ::
        StructField("userCookie", StringType, false) ::
        StructField("site", StringType, false) ::
        StructField("ipAddress", StringType, false) ::
        StructField("useragent", StringType, false) ::
        StructField("segments", ArrayType(StringType, true), true) ::
        StructField("AdId", IntegerType, true) ::
        Nil
    )

    sqlContext.createDataFrame(parsedData, schema)
  }
}
