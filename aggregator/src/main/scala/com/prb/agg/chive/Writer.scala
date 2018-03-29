package com.prb.agg.chive

import com.prb.agg.entities.Ads
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

abstract class Writer {

  val sqlContext: HiveContext

  def write(data: RDD[Ads]): Unit ={

    import sqlContext.implicits._

    val dataDF: DataFrame = data.toDF
  }
}
