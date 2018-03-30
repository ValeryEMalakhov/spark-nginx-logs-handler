package com.prb.agg.file

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

abstract class SparkFileReader {

  val sparkContext: SparkContext

  def read(path: String = "data/*.csv"): RDD[String] = {

    sparkContext.textFile(path)
  }
}
