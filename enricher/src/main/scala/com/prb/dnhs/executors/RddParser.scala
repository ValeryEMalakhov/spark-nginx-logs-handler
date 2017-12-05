package com.prb.dnhs.executors

import com.prb.dnhs.entities._
import org.apache.spark.rdd.RDD

trait RddParser {

  def parse(logRDD: RDD[String]): RDD[ParsedPixel]
}
