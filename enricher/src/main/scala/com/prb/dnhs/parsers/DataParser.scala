package com.prb.dnhs.parsers

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

trait DataParser[T, O] {

  def parse(logData: T): O
}
