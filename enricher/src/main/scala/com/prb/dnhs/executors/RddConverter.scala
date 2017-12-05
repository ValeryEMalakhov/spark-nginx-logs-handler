package com.prb.dnhs.executors

import com.prb.dnhs.entities.DefaultParsedPixel
import org.apache.spark.rdd.RDD

/**
  * The `RddConverter` trait can be used to
  * convert the RDD[ParsedPixel] in the different data structures (DataFrame etc.)
  */
trait RddConverter[T] {

  /**
    * Convert a RDD in the different data type.
    *
    * @param logRDD the RDD which contain the objects with `ParsedPixel` type
    * @return a new data structure (DataFrame etc.)
    */
  def convert(logRDD: RDD[DefaultParsedPixel]): T
}

