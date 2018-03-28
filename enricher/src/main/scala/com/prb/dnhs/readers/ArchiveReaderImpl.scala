package com.prb.dnhs.readers

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

abstract class ArchiveReaderImpl extends DataReader[RDD[String]] {

  val sparkContext: SparkContext
  val defInputPath: String
  val batchId: String

  /**
    * Get the data from the default directory or, if exists,
    * from the directory specified in the command-line arguments.
    */
  override def read(inputDir: String): RDD[String] =
    sparkContext.textFile(s"$inputDir/*.gz")
}
