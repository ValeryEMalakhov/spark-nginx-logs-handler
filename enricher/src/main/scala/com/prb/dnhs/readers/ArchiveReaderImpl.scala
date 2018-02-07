package com.prb.dnhs.readers

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

abstract class ArchiveReaderImpl extends DataReader[RDD[String]] {

  val spark: SparkSession
  val defInputPath: String

  /**
    * Get the data from the default directory or, if exists,
    * from the directory specified in the command-line arguments.
    */
  override def read(inputDir: String): RDD[String] = {
    spark
      .sparkContext
      .textFile {
        if (inputDir == "") s"$defInputPath/READY/*.gz"
        else s"$inputDir/*.gz"
      }
  }
}
