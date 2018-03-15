package com.prb.dnhs.readers

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

abstract class ArchiveReaderImpl extends DataReader[RDD[String]] {

  val sparkSession: SparkSession
  val defInputPath: String
  val batchId: String

  /**
    * Get the data from the default directory or, if exists,
    * from the directory specified in the command-line arguments.
    */
  override def read(inputDir: String): RDD[String] =
    sparkSession
      .sparkContext
      .textFile {
        //TODO: to handler dir
        if (inputDir == "") s"$defInputPath/processing/$batchId/*.gz"
        else s"$inputDir/*.gz"
      }
}
