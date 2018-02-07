package com.prb.dnhs.recorders

import org.apache.spark.rdd.RDD

abstract class FileRecorderImpl extends DataRecorder[RDD[String]] {

  val filePath: String
  val batchId: Long

  override def save(data: RDD[String], path: String): Unit = {

    if (path == "")
      data.saveAsTextFile(s"$filePath/$batchId/")
    else
      data.saveAsTextFile(s"$path/$batchId/")
  }
}

