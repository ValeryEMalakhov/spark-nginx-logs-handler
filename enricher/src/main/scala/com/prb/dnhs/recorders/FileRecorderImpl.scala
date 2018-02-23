package com.prb.dnhs.recorders

import org.apache.spark.rdd.RDD

abstract class FileRecorderImpl extends DataRecorder[RDD[String]] {

  val fileSaveDirPath: String
  val batchId: String

  override def save(data: RDD[String], path: String): Unit = {

    if (path == "")
      data.saveAsTextFile(s"$fileSaveDirPath/$batchId/")
    else
      data.saveAsTextFile(s"$path/$batchId/")
  }
}

