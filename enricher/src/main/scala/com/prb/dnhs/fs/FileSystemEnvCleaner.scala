package com.prb.dnhs.fs

import org.apache.hadoop.fs._
import org.apache.spark.sql.SparkSession
import org.slf4j.Logger

abstract class FileSystemEnvCleaner {

  val log: Logger
  val sparkSession: SparkSession
  val fs: FileSystem

  val hdfsPath: String

  def cleanup(pathToFiles: Path): Unit = {
    val processedPath = new Path(s"$hdfsPath/processed")

    fs.moveFromLocalFile(pathToFiles, processedPath)
  }
}
