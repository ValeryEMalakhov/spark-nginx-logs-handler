package com.prb.dnhs.handlers

import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.Logger

abstract class FileSystemCleanerImpl extends FileSystemHandler[Unit] {

  val log: Logger
  val fs: FileSystem
  val mainPath: String

  override def handle(): Unit = {

    val procPath = new Path(s"$mainPath/processing")

    if (fs.exists(procPath)) {
      log.info("Clean up the working folder")
      fs.delete(procPath, true)
    } else {
      log.error("Missing working folder!")
      throw new RuntimeException("500") // 404
    }
  }
}
