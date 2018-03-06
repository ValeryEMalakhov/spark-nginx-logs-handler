package com.prb.dnhs.handlers

import org.apache.hadoop.fs._
import org.apache.spark.sql.SparkSession
import org.slf4j.Logger

abstract class ProcessedFolderHandlerImpl extends FileSystemHandler[Unit] {

  val log: Logger
  val sparkSession: SparkSession
  val fs: FileSystem

  val hdfsPath: String
  val batchTableName: String
  val batchId: String

  override def handle(): Unit = {

    val batchPath = new Path(s"$hdfsPath/processing/$batchId")
    val processedPath = new Path(s"$hdfsPath/processed/$batchId")

    sparkSession.sql(
      s"INSERT INTO TABLE default.$batchTableName " +
        s"VALUES $batchId"
    )

    createFolder(batchPath, processedPath)

    if (fs.exists(batchPath)) {
      log.info("Clean up the working folder")
      fs.delete(batchPath, true)
    } else {
      log.error("Missing working folder!")
      throw new RuntimeException("500") // 404
    }
  }

  private def createFolder(from: Path, into: Path) = {
    fs.mkdirs(into)

    fileTransfer(from, into)
  }

  private def fileTransfer(from: Path, into: Path) = {
    val gzFilter = (path: Path) => path.getName.endsWith(".log.gz")

    val logFilesPathSeq = fs.listStatus(from).map(_.getPath).filter(f => gzFilter(f))

    fs.moveFromLocalFile(logFilesPathSeq, into)
  }
}
