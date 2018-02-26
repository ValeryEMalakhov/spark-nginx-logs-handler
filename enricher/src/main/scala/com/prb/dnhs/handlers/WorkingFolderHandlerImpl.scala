package com.prb.dnhs.handlers

import org.apache.hadoop.fs._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.Logger

abstract class WorkingFolderHandlerImpl extends FileSystemHandler[Unit] {

  val log: Logger
  val sparkSession: SparkSession
  val fs: FileSystem

  val hdfsPath: String
  val batchTableName: String
  val batchId: String

  override def handle(): Unit = {

    val mainPath = new Path(s"$hdfsPath/")
    val batchPath = new Path(s"$hdfsPath/processing/$batchId")

    val processedFolder = fs.listStatus(new Path(s"$hdfsPath/processing/"))

    log.warn("The processing folder has remained from the previous processing - check folder data")

    if (processedFolder.isEmpty) {
      log.info("The processing folder is empty - re-create the working folder")

      createFolder(mainPath, batchPath)
    } else {
      log.info("The processing folder is not empty - try to find data among processed")

      val batchDefiningPath = processedFolder.map(_.getPath).head
      val batchDefiningFile = batchDefiningPath.toString
      val batchDefiningId = batchDefiningFile.substring(batchDefiningFile.lastIndexOf('/') + 1)

      val batchTable: DataFrame = sparkSession.sql(
        "SELECT batchId " +
          s"FROM default.$batchTableName " +
          s"WHERE batchId = $batchDefiningId ;"
      )

      if (batchTable.collect.isEmpty) {
        log.info("Data not processed - re-create the batch folder")

        // fs.rename(batchDefiningPath, batchPath)
        createFolder(batchDefiningPath, batchPath)

        fs.delete(batchDefiningPath, true)
      } else {
        log.info("Data already processed - re-create the working folder")

        fs.delete(batchDefiningPath, true)

        createFolder(mainPath, batchPath)
      }
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
