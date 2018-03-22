package com.prb.dnhs.fs

import org.apache.hadoop.fs._
import org.apache.spark.sql.SparkSession
import org.slf4j.Logger

abstract class FileSystemEnvPreparator {

  val log: Logger
  val sparkSession: SparkSession
  val fs: FileSystem

  val hdfsPath: String
  val dataTableName: String
  val batchId: String

  def prepareEnv(pathToFS: String): Path = {

    val mainPath = if (pathToFS != "") new Path(pathToFS) else new Path(hdfsPath)
    val batchPath = new Path(s"${mainPath.toString}/processing/$batchId")

    val processedFolder = fs.listStatus(new Path(s"${mainPath.toString}/processing"))

    log.warn("The processing folder has remained from the previous processing - check folder data")

    if (processedFolder.isEmpty) {
      log.info("The processing folder is empty - re-create the working folder")

      moveFiles(mainPath, batchPath)
    } else {
      log.info("The processing folder is not empty - try to find data among processed")

      validateFiles(mainPath, batchPath, processedFolder)
    }

    batchPath
  }

  private def validateFiles(mainPath: Path, batchPath: Path, processedFolder: Array[FileStatus]) = {
    val batchDefiningPath = processedFolder.map(_.getPath).head
    val batchDefiningId = batchDefiningPath.toString
      .substring(batchDefiningPath.toString.lastIndexOf('/') + 1)

    if (sparkSession.sql(s"SHOW PARTITIONS default.$dataTableName")
      .collect
      .exists(_.toString.contains(batchDefiningId))) {
      log.info("Data already processed - re-create the working folder")

      fs.delete(batchDefiningPath, true)

      moveFiles(mainPath, batchPath)
    } else {
      log.info("Data not processed - re-create the batch folder")

      moveFiles(batchDefiningPath, batchPath)

      fs.delete(batchDefiningPath, true)
    }
  }

  private def moveFiles(from: Path, into: Path) = {
    fs.mkdirs(into)

    val gzFilter = (path: Path) => path.getName.endsWith(".log.gz")

    val logFilesPathSeq = fs.listStatus(from).map(_.getPath).filter(f => gzFilter(f))

    fs.moveFromLocalFile(logFilesPathSeq, into)
  }
}
