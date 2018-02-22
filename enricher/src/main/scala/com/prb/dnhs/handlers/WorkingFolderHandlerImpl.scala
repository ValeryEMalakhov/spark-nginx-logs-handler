package com.prb.dnhs.handlers

import org.apache.hadoop.fs._
import org.slf4j.Logger

abstract class WorkingFolderHandlerImpl extends FileSystemHandler[String] {

  val log: Logger
  val fs: FileSystem
  val mainPath: String

  override def handle(): String = {

    val procPath = new Path(s"$mainPath/processing")

    if (!fs.exists(procPath)) {
      log.info("No processing folder - create one and transfer files for processing")

      createWorkingFolder(procPath)
    } else {
      log.warn("The processing folder has remained from the previous processing - check folder data")

      val processedFiles = fs.listStatus(new Path(s"$mainPath/processing/"))

      if (processedFiles.isEmpty) {
        log.info("The processing folder is empty - re-create the working folder")

        fs.delete(procPath, true)

        createWorkingFolder(procPath)
      } else {
        log.info("The processing folder is not empty - try to find data among processed")

        val batchId = getBatchId(processedFiles)

        if (!fs.exists(new Path(s"$mainPath/processed/$batchId"))) {
          log.info("Data not processed yet - redefine batch id")

          batchId
        } else {
          log.info("Data already processed - re-create the working folder")

          fs.delete(procPath, true)

          createWorkingFolder(procPath)
        }
      }
    }
  }

  private def createWorkingFolder(procPath: Path) = {
    fs.mkdirs(procPath)

    fileTransfer(procPath)

    getBatchId(fs.listStatus(new Path(s"$mainPath/processing/")))
  }

  private def fileTransfer(procPath: Path) = {
    val gzFilter = (path: Path) => path.getName.endsWith(".log.gz")

    val logFilesPathSeq = fs.listStatus(new Path(s"$mainPath/")).map(_.getPath).filter(f => gzFilter(f))

    fs.moveFromLocalFile(logFilesPathSeq, procPath)
  }

  private def getBatchId(processedFiles: Seq[FileStatus]) = {
    val batchDefiningFile = processedFiles.map(_.getPath.toString).head

    val batchDefiningFileName = batchDefiningFile.substring(batchDefiningFile.lastIndexOf('/') + 1)

    batchDefiningFileName.split("_").head
  }
}
