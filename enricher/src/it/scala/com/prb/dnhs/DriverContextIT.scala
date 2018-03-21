package com.prb.dnhs

import java.io.File
import java.net.URI

import com.prb.dnhs.processor.Processor
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession

object DriverContextIT extends DriverContext {

  override lazy val pathToFiles: String =
    FileSystem.DEFAULT_FS + new File("ITest").getAbsolutePath

  override lazy val dcSparkSession = SparkSession
    .builder()
    .appName("LogsEnricherIT")
    .master("local[1]")
    .config("spark.sql.warehouse.dir", new File("ITest/hive").getAbsolutePath)
    .config("spark.sql.test", "")
    .enableHiveSupport()
    .getOrCreate()

  override lazy val dcFS: FileSystem =
    FileSystem.get(
      new URI(FileSystem.DEFAULT_FS),
      new Configuration(),
      FileSystem.FS_DEFAULT_NAME_KEY
    )

  override val processor = new Processor() {
    val log = logger
    val fsPreparator = dcFileSystemPreparator
    val fsCleaner = dcFileSystemCleaner
    val gzReader = dcArchiveReader
    val parser = mainParser
    val handler = dcMainHandler
    val hiveRecorder = dcHiveRecorder
  }
}
