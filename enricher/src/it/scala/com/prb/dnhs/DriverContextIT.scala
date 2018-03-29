package com.prb.dnhs

import java.io.File
import java.net.URI

import com.prb.dnhs.processor.Processor
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object DriverContextIT extends DriverContext {

  override lazy val warehouseLocation: String = "ITest/hive"

  override lazy val pathToFiles: String =
    FileSystem.DEFAULT_FS + new File("ITest").getAbsolutePath

  override lazy val dcSparkConfig: SparkConf =
    new SparkConf()
      .setAppName("LogsEnricherIT")
      .setMaster("local[1]")
      .set("spark.sql.warehouse.dir", warehouseLocation)
      .set("spark.sql.test", "")
      .set("hive.exec.dynamic.partition", "true")
      .set("hive.exec.dynamic.partition.mode", "nonstrict")
      .set("spark.sql.hive.convertMetastoreParquet", "true")

  override lazy val dcSparkContext: SparkContext =
    new SparkContext(dcSparkConfig)

  override lazy val dcHiveContext: HiveContext =
    new HiveContext(dcSparkContext)

  override lazy val dcSQLContext: SQLContext =
    new SQLContext(dcSparkContext)

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
    val hbaseRecorder = dcHBaseRecorder
  }
}
