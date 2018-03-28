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

  override lazy val pathToFiles: String =
    FileSystem.DEFAULT_FS + new File("ITest").getAbsolutePath

  override lazy val dcSparkConfig: SparkConf =
    new SparkConf()
      .setAppName("LogsEnricherIT")
      .setMaster("local[1]")
      .set("spark.sql.warehouse.dir", new File("ITest/hive").getAbsolutePath)
      .set("spark.sql.test", "")

  override lazy val dcSparkContext: SparkContext =
    new SparkContext(dcSparkConfig)

  override lazy val dcHiveContext: HiveContext =
    new HiveContext(dcSparkContext)

//  dcHiveContext.setConf("spark.sql.warehouse.dir", new File("ITest/hive").getAbsolutePath)
//  dcHiveContext.setConf("spark.sql.test", "")

  //  dcHiveContext.setConf("hive.metastore.uris", config.getString("hive.address"))

  override lazy val dcSQLContext: SQLContext =
    new SQLContext(dcSparkContext)

  //  override lazy val dcSparkSession = SparkSession
  //    .builder()
  //    .appName("LogsEnricherIT")
  //    .master("local[1]")
  //    .config("spark.sql.warehouse.dir", new File("ITest/hive").getAbsolutePath)
  //    .config("spark.sql.test", "")
  //    .enableHiveSupport()
  //    .getOrCreate()

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
