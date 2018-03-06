package com.prb.dnhs

import java.io.File
import java.net.URI

import com.prb.dnhs.handlers.{FileSystemHandler, ProcessedFolderHandlerImpl, WorkingFolderHandlerImpl}
import com.prb.dnhs.readers.{ArchiveReaderImpl, DataReader}
import com.prb.dnhs.recorders.{DataRecorder, HiveRecorderImpl}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import com.prb.dnhs.entities.SchemaRepository._
import com.prb.dnhs.processor.Processor
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

object DriverContextIT extends DriverContext {

  override lazy val pathToFiles: String =
    FileSystem.DEFAULT_FS + new File("ITest").getAbsolutePath

  override lazy val dcSparkSession: SparkSession =
    SparkSession
      .builder()
      .appName("LogsEnricherIT")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", new File("ITest/hive").getAbsolutePath)
      .enableHiveSupport()
      .getOrCreate()

  override lazy val dcFS: FileSystem =
    FileSystem.get(
      new URI(FileSystem.DEFAULT_FS),
      new Configuration(),
      FileSystem.FS_DEFAULT_NAME_KEY
    )
/*
  override val dcArchiveReader
  : DataReader[RDD[String]] =
    new ArchiveReaderImpl() {

      lazy val sparkSession = dcSparkSession
      lazy val defInputPath = s"$pathToFiles/READY"
      lazy val batchId = globalBatchId.toString
    }

  override val dcHiveRecorder
  : DataRecorder[RDD[Row]] =
    new HiveRecorderImpl() {

      lazy val log = logger
      lazy val sparkSession = dcSparkSession
      lazy val fs = dcFS
      lazy val dataTableName = config.getString("hive.logTable")
      lazy val batchTableName = config.getString("hive.batchTable")
      lazy val dataFrameGenericSchema = dcSchemaRepos.getSchema(GENERIC_EVENT).get
      lazy val batchId = globalBatchId.toString
    }

  override val dcWorkingFolderHandler
  : FileSystemHandler[Unit] =
    new WorkingFolderHandlerImpl() {

      lazy val log = logger
      lazy val sparkSession = dcSparkSession
      lazy val fs = dcFS
      lazy val hdfsPath = s"$pathToFiles/READY"
      lazy val batchTableName = config.getString("hive.batchTable")
      lazy val batchId = globalBatchId.toString
    }

  override val dcProcessedFolderHandler
  : FileSystemHandler[Unit] =
    new ProcessedFolderHandlerImpl() {

      lazy val log = logger
      lazy val sparkSession = dcSparkSession
      lazy val fs = dcFS
      lazy val hdfsPath = s"$pathToFiles/READY"
      lazy val batchTableName = config.getString("hive.batchTable")
      lazy val batchId = globalBatchId.toString
    }

  override lazy val dbData = dcSparkSession.sql(
    s"SELECT userCookie FROM ${config.getString("app.name")} " +
      "WHERE eventType = \"rt\""
  )

  override val processor = new Processor() {
    val log = logger
    val fsHandler = dcWorkingFolderHandler
    val fsProcessedHandler = dcProcessedFolderHandler
    val gzReader = dcArchiveReader
    val parser = mainParser
    val handler = dcMainHandler
    val hiveRecorder = dcHiveRecorder
  }*/
}
