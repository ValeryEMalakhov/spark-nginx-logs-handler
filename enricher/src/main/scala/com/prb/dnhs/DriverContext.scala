package com.prb.dnhs

import java.io.File
import java.net.URI
import java.time.Instant

import com.prb.dnhs.entities.SchemaRepository._
import com.prb.dnhs.entities._
import com.prb.dnhs.exceptions._
import com.prb.dnhs.handlers.{FileSystemHandler, _}
import com.prb.dnhs.helpers._
import com.prb.dnhs.parsers._
import com.prb.dnhs.processor.Processor
import com.prb.dnhs.readers._
import com.prb.dnhs.recorders._
import com.prb.dnhs.validators._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

/**
  * The DriverContext object contains a number of parameters
  * that enable to work with Spark.
  */
class DriverContext extends ConfigHelper with LoggerHelper {

  ///////////////////////////////////////////////////////////////////////////
  // Spark conf
  ///////////////////////////////////////////////////////////////////////////

  lazy val warehouseLocation: String =
    new File("sparkSession-warehouse").getAbsolutePath

  // default path to hdfs data folder

  lazy val pathToFiles: String =
    s"${config.getString("hdfs.node")}/${config.getString("hdfs.files")}"

  // SparkSession for Spark 2.*.*
  lazy val dcSparkSession: SparkSession =
    SparkSession
      .builder()
      .appName(config.getString("app.name"))
      .master(config.getString("spark.master"))
      //.config("spark.sql.warehouse.dir", warehouseLocation)
      .config("hive.metastore.uris", config.getString("hive.address"))
      .enableHiveSupport()
      .getOrCreate()

  lazy val dcFS: FileSystem =
    FileSystem.get(
      new URI(config.getString("hdfs.node")),
      new Configuration(),
      config.getString("hdfs.user")
    )

  ///////////////////////////////////////////////////////////////////////////
  // Parsers
  ///////////////////////////////////////////////////////////////////////////

  private val dcSchemaRepos = new SchemaRepositoryImpl()

  // string to row log parser in serializable container
  private lazy val dcDataParser =
    new SerializableContainer[DataParser[String, Either[ErrorDetails, Row]]] {
      override def obj = ExecutorContext.dataParserImpl
    }

  private val mainParser
  : DataParser[RDD[String], RDD[Either[ErrorDetails, Row]]] =
    new MainParser() {

      lazy val parser = dcDataParser
    }

  ///////////////////////////////////////////////////////////////////////////
  // Data readers
  ///////////////////////////////////////////////////////////////////////////

  private val dcArchiveReader
  : DataReader[RDD[String]] =
    new ArchiveReaderImpl() {

      lazy val sparkSession = dcSparkSession
      lazy val defInputPath = s"$pathToFiles/READY"
      lazy val batchId = globalBatchId
    }

  ///////////////////////////////////////////////////////////////////////////
  // Data recorders
  ///////////////////////////////////////////////////////////////////////////

  private lazy val globalBatchId = Instant.now.toEpochMilli

  // a recorder for storing the processed data and adding it to the database
  private val dcHiveRecorder
  : DataRecorder[RDD[Row]] =
    new HiveRecorderImpl() {

      lazy val log = logger
      lazy val sparkSession = dcSparkSession
      lazy val fs = dcFS
      lazy val dataTableName = config.getString("hive.processed_data")
      lazy val batchTableName = config.getString("hive.processed_batches")
      lazy val dataFrameGenericSchema = dcSchemaRepos.getSchema(GENERIC_EVENT).get
      lazy val batchId = globalBatchId
    }

  // a recorder for storing the invalid data
  private val dcFileRecorder
  : DataRecorder[RDD[String]] =
    new FileRecorderImpl() {

      lazy val fileSaveDirPath = pathToFiles + "/DEFAULT"
      lazy val batchId = globalBatchId
    }

  ///////////////////////////////////////////////////////////////////////////
  // Data handlers
  ///////////////////////////////////////////////////////////////////////////

  private val dcValidRowHandler
  : RowHandler[RDD[Either[ErrorDetails, Row]], RDD[Row]] =
    new ValidRowHandler()

  private val dcInvalidRowHandler
  : RowHandler[RDD[Either[ErrorDetails, Row]], Unit] =
    new InvalidRowHandler() {

      lazy val fileRecorder = dcFileRecorder
    }

  private val dcMainHandler
  : RowHandler[RDD[Either[ErrorDetails, Row]], RDD[Row]] =
    new MainHandler {

      lazy val saveValidator = dcSaveValidatorImpl

      lazy val validRowHandler = dcValidRowHandler
      lazy val invalidRowHandler = dcInvalidRowHandler
    }

  private val dcWorkingFolderHandler
  : FileSystemHandler[Unit] =
    new WorkingFolderHandlerImpl() {

      lazy val log = logger
      lazy val sparkSession = dcSparkSession
      lazy val fs = dcFS
      lazy val hdfsPath = s"$pathToFiles/READY"
      lazy val batchTableName = config.getString("hive.batchTable")
      lazy val batchId = globalBatchId
    }

  private val dcProcessedFolderHandler
  : FileSystemHandler[Unit] =
    new ProcessedFolderHandlerImpl() {

      lazy val log = logger
      lazy val sparkSession = dcSparkSession
      lazy val fs = dcFS
      lazy val hdfsPath = s"$pathToFiles/READY"
      lazy val batchTableName = config.getString("hive.batchTable")
      lazy val batchId = globalBatchId
    }

  ///////////////////////////////////////////////////////////////////////////
  // DB requests
  ///////////////////////////////////////////////////////////////////////////

  // stores data from the database at the time of application execution
  // this implementation assumes the existence of required table
  private lazy val dbData = dcSparkSession.sql(
    s"SELECT userCookie FROM ${config.getString("app.name")} " +
      "WHERE eventType = \"rt\""
  )
  // this one can be used even if the table not exists
  /*
    if (sparkSession.catalog.tableExists(config.getString("sparkSession.name"))) {
        Some(sparkSession.sql(
          "SELECT dateTime, eventType, requesrId, userCookie " +
            s"FROM ${config.getString("sparkSession.name")} " +
            "WHERE eventType = \"rt\""
        ))
    } else None
  */

  ///////////////////////////////////////////////////////////////////////////
  // Validators
  ///////////////////////////////////////////////////////////////////////////

  private val dcSaveValidator
  : Validator[Row, Either[ErrorDetails, Row]] =
    new SaveAbilityValidatorImpl() {

      // lazy val table = dbData
      // lazy val clearTable = dbData.drop("batchId").collect.toSeq
      lazy val userCookies = dbData.collect.mkString("\t")
    }

  private lazy val dcSaveValidatorImpl =
    new SerializableContainer[Validator[Row, Either[ErrorDetails, Row]]] {
      override def obj = dcSaveValidator
    }

  ///////////////////////////////////////////////////////////////////////////
  // Processor
  ///////////////////////////////////////////////////////////////////////////

  // scopt object for app coordination
  val processor = new Processor() {
    val log = logger
    val fsHandler = dcWorkingFolderHandler
    val fsProcessedHandler = dcWorkingFolderHandler
    val gzReader = dcArchiveReader
    val parser = mainParser
    val handler = dcMainHandler
    val hiveRecorder = dcHiveRecorder
  }

  ///////////////////////////////////////////////////////////////////////////
  // Other
  ///////////////////////////////////////////////////////////////////////////
}

object DriverContext extends DriverContext

