package com.prb.dnhs

import java.io.File
import java.net.URI

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
object DriverContext extends ConfigHelper with LoggerHelper {

  ///////////////////////////////////////////////////////////////////////////
  // Spark conf
  ///////////////////////////////////////////////////////////////////////////

  private val warehouseLocation = new File("sparkSession-warehouse").getAbsolutePath

  // default path to hdfs data folder
  private val pathToFiles =
    s"${config.getString("hdfs.node")}/${config.getString("hdfs.files")}"

  // SparkSession for Spark 2.*.*
  private lazy val dcSparkSession = SparkSession
    .builder()
    .appName(config.getString("app.name"))
    .master(config.getString("spark.master"))
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .enableHiveSupport()
    .getOrCreate()

  private lazy val dcFS = FileSystem.get(
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

  val mainParser
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
    }

  ///////////////////////////////////////////////////////////////////////////
  // Data recorders
  ///////////////////////////////////////////////////////////////////////////

  // private lazy val globalBatchId = Instant.now.toEpochMilli

  // a recorder for storing the processed data and adding it to the database
  private val dcHiveRecorder
  : DataRecorder[RDD[Row]] =
    new HiveRecorderImpl() {

      lazy val sparkSession = dcSparkSession
      lazy val hiveTableName = config.getString("app.name")
      lazy val dataFrameGenericSchema = dcSchemaRepos.getSchema(GENERIC_EVENT).get
    }

  // a recorder for storing the invalid data
  private val dcFileRecorder
  : DataRecorder[RDD[String]] =
    new FileRecorderImpl() {

      lazy val fileSaveDirPath = pathToFiles + "/DEFAULT"
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
  : FileSystemHandler[String] =
    new WorkingFolderHandlerImpl() {

      lazy val log = logger
      lazy val fs = dcFS
      lazy val mainPath = s"$pathToFiles/READY"
    }

  private val dcFileSystemCleaner
  : FileSystemHandler[Unit] =
    new FileSystemCleanerImpl() {

      lazy val log = logger
      lazy val fs = dcFS
      lazy val mainPath = s"$pathToFiles/READY"
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
    val gzReader = dcArchiveReader
    val parser = mainParser
    val handler = dcMainHandler
    val hiveRecorder = dcHiveRecorder
    val fsCleaner = dcFileSystemCleaner
  }

  ///////////////////////////////////////////////////////////////////////////
  // Other
  ///////////////////////////////////////////////////////////////////////////
}

