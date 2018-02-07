package com.prb.dnhs

import java.io.File
import java.time.Instant

import com.prb.dnhs.entities._
import com.prb.dnhs.exceptions._
import com.prb.dnhs.handlers._
import com.prb.dnhs.helpers._
import com.prb.dnhs.parsers._
import com.prb.dnhs.processor.Processor
import com.prb.dnhs.readers._
import com.prb.dnhs.recorders._
import com.prb.dnhs.validators._
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

  private val warehouseLocation = new File("spark-warehouse").getAbsolutePath

  // default path to hdfs data folder
  private val pathToFile =
    config.getString("hdfs.node") + config.getString("hdfs.files")

  // SparkSession for Spark 2.*.*
  lazy val sparkSession = SparkSession
    .builder()
    .appName(config.getString("app.name"))
    .master(config.getString("spark.master"))
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .enableHiveSupport()
    .getOrCreate()

  ///////////////////////////////////////////////////////////////////////////
  // Parsers
  ///////////////////////////////////////////////////////////////////////////

  private val schemasImpl: SchemaRepositorу = new SchemaRepositoryImpl()

  // string to row log parser in serializable container
  lazy val dcDataParserImpl =
    new SerializableContainer[DataParser[String, Either[ErrorDetails, Row]]] {
      override def obj = ExecutorContext.dataParserImpl
    }

  val mainParser
  : DataParser[RDD[String], RDD[Either[ErrorDetails, Row]]] =
    new MainParser() {

      lazy val parser = dcDataParserImpl
    }

  ///////////////////////////////////////////////////////////////////////////
  // Data readers
  ///////////////////////////////////////////////////////////////////////////

  private val archiveReaderImpl
  : DataReader[RDD[String]] =
    new ArchiveReaderImpl() {

      lazy val spark: SparkSession = sparkSession
      lazy val defInputPath: String = pathToFile
    }

  ///////////////////////////////////////////////////////////////////////////
  // Data recorders
  ///////////////////////////////////////////////////////////////////////////

  private lazy val globalBatchId = Instant.now.toEpochMilli

  // a recorder for storing the processed data and adding it to the database
  private val hiveRecorderImpl
  : DataRecorder[RDD[Row]] =
    new HiveRecorderImpl() {

      lazy val spark = sparkSession
      lazy val tableName = config.getString("app.name")
      lazy val schema = schemasImpl.getSchema(schemasImpl.GENERIC_EVENT).get
      lazy val batchId = globalBatchId
    }

  // a recorder for storing the invalid data
  private val fileRecorderImpl
  : DataRecorder[RDD[String]] =
    new FileRecorderImpl() {

      lazy val filePath = pathToFile + "DEFAULT/"
      lazy val batchId = globalBatchId
    }

  ///////////////////////////////////////////////////////////////////////////
  // Data handlers
  ///////////////////////////////////////////////////////////////////////////

  private val validRowHandlerImpl
  : RowHandler[RDD[Either[ErrorDetails, Row]], RDD[Row]] =
    new ValidRowHandler()

  private val invalidRowHandlerImpl
  : RowHandler[RDD[Either[ErrorDetails, Row]], Unit] =
    new InvalidRowHandler() {

      lazy val fileRecorder = fileRecorderImpl
    }

  private val mainHandler
  : RowHandler[RDD[Either[ErrorDetails, Row]], RDD[Row]] =
    new MainHandler {

      lazy val validRowHandler = validRowHandlerImpl
      lazy val invalidRowHandler = invalidRowHandlerImpl

      lazy val saveValidator = dcSaveValidatorImpl
    }

  ///////////////////////////////////////////////////////////////////////////
  // DB requests
  ///////////////////////////////////////////////////////////////////////////

  // stores data from the database at the time of application execution
  // this implementation assumes the existence of required table
  private lazy val dbData = sparkSession.sql(
    s"SELECT userCookie FROM ${config.getString("app.name")} " +
      "WHERE eventType = \"rt\""
  )
  // this one can be used even if the table not exists
  /*
    if (sparkSession.catalog.tableExists(config.getString("spark.name"))) {
        Some(sparkSession.sql(
          "SELECT dateTime, eventType, requesrId, userCookie " +
            s"FROM ${config.getString("spark.name")} " +
            "WHERE eventType = \"rt\""
        ))
    } else None
  */

  ///////////////////////////////////////////////////////////////////////////
  // Validators
  ///////////////////////////////////////////////////////////////////////////

  private val saveValidatorImpl
  : Validator[Row, Either[ErrorDetails, Row]] =
    new SaveAbilityValidatorImpl() {

      // lazy val table = dbData
      // lazy val clearTable = dbData.drop("batchId").collect.toSeq
      lazy val userCookies = dbData.collect.mkString("\t")
    }

  lazy val dcSaveValidatorImpl =
    new SerializableContainer[Validator[Row, Either[ErrorDetails, Row]]] {
      override def obj = saveValidatorImpl
    }

  ///////////////////////////////////////////////////////////////////////////
  // Processor
  ///////////////////////////////////////////////////////////////////////////

  // scopt object for app coordination
  val processor = new Processor() {
    val gzReader = archiveReaderImpl
    val parser = mainParser
    val handler = mainHandler
    val hiveRecorder = hiveRecorderImpl
  }

  ///////////////////////////////////////////////////////////////////////////
  // Other
  ///////////////////////////////////////////////////////////////////////////
}

