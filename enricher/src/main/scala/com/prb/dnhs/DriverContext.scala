package com.prb.dnhs

import java.io.File
import java.net.URI
import java.time.Instant

import com.prb.dnhs.entities.SchemaRepository._
import com.prb.dnhs.entities._
import com.prb.dnhs.exceptions._
import com.prb.dnhs.fs.{FileSystemEnvCleaner, FileSystemEnvPreparator}
import com.prb.dnhs.handlers._
import com.prb.dnhs.helpers._
import com.prb.dnhs.parsers._
import com.prb.dnhs.processor.Processor
import com.prb.dnhs.readers._
import com.prb.dnhs.recorders._
import com.prb.dnhs.recorders.hbase.HBaseRecorder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.StructType
import org.slf4j.Logger

/**
  * The DriverContext object contains a number of parameters
  * that enable to work with Spark.
  */
class DriverContext extends ConfigHelper
  with LoggerHelper
  with Serializable {

  ///////////////////////////////////////////////////////////////////////////
  // Spark conf
  ///////////////////////////////////////////////////////////////////////////

  lazy val warehouseLocation: String =
    new File("sparkSession-warehouse").getAbsolutePath

  // default path to hdfs data folder

  lazy val pathToFiles: String =
    s"${config.getString("hdfs.node")}/${config.getString("hdfs.files")}"

  // SparkSession for Spark 1.6.* and older
  lazy val dcSparkConfig: SparkConf =
    new SparkConf()
      .setAppName(config.getString("app.name"))
      .setMaster(config.getString("spark.master"))
      .set("spark.sql.warehouse.dir", warehouseLocation)
      .set("hive.metastore.uris", config.getString("hive.address"))
      .set("spark.sql.hive.convertMetastoreParquet", "true")

  lazy val dcSparkContext: SparkContext =
    new SparkContext(dcSparkConfig)

  lazy val dcHiveContext: HiveContext =
    new HiveContext(dcSparkContext)

  lazy val dcSQLContext: SQLContext =
    new SQLContext(dcSparkContext)

  // SparkSession for Spark 2.* and higher
  //  lazy val dcSparkSession: SparkSession =
  //    SparkSession
  //      .builder()
  //      .appName(config.getString("app.name"))
  //      .master(config.getString("spark.master"))
  //      //.config("hive.metastore.uris", config.getString("hive.address"))
  //      //.enableHiveSupport()
  //      .getOrCreate()

  lazy val dcFS: FileSystem =
    FileSystem.get(
      new URI(config.getString("hdfs.node")),
      new Configuration(),
      config.getString("hdfs.user")
    )

  lazy val dcHBaseConf = HBaseConfiguration.create()
  // conf.addResource(new Path("/etc/hbase/conf/core-site.xml"))
  // conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"))
  lazy val dcHBaseConnection: Connection = ConnectionFactory.createConnection(dcHBaseConf)
  lazy val dcHBaseContext = new HBaseContext(dcSparkContext, dcHBaseConf)

  ///////////////////////////////////////////////////////////////////////////
  // Parsers
  ///////////////////////////////////////////////////////////////////////////

  val dcSchemaRepos = new SchemaRepositoryImpl()

  // string to row log parser in serializable container
  val dcDataParser =
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

  val dcArchiveReader
  : DataReader[RDD[String]] =
    new ArchiveReaderImpl() {

      lazy val sparkContext = dcSparkContext
      lazy val defInputPath = s"$pathToFiles/READY"
      lazy val batchId = globalBatchId.toString
    }

  ///////////////////////////////////////////////////////////////////////////
  // Data recorders
  ///////////////////////////////////////////////////////////////////////////

  lazy val globalBatchId = Instant.now.toEpochMilli

  // a recorder for storing the processed data and adding it to the database
  val dcHiveRecorder
  : DataRecorder[RDD[Row]] =
    new HiveRecorderImpl() {
      lazy val log = logger
      lazy val hiveContext = dcHiveContext
      lazy val warehouse = warehouseLocation
      lazy val dataTableName = config.getString("hive.table")
      lazy val dataFrameGenericSchema = dcSchemaRepos.getSchema(GENERIC_EVENT).get
      lazy val batchId = globalBatchId.toString
    }

  val dcHBaseRecorder
  : DataRecorder[RDD[Row]] =
    new HBaseRecorder {
      lazy val log = logger
      lazy val sparkContext = dcSparkContext
      lazy val sqlContext = dcSQLContext
      lazy val hbaseConn = dcHBaseConnection
      lazy val hbaseContext = dcHBaseContext
      lazy val tableName = config.getString("hbase.table")
      lazy val columnFamily = config.getString("hbase.family")
      lazy val columnQualifier = config.getString("hbase.qualifier")
      lazy val genericSchema = dcSchemaRepos.getSchema(GENERIC_EVENT).get

    }

  /*
    val dcHBaseRecorder =
      new SerializableContainer[DataRecorder[RDD[Row]]] {
        override def obj = hbaseRecorder
      }
  */

  ///////////////////////////////////////////////////////////////////////////
  // Data handlers
  ///////////////////////////////////////////////////////////////////////////

  val dcValidRowHandler
  : RowHandler[RDD[Either[ErrorDetails, Row]], RDD[Row]] =
    new ValidRowHandler()

  val dcInvalidRowHandler
  : RowHandler[RDD[Either[ErrorDetails, Row]], Unit] =
    new InvalidRowHandler() {}

  val dcMainHandler
  : RowHandler[RDD[Either[ErrorDetails, Row]], RDD[Row]] =
    new MainHandler {
      lazy val validRowHandler = dcValidRowHandler
      lazy val invalidRowHandler = dcInvalidRowHandler
    }

  val dcFileSystemPreparator
  : FileSystemEnvPreparator =
    new FileSystemEnvPreparator() {
      lazy val log = logger
      lazy val sqlContext = dcSQLContext
      lazy val fs = dcFS
      lazy val hdfsPath = s"$pathToFiles/READY"
      lazy val dataTableName = config.getString("hive.table")
      lazy val batchId = globalBatchId.toString
    }

  val dcFileSystemCleaner
  : FileSystemEnvCleaner =
    new FileSystemEnvCleaner() {
      lazy val fs = dcFS
      lazy val hdfsPath = s"$pathToFiles/READY"
    }

  ///////////////////////////////////////////////////////////////////////////
  // Processor
  ///////////////////////////////////////////////////////////////////////////

  // scopt object for app coordination
  val processor = new Processor() {
    val log = logger
    val fsPreparator = dcFileSystemPreparator
    val fsCleaner = dcFileSystemCleaner
    val gzReader = dcArchiveReader
    val parser = mainParser
    val handler = dcMainHandler
    val hiveRecorder = dcHiveRecorder
    val hbaseRecorder = dcHBaseRecorder
  }

  ///////////////////////////////////////////////////////////////////////////
  // Other
  ///////////////////////////////////////////////////////////////////////////
}

object DriverContext extends DriverContext

