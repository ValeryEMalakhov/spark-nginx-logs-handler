package com.prb.agg

import java.io.File
import java.net.URI
import java.time.Instant

import com.prb.agg.chive.{AdsHiveWriter, CHiveReader, LogHiveWriter}
import com.prb.agg.file.SparkFileReader
import com.prb.agg.helpers.{ConfigHelper, LoggerHelper}
import com.prb.agg.prep.{Advertiser, Enricher}
import com.prb.agg.proc.Processor
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.slf4j.Logger

class DriverContext extends Serializable
  with ConfigHelper
  with LoggerHelper {

  ///////////////////////////////////////////////////////////////////////////
  // Spark conf
  ///////////////////////////////////////////////////////////////////////////

  val _warehouseLocation: String =
    new File("warehouse").getAbsolutePath

  lazy val _pathToFiles: String =
    s"${config.getString("hdfs.node")}/${config.getString("hdfs.files")}"

  // SparkSession for Spark 1.6.* and older
  lazy val _sparkConfig: SparkConf =
    new SparkConf()
      .setAppName(config.getString("spark.name"))
      .setMaster(config.getString("spark.master"))
      .set("spark.sql.warehouse.dir", _warehouseLocation)
      //.set("hive.metastore.uris", config.getString("hive.address"))
      //.set("spark.sql.hive.convertMetastoreParquet", "true")

  lazy val _sparkContext: SparkContext =
    new SparkContext(_sparkConfig)

  lazy val _hiveContext: HiveContext =
    new HiveContext(_sparkContext)

  lazy val _sqlContext: SQLContext =
    new SQLContext(_sparkContext)

  lazy val _fs: FileSystem =
  /*
      FileSystem.get(
        new URI(config.getString("hdfs.node")),
        new Configuration(),
        config.getString("hdfs.user")
      )
  */
  FileSystem.get(
    new URI(FileSystem.DEFAULT_FS),
    new Configuration(),
    FileSystem.FS_DEFAULT_NAME_KEY
  )

  lazy val _batchId = Instant.now.toEpochMilli

  ///////////////////////////////////////////////////////////////////////////
  // Custom Hive
  ///////////////////////////////////////////////////////////////////////////

  val _hiveReader: CHiveReader =
    new CHiveReader() {
      lazy val sqlContext = _hiveContext
    }

  val _adsHiveWriter: AdsHiveWriter =
    new AdsHiveWriter() {
      lazy val sqlContext = _hiveContext
      lazy val warehouse = _warehouseLocation
      lazy val tableName = "ads"
      lazy val log = logger
    }

  val _logHiveWriter: LogHiveWriter =
    new LogHiveWriter() {
      lazy val sqlContext = _hiveContext
      lazy val warehouse = _warehouseLocation
      lazy val tableName = "processed_data"
      lazy val batchId = _batchId.toString
      lazy val log = logger
    }

  ///////////////////////////////////////////////////////////////////////////
  // File handlers
  ///////////////////////////////////////////////////////////////////////////

  val _fileReader: SparkFileReader =
    new SparkFileReader() {
      lazy val sparkContext = _sparkContext
    }

  ///////////////////////////////////////////////////////////////////////////
  // Data handlers
  ///////////////////////////////////////////////////////////////////////////

  val _advertiser = new Advertiser()
  val _enricher = new Enricher(){
    lazy val sqlContext = _hiveContext
  }

  ///////////////////////////////////////////////////////////////////////////
  // Processor
  ///////////////////////////////////////////////////////////////////////////

  val processor = new Processor {
    lazy val log = logger
    lazy val hiveReader = _hiveReader
    lazy val adsHiveWriter = _adsHiveWriter
    lazy val logHiveWriter = _logHiveWriter
    lazy val fileReader = _fileReader
    lazy val adsAdvertiser = _advertiser
    lazy val logsEnricher = _enricher
  }
}

object DriverContext extends DriverContext