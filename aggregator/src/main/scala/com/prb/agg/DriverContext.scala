package com.prb.agg

import java.io.File
import java.net.URI

import com.prb.agg.chive.{Reader, Writer}
import com.prb.agg.helpers._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.slf4j.Logger

class DriverContext extends Serializable
  with ConfigHelper
  with LoggerHelper {

  ///////////////////////////////////////////////////////////////////////////
  // Spark conf
  ///////////////////////////////////////////////////////////////////////////

  lazy val _warehouseLocation: String =
    new File("sparkSession-warehouse").getAbsolutePath

  // default path to hdfs data folder

  lazy val _pathToFiles: String =
    s"${config.getString("hdfs.node")}/${config.getString("hdfs.files")}"

  // SparkSession for Spark 1.6.* and older
  lazy val _sparkConfig: SparkConf =
    new SparkConf()
      .setAppName(config.getString("app.name"))
      .setMaster(config.getString("spark.master"))
      .set("spark.sql.warehouse.dir", _warehouseLocation)
      .set("hive.metastore.uris", config.getString("hive.address"))
      .set("spark.sql.hive.convertMetastoreParquet", "true")

  lazy val _sparkContext: SparkContext =
    new SparkContext(_sparkConfig)

  lazy val _hiveContext: HiveContext =
    new HiveContext(_sparkContext)

  lazy val _sqlContext: SQLContext =
    new SQLContext(_sparkContext)

  lazy val _fs: FileSystem =
    FileSystem.get(
      new URI(config.getString("hdfs.node")),
      new Configuration(),
      config.getString("hdfs.user")
    )

  ///////////////////////////////////////////////////////////////////////////
  // Hive
  ///////////////////////////////////////////////////////////////////////////

  val _hiveReader: chive.Reader =
    new chive.Reader()

  val _hiveWriter: chive.Writer =
    new chive.Writer(){
      lazy val sqlContext = _hiveContext
    }

  ///////////////////////////////////////////////////////////////////////////
  // Processor
  ///////////////////////////////////////////////////////////////////////////

  val processor = new proc.Processor {
    lazy val log: Logger = logger
    lazy val hiveReader: Reader = _hiveReader
    lazy val hiveWriter: Writer = _hiveWriter
  }
}

object DriverContext extends DriverContext