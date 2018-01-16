package com.prb.dnhs

import java.io.File

import cats.data.Validated
import com.prb.dnhs.entities.{SerializableContainer, _}
import com.prb.dnhs.exceptions.DataValidationExceptions
import com.prb.dnhs.helpers._
import com.prb.dnhs.parsers.DataParser
import com.prb.dnhs.processor.{Processor, ProcessorConfig}
import com.prb.dnhs.recorders.{DataRecorder, ParquetRecorder}
import org.apache.spark._
import org.apache.spark.sql._

/**
  * The DriverContext object contains a number of parameters
  * that enable to work with Spark.
  */
object DriverContext extends ConfigHelper with LoggerHelper {

  private val runStatus = "prod"
  //  private val runStatus = "debug"

  val pathToFile: String = config.getString(s"hdfs.$runStatus.node") + config.getString(s"hdfs.$runStatus.files")
  val warehouseLocation: String = new File("spark-warehouse").getAbsolutePath

  // create Spark config with default settings
  private lazy val sparkConf: SparkConf =
    if (runStatus == "debug") {
      new SparkConf()
        .setAppName(config.getString("spark.name"))
        .setMaster(config.getString(s"spark.$runStatus.master"))
    } else {
      new SparkConf()
        .setAppName(config.getString("spark.name"))
    }

  // SparkSession for Spark 2.2.0
  lazy val spark: SparkSession = SparkSession
    .builder()
    .appName(config.getString("spark.name"))
    .config(sparkConf)
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .enableHiveSupport()
    .getOrCreate()

  // app values

  val scDataParserImpl = new SerializableContainer[DataParser[String, Option[Row]]] {
    override def obj: DataParser[String, Option[Row]] = ExecutorContext.dataParserImpl
  }

  val processor = new Processor() {
    lazy val sparkSession: SparkSession = DriverContext.spark
    lazy val defInputPath: String = DriverContext.pathToFile + "READY/*.gz"
    lazy val parser = DriverContext.scDataParserImpl
    lazy val schemas: SchemaRepositorуImpl = ExecutorContext.schemasImpl
  }

  val processorParser = new scopt.OptionParser[ProcessorConfig]("scopt") {

    head("scopt", DriverContext.config.getString("scopt.version"))

    opt[String]("input")
      .abbr("in")
      .action((x, c) => c.copy(inputDir = x))
      .text("inputDir is an address to income hdfs files")

    opt[String]("mode")
      .abbr("m")
      .action((x, c) => c.copy(startupMode = x))
      .text("startupMode is a runtime arg")

    opt[Unit]("debug")
      .hidden()
      .action((x, c) => c.copy(debug = true))
      .text("activates debug mode functions")

    help("help").text("prints this usage text")
  }

  val recorder = new ParquetRecorder() {
    lazy val tableName: String = "SparkEnricher"
    lazy val warehouseLocation: String = DriverContext.warehouseLocation
    lazy val sparkSession: SparkSession = DriverContext.spark
  }


}
