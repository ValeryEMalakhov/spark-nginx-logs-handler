package com.prb.dnhs

import com.typesafe.config._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import com.prb.dnhs.recorders.ParquetRecorder._

case class Processor(input: String = "")

object Processor {

  def run(args: Array[String]): Unit = {

    val config: Config = ConfigFactory.load("application.conf")

    val parser = new scopt.OptionParser[Processor]("scopt") {

      head("scopt", config.getString("scopt.version"))

      opt[String]('i', "input").action((x, c) =>
        c.copy(input = x)).text("input is an address to hdfs files")


      help("help").text("prints this usage text")
    }

    // parser.parse returns Option[C]
    parser.parse(args, Processor()) match {
      case Some(proc) =>
        // do stuff
        proc.input match {
          case "" =>
          case "def" => {
            val logRDD: RDD[String] = DriverContext.spark.sparkContext
              .textFile(DriverContext.pathToFile + "READY/*.gz")

            val logRow: RDD[Row] = DriverContext.dataParser.parse(logRDD)

            // obtain a combined dataframe from the created rdd and the merged scheme
            val logDF = DriverContext.spark
              .createDataFrame(logRow, ExecutorContext.schemas.getSchema("generic-event").get)

            logDF.save()
          }
          case "debug" => {

            val logRDD: RDD[String] = DriverContext.spark.sparkContext
              .textFile(DriverContext.pathToFile + "READY/*.gz")

            // logRDD.collect.foreach(println)

            val logRow: RDD[Row] = DriverContext.dataParser.parse(logRDD)

            // obtain a combined dataframe from the created rdd and the merged scheme
            val logDF = DriverContext.spark
              .createDataFrame(logRow, ExecutorContext.schemas.getSchema("generic-event").get)

            logDF.sort("dateTime").show(100, truncate = true)
          }
        }
      case None =>
      // arguments are bad, error message will have been displayed
    }
  }

}
