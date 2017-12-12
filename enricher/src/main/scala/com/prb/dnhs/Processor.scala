package com.prb.dnhs

import com.typesafe.config._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import com.prb.dnhs.entities._
import com.prb.dnhs.exceptions.DataException

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
        if (proc.input != "") {
          val logRDD: RDD[String] = DriverContext.sc
            .textFile(proc.input)

          val logEntryRDD: RDD[LogEntry] = ExecutorContext.rddParser.parse(logRDD)

          val parsedRDD: RDD[Row] = ExecutorContext.logEntryParser.parse(logEntryRDD)

          //  ExecutorContext.packagerAsTextFile.save(parsed_logRDD)
          //  ExecutorContext.packagerAsCSV.save(logDataFrame)

        } else {
          val logRDD: RDD[String] = DriverContext.sc
            .textFile(DriverContext.pathToFile + "READY/*")

          val logEntryRDD: RDD[LogEntry] = ExecutorContext.rddParser.parse(logRDD)

          val parsedRDD: RDD[Row] = ExecutorContext.logEntryParser.parse(logEntryRDD)

          //  obtain a combined dataframe from the created rdd and the merged scheme
          val logDF = DriverContext.sqlContext.createDataFrame(parsedRDD, DriverContext.mergedSchema.schema)

          logDF.show()
        }
      case None =>
      // arguments are bad, error message will have been displayed
    }
  }
}
