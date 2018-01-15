package com.prb.dnhs

import com.prb.dnhs.processor.ProcessorConfig
import com.typesafe.config.{Config, ConfigFactory}

object MainApp extends App {

  val parser = new scopt.OptionParser[ProcessorConfig]("scopt") {

    //    head("scopt", config.getString("scopt.version"))

    opt[String]('i', "in").action((x, c) =>
      c.copy(inputDir = x)).text("inputDir is an address to income hdfs files")

    opt[String]('m', "mode").action((x, c) =>
      c.copy(startupMode = x)).text("startupMode is a runtime arg")

    help("help").text("prints this usage text")
  }

  parser.parse(args, ProcessorConfig()) match {
    case Some(value) =>
      // do stuff
      DriverContext.processor.process(value)

    case None =>
    // arguments are bad, error message will have been displayed
  }

}

