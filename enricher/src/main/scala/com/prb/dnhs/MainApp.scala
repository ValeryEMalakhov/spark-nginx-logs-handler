package com.prb.dnhs

import com.prb.dnhs.processor.ProcessorConfig

object MainApp extends App {

  // Const block
  val config = DriverContext.config
  val log = DriverContext.logger
  val proc = DriverContext.processor

  val appName = config.getString("app.name")
  val appVersion = config.getString("app.version")

  val processorParser = buildParser().parse(args, ProcessorConfig()) match {
    case Some(value) =>
      // do stuff
      proc.process(value)
    case None =>
      // arguments are bad, error message will have been displayed
      log.error("Arguments are bad")
  }

  // command line arguments parser
  private def buildParser() =
    new scopt.OptionParser[ProcessorConfig](appName) {

      head(appName, appVersion)

      opt[String]("input")
        .abbr("in")
        .action((x, c) => c.copy(inputDir = x))
        .text("inputDir is an address to income hdfs files")

      opt[String]("output")
        .abbr("out")
        .action((x, c) => c.copy(outputDir = x))
        .text("outputDir is an address to save files with reparse data")

      opt[String]("mode")
        .abbr("m")
        .action((x, c) => c.copy(startupMode = x))
        .text("startupMode is a runtime arg")

      opt[Unit]("debug")
        .hidden()
        .action((x, c) => c.copy(debug = true))
        .text("activates debug mode functions")

      help("help")
        .text("prints this usage text")
    }
}

