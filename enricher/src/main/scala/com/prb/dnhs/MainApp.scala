package com.prb.dnhs

import com.prb.dnhs.processor.ProcessorConfig

object MainApp extends App {

  DriverContext.processorParser.parse(args, ProcessorConfig()) match {
    case Some(value) =>
      // do stuff
      DriverContext.processor.process(value)

    case None =>
    // arguments are bad, error message will have been displayed
  }
}

