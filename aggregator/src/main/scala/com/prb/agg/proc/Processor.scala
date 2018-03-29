package com.prb.agg.proc

import org.slf4j.Logger
import com.prb.agg.chive

abstract class Processor {

  val log: Logger

  val hiveReader: chive.Reader
  val hiveWriter: chive.Writer

  def process(args: ProcessorConfig): Unit = {
    log.debug("Application started")

    println("Hello proc!")










    log.debug("Application is finished")
  }
}
