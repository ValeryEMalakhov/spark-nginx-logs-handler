package com.prb.dnhs.helpers

import org.slf4j.{Logger, LoggerFactory}

trait LoggerHelper { self =>

  @transient
  lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)
}
