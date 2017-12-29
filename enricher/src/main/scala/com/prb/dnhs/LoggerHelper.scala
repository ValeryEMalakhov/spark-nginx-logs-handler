package com.prb.dnhs

import org.slf4j.{Logger, LoggerFactory}

trait LoggerHelper { self =>

  @transient
//  lazy val logger = LoggerFactory.getLogger(classOf[self.type])
  lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)
}
