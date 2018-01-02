package com.prb.dnhs.helpers

import com.typesafe.config.{Config, ConfigFactory}

trait ConfigHelper {

  @transient
  lazy val config: Config = ConfigFactory.load("application.conf")
}
