package com.prb.agg.helpers

import com.typesafe.config.{Config, ConfigFactory}

trait ConfigHelper {

  @transient
  lazy val config: Config = ConfigFactory.load("application.conf")
}