package com.prb.dnhs

import com.prb.dnhs.entities._
import com.prb.dnhs.helpers.LoggerHelper
import com.prb.dnhs.parsers._

/**
  * The ExecutorContext object contains a number of parameters
  * that enable to work with application classes.
  */
object ExecutorContext extends LoggerHelper {

  val dataParser: DataParserImpl.type = DataParserImpl

  val schemas: SchemaRepositorуImpl = new SchemaRepositorуImpl()
}

