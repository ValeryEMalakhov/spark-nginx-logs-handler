package com.prb.dnhs.entities

import org.apache.spark.sql.types.StructType

trait SchemaRepositorу {

  val GENERIC_EVENT: String

  def getSchema(schemaName: String): Option[StructType]
}

