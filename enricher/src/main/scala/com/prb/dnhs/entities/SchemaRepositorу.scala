package com.prb.dnhs.entities

import org.apache.spark.sql.types.StructType

trait SchemaRepositorу {

  def getSchema(schemaName: String): Option[StructType]
}

