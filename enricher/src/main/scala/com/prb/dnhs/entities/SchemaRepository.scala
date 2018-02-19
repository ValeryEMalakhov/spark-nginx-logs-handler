package com.prb.dnhs.entities

import org.apache.spark.sql.types.StructType

trait SchemaRepository {

  def getSchema(schemaName: String): Option[StructType]
}

object SchemaRepository {

  val GENERIC_EVENT = "generic-event"

  val DEFAULT_SCHEMAS = Map(
    "rt" -> "rt.parquet",
    "impr" -> "impr.parquet",
    "clk" -> "clk.parquet"
  )
}