package com.prb.dnhs.entities

import org.apache.spark.sql.types._

class SchemaRepositoryImplTest extends SchemaRepositoryImpl {

  lazy val testSchemas: Map[String, StructType] = Map(
    "rt.parquet" -> StructType(
      StructField("dateTime", StringType, false) ::
        StructField("eventType", StringType, false) ::
        StructField("requesrId", StringType, false) ::
        StructField("userCookie", StringType, false) ::
        StructField("site", StringType, false) ::
        StructField("ipAddress", StringType, false) ::
        StructField("useragent", StringType, false) ::
        StructField("segments", ArrayType(StringType, false), false) :: Nil
    ),
    "impr.parquet" -> StructType(
      StructField("dateTime", StringType, false) ::
        StructField("eventType", StringType, false) ::
        StructField("requesrId", StringType, false) ::
        StructField("userCookie", StringType, false) ::
        StructField("site", StringType, false) ::
        StructField("ipAddress", StringType, false) ::
        StructField("useragent", StringType, false) ::
        StructField("AdId", IntegerType, true) :: Nil
    ),
    "clk.parquet" -> StructType(
      StructField("dateTime", StringType, false) ::
        StructField("eventType", StringType, false) ::
        StructField("requesrId", StringType, false) ::
        StructField("userCookie", StringType, false) ::
        StructField("site", StringType, false) ::
        StructField("ipAddress", StringType, false) ::
        StructField("useragent", StringType, false) ::
        StructField("AdId", IntegerType, true) ::
        StructField("SomeId", StringType, true) :: Nil
    )
  )

  override private[entities] def readParquetSchema(schemaName: String) = {

    testSchemas(schemaName)
  }
}
