package com.prb.dnhs.entities

import org.apache.spark.sql.types._
import org.specs2.mutable

class SchemaRepositoryImplTest extends mutable.Specification {

  ///////////////////////////////////////////////////////////////////////////
  // Test values
  ///////////////////////////////////////////////////////////////////////////

  private val testLogEntry =
    LogEntry("01/Jan/2000:00:00:01", "clk", "01234567890123456789012345678901", "001",
      "127.0.0.1", "127.0.0.1", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
      Map("AdId" -> "100", "SomeId" -> "012345")
    )

  ///////////////////////////////////////////////////////////////////////////
  // An objects of the test classes
  ///////////////////////////////////////////////////////////////////////////

  private def schemaRepository = new SchemaRepositoryTestImpl()

  ///////////////////////////////////////////////////////////////////////////
  // Test body
  ///////////////////////////////////////////////////////////////////////////

  "When an application requests" >> {
    "specific event scheme, SchemaRepository must get and return StructType with that schema" >> {
      schemaRepository.getSchema(testLogEntry.eventType)
        .must(beSome(schemaRepository.testSchemas(testLogEntry.eventType + ".parquet")))
    }
    "generic event schema, SchemaRepository must build and return StructType with generic schema" >> {
      schemaRepository.getSchema(schemaRepository.GENERIC_EVENT)
        .must(beSome(schemaRepository.genericSchema))
    }
    "wrong event schema, SchemaRepository must return `None`" >> {
      schemaRepository.getSchema("err") must beNone
    }
  }
}

class SchemaRepositoryTestImpl extends SchemaRepositoryImpl {

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

  lazy val genericSchema = StructType(
    StructField("dateTime", StringType, false) ::
      StructField("eventType", StringType, false) ::
      StructField("requesrId", StringType, false) ::
      StructField("userCookie", StringType, false) ::
      StructField("site", StringType, false) ::
      StructField("ipAddress", StringType, false) ::
      StructField("useragent", StringType, false) ::
      StructField("segments", ArrayType(StringType, false), false) ::
      StructField("AdId", IntegerType, true) ::
      StructField("SomeId", StringType, true) :: Nil
  )

  override private[entities] def readParquetSchema(schemaName: String) = testSchemas(schemaName)

}