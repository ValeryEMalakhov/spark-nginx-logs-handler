package com.prb.dnhs.entities

import org.apache.spark.sql.types._
import org.specs2.mutable

class SchemaRepositoryTest extends mutable.Specification {

  ///////////////////////////////////////////////////////////////////////////
  // Test values
  ///////////////////////////////////////////////////////////////////////////

  val testLogEntry =
    LogEntry("01/Jan/2000:00:00:01",
      "clk",
      "01234567890123456789012345678901",
      "001",
      "127.0.0.1",
      "127.0.0.1",
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
      Map("AdId" -> "100", "SomeId" -> "012345")
    )

  val genericSchema = StructType(
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

  ///////////////////////////////////////////////////////////////////////////
  // An objects of the test classes
  ///////////////////////////////////////////////////////////////////////////

  val schemaRepository = new SchemaRepositoryImplTest()

  ///////////////////////////////////////////////////////////////////////////
  // Test body
  ///////////////////////////////////////////////////////////////////////////

  "When an application requests" >> {

    "specific event scheme, SchemaRepository must get and return it" >> {

      schemaRepository.getSchema(testLogEntry.eventType)
        .must(beSome(schemaRepository.testSchemas(testLogEntry.eventType + ".parquet")))
    }

    "generic event schema, SchemaRepository must build and return it" >> {

      schemaRepository.getSchema(schemaRepository.GENERIC_EVENT) must beSome(genericSchema)
    }

    "wrong event schema, SchemaRepository must return `None`" >> {

      schemaRepository.getSchema("err") must beNone
    }
  }
}

