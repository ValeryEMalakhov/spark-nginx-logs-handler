package com.prb.dnhs.entities

import org.apache.spark.sql.types._
import org.specs2.mutable

class SchemaRepositoryImplTest extends mutable.Specification {

  ///////////////////////////////////////////////////////////////////////////
  // Test values
  ///////////////////////////////////////////////////////////////////////////

  lazy val schemas: Map[String, StructType] = Map(
    "rt.parquet" -> StructType(
      StructField("val_0", StringType, false) ::
      StructField("val_1", IntegerType, false) ::
      Nil
    ),
    "impr.parquet" -> StructType(
      StructField("val_1", IntegerType, false) ::
      StructField("val_2", StringType, false) ::
      Nil
    ),
    "clk.parquet" -> StructType(
      StructField("val_1", IntegerType, false) ::
      StructField("val_2", StringType, false) ::
      StructField("val_3", StringType, false) ::
      Nil
    )
  )

  lazy val genericSchema = StructType(
    StructField("val_0", StringType, false) ::
    StructField("val_1", IntegerType, false) ::
    StructField("val_2", StringType, false) ::
    StructField("val_3", StringType, false) ::
    Nil
  )

  ///////////////////////////////////////////////////////////////////////////
  // An objects of the test classes
  ///////////////////////////////////////////////////////////////////////////

  private def schemaRepository = new SchemaRepositoryImpl() {
    override private[entities] def readParquetSchema(schemaName: String) = schemas(schemaName)
  }

  ///////////////////////////////////////////////////////////////////////////
  // Test body
  ///////////////////////////////////////////////////////////////////////////

  "When an application requests" >> {
    "specific event scheme, SchemaRepository must get and return StructType with that schema" >> {
      schemaRepository
        .getSchema("clk")
        .must(beSome(schemas("clk.parquet")))
    }
    "generic event schema, SchemaRepository must build and return StructType with generic schema" >> {
      schemaRepository
        .getSchema(SchemaRepository.GENERIC_EVENT)
        .must(beSome(genericSchema))
    }
    "wrong event schema, SchemaRepository must return `None`" >> {
      schemaRepository.getSchema("err") must beNone
    }
  }
}

