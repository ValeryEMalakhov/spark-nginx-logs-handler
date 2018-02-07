package com.prb.dnhs.entities

import com.prb.dnhs.constants.TestConst
import org.specs2.mutable

class SchemaRepositoryImplTest extends mutable.Specification
  with TestConst {

  ///////////////////////////////////////////////////////////////////////////
  // An objects of the test classes
  ///////////////////////////////////////////////////////////////////////////

  val schemaRepository = new SchemaRepositoryTestImpl()

  ///////////////////////////////////////////////////////////////////////////
  // Test body
  ///////////////////////////////////////////////////////////////////////////

  "When an application requests" >> {
    // valid
    "specific event scheme, SchemaRepository must get and return it" >> {

      schemaRepository.getSchema(testLogEntry.eventType)
        .must(beSome(schemaRepository.testSchemas(testLogEntry.eventType + ".parquet")))
    }
    // invalid
    "generic event schema, SchemaRepository must build and return it" >> {

      schemaRepository.getSchema(schemaRepository.GENERIC_EVENT) must beSome(genericSchema)
    }
    // empty
    "wrong event schema, SchemaRepository must return `None`" >> {

      schemaRepository.getSchema("err") must beNone
    }
  }
}

