package com.prb.dnhs.exceptions

case class SchemaValidationException(message: String = "Schema validation failed") extends RuntimeException(message)

