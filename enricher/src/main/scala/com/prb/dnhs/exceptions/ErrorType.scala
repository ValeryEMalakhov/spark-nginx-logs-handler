package com.prb.dnhs.exceptions

sealed trait ErrorType

object ErrorType {

  case object InvalidSchemaError extends ErrorType

  case object ParserError extends ErrorType

  case object OtherError extends ErrorType

}
