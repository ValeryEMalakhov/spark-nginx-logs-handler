package com.prb.dnhs.validators

import com.prb.dnhs.exceptions.ErrorDetails
import com.prb.dnhs.exceptions.ErrorType.ParserError

class NonEmptinessValidator extends Validator[String, Either[ErrorDetails, String]] {

  override def validate(row: String) = {
    Either.cond(
      row != "" && row.nonEmpty,
      row,
      ErrorDetails(
        errorType = ParserError,
        errorMessage = s"Log-string is empty!",
        line = row
      )
    )
  }
}
