package com.prb.dnhs.validators

import com.prb.dnhs.exceptions.ErrorDetails
import com.prb.dnhs.exceptions.ErrorType.ParserError

class NonEmptinessValidator extends Validator[String, Either[ErrorDetails, String]] {

  override def validate(inputLog: String) = {
    Either.cond(
      inputLog != "" && inputLog.nonEmpty,
      inputLog,
      ErrorDetails(
        errorType = ParserError,
        errorMessage = s"Log-string is empty!",
        line = inputLog
      )
    )
  }
}
