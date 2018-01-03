package com.prb.dnhs.exceptions

sealed trait DataValidationExceptions extends RuntimeException {
  override def getMessage: String
}

case object ImmutableFieldIsEmpty extends DataValidationExceptions {
  override def getMessage: String = "Immutable fields cannot be empty."
}

case object MutableFieldIsEmpty extends DataValidationExceptions {
  override def getMessage: String = "Any value was expected."
}

case object UnexpectedEventType extends DataValidationExceptions {
  override def getMessage: String = "A non-existent event type."
}

case object IncorrectDataTypeExceptions extends DataValidationExceptions {
  override def getMessage: String = "Another type of data was expected."
}

/**
  * Exception messages for `CorrectTypeValidator`
  */

case object IncorrectDateTimeValueType extends DataValidationExceptions {
  override def getMessage: String = "The variable type in `dateTime` must be String."
}

case object IncorrectEventTypeValueType extends DataValidationExceptions {
  override def getMessage: String = "The variable type in `eventType` must be String."
}

case object IncorrectRequestIdValueType extends DataValidationExceptions {
  override def getMessage: String = "The variable type in `requestId` must be String."
}

case object IncorrectUserCookieValueType extends DataValidationExceptions {
  override def getMessage: String = "The variable type in `userCookie` must be String."
}

case object IncorrectSiteValueType extends DataValidationExceptions {
  override def getMessage: String = "The variable type in `site` must be String."
}

case object IncorrectIpAddressValueType extends DataValidationExceptions {
  override def getMessage: String = "The variable type in `ipAddress` must be String."
}

case object IncorrectUseragentValueType extends DataValidationExceptions {
  override def getMessage: String = "The variable type in `useragent` must be String."
}
