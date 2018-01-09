package com.prb.dnhs.validators

import cats.data._
import cats.implicits._
import scala.concurrent.ExecutionContext.Implicits.global
import com.prb.dnhs.entities.LogEntry
import com.prb.dnhs.exceptions._

sealed trait NonEmptyLogEntryValidator {

  def validate(
      dateTime: String,
      eventType: String,
      requestId: String,
      userCookie: String,
      site: String,
      ipAddress: String,
      useragent: String,
      queryString: Map[String, String]):
  Either[DataValidationExceptions, LogEntry] = {

    for {
      validatedDateTime <- validateNonEmptiness(dateTime)
      validatedEventType <- validateNonEmptiness(eventType)
      validatedRequestId <- validateNonEmptiness(requestId)
      validatedUserCookie <- validateNonEmptiness(userCookie)
      validatedSite <- validateNonEmptiness(site)
      validatedIpAddress <- validateNonEmptiness(ipAddress)
      validatedUseragent <- validateNonEmptiness(useragent)
    }
      yield LogEntry(validatedDateTime, validatedEventType, validatedRequestId, validatedUserCookie,
        validatedSite, validatedIpAddress, validatedUseragent, queryString)
  }

  /* : Either[DataValidationExceptions, String] */
  private def validateNonEmptiness(value: String) = {
    Either.cond(
      value != "-" && value != null,
      value,
      GeneralFieldIsEmpty
    )
  }
}

object NonEmptyLogEntryValidator extends NonEmptyLogEntryValidator

