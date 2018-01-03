package com.prb.dnhs.validators

import cats.implicits._
import com.prb.dnhs.entities.LogEntry
import com.prb.dnhs.exceptions._

sealed trait CorrectTypesValidator {

  def validate(logEntry: LogEntry): Either[DataValidationExceptions, LogEntry] = {
    for {
      validatedDateTime <- validateDateTime(logEntry.dateTime)
      validatedEventType <- validateEventType(logEntry.eventType)
      validatedRequestId <- validateRequestId(logEntry.requestId)
      validatedUserCookie <- validateUserCookie(logEntry.userCookie)
      validatedSite <- validateSite(logEntry.site)
      validatedIpAddress <- validateIpAddress(logEntry.ipAddress)
      validatedUseragent <- validateUseragent(logEntry.useragent)
    }
      yield LogEntry(validatedDateTime, validatedEventType, validatedRequestId, validatedUserCookie,
        validatedSite, validatedIpAddress, validatedUseragent, logEntry.queryString)
  }

  private def validateDateTime(dateTime: String) = {
    Either.cond(
      try {
        dateTime.toString
        true
      } catch {
        case _ => false
      },
      dateTime,
      IncorrectDateTimeValueType
    )
  }

  private def validateEventType(eventType: String) = {
    Either.cond(
      try {
        eventType.toString
        true
      } catch {
        case _ => false
      },
      eventType,
      IncorrectEventTypeValueType
    )
  }

  private def validateRequestId(requestId: String) = {
    Either.cond(
      try {
        requestId.toString
        true
      } catch {
        case _ => false
      },
      requestId,
      IncorrectRequestIdValueType
    )
  }

  private def validateUserCookie(userCookie: String) = {
    Either.cond(
      try {
        userCookie.toString
        true
      } catch {
        case _ => false
      },
      userCookie,
      IncorrectUserCookieValueType
    )
  }

  private def validateSite(site: String) = {
    Either.cond(
      try {
        site.toString
        true
      } catch {
        case _ => false
      },
      site,
      IncorrectSiteValueType
    )
  }

  private def validateIpAddress(ipAddress: String) = {
    Either.cond(
      try {
        ipAddress.toString
        true
      } catch {
        case _ => false
      },
      ipAddress,
      IncorrectIpAddressValueType
    )
  }

  private def validateUseragent(useragent: String) = {
    Either.cond(
      try {
        useragent.toString
        true
      } catch {
        case _ => false
      },
      useragent,
      IncorrectUseragentValueType
    )
  }
}

object CorrectTypesValidator extends CorrectTypesValidator

