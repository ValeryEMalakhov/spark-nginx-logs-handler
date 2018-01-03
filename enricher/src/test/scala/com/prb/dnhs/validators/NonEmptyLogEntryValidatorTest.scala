package com.prb.dnhs.validators

import com.prb.dnhs.entities.LogEntry
import com.prb.dnhs._
import com.prb.dnhs.exceptions._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.specs2._

class NonEmptyLogEntryValidatorTest extends mutable.Specification {

  private val testLogEntry = LogEntry("01/Jan/2000:00:00:01", "clk", "01234567890123456789012345678901", "001", "127.0.0.1", "127.0.0.1", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)", Map("AdId" -> "100", "SomeId" -> "012345"))

  "Validator must return exception if" >> {
    "log string got empty value" >> {
      "in dateTime" >> {
        NonEmptyLogEntryValidator
          .validate("-", testLogEntry.eventType, testLogEntry.requestId, testLogEntry.userCookie,
            testLogEntry.site, testLogEntry.ipAddress, testLogEntry.useragent, testLogEntry.queryString)
          .left.get must_== ImmutableFieldIsEmpty
      }
      "in eventType" >> {
        NonEmptyLogEntryValidator
          .validate(testLogEntry.dateTime, "-", testLogEntry.requestId, testLogEntry.userCookie,
            testLogEntry.site, testLogEntry.ipAddress, testLogEntry.useragent, testLogEntry.queryString)
          .left.get must_== ImmutableFieldIsEmpty
      }
      "in requestId" >> {
        NonEmptyLogEntryValidator
          .validate(testLogEntry.dateTime, testLogEntry.eventType, "-", testLogEntry.userCookie,
            testLogEntry.site, testLogEntry.ipAddress, testLogEntry.useragent, testLogEntry.queryString)
          .left.get must_== ImmutableFieldIsEmpty
      }
      "in userCookie" >> {
        NonEmptyLogEntryValidator
          .validate(testLogEntry.dateTime, testLogEntry.eventType, testLogEntry.requestId, "-",
            testLogEntry.site, testLogEntry.ipAddress, testLogEntry.useragent, testLogEntry.queryString)
          .left.get must_== ImmutableFieldIsEmpty
      }
      "in site" >> {
        NonEmptyLogEntryValidator
          .validate(testLogEntry.dateTime, testLogEntry.eventType, testLogEntry.requestId, testLogEntry.userCookie,
            "-", testLogEntry.ipAddress, testLogEntry.useragent, testLogEntry.queryString)
          .left.get must_== ImmutableFieldIsEmpty
      }
      "in ipAddress" >> {
        NonEmptyLogEntryValidator
          .validate(testLogEntry.dateTime, testLogEntry.eventType, testLogEntry.requestId, testLogEntry.userCookie,
            testLogEntry.site, "-", testLogEntry.useragent, testLogEntry.queryString)
          .left.get must_== ImmutableFieldIsEmpty
      }
      "in useragent" >> {
        NonEmptyLogEntryValidator
          .validate(testLogEntry.dateTime, testLogEntry.eventType, testLogEntry.requestId, testLogEntry.userCookie,
            testLogEntry.site, testLogEntry.ipAddress, "-", testLogEntry.queryString)
          .left.get must_== ImmutableFieldIsEmpty
      }
    }
  }
}
