package com.prb.dnhs.entities

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD

case class ParsedPixel(
    dateTime: String,
    eventType: String,
    requesrId: String,
    userCookie: String,
    site: String,
    ipAddress: String,
    useragent: String,
    segments: String)

/**
  * The ParsedPixel object contains a number of implicit conversions (currently only one)
  * for use with various RDDs.
  */
object ParsedPixel {

  /**
    * An implicit conversion that converts a RDD with String to a RDD with ParsedPixel objects.
    */
  implicit def parsePixel(logRDD: RDD[String]): RDD[ParsedPixel] = {

    // breaks the input RDD into strings
    logRDD.flatMap(_.split('\n')).map { str =>

      // breaks the input string into tabs
      val parsedLogString = str.split('\t')

      ParsedPixel(parsedLogString(0).dropRight(6), parsedLogString(1), parsedLogString(2), parsedLogString(3), parsedLogString(4),
        parsedLogString(5), parsedLogString(6), parsedLogString(7))
    }
  }
}

