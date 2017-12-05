package com.prb.dnhs.executors

import scala.language.implicitConversions

import com.prb.dnhs.entities._
import org.apache.spark.rdd.RDD

class RddFirstParser extends RddParser {

  override def parse(logRDD: RDD[String]): RDD[ParsedPixel] = {

    // breaks the input RDD into strings
    logRDD.map { str =>

      // breaks the input string into tabs
      val parsedLogString = str.split('\t')

      ParsedPixel(parsedLogString(0).dropRight(6), parsedLogString(1), parsedLogString(2), parsedLogString(3),
        parsedLogString(4), parsedLogString(5), parsedLogString(6), parsedLogString(7))
    }
  }
}
