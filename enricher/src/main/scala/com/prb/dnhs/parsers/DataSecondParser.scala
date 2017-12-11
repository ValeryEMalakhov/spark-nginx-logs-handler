package com.prb.dnhs.parsers

import scala.collection.immutable
import scala.language.implicitConversions

import com.prb.dnhs.DriverContext._
import com.prb.dnhs.entities.LogEntry
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._


class DataSecondParser extends DataParser[RDD[LogEntry], RDD[Row]] {

  def parse(logRDD: RDD[LogEntry]): RDD[Row] = {

    //TODO: add logs Rows validation with parquet schemas

    val mergedList: DataFrame =
      rt.join(right = impr, usingColumns = rt.columns)
        .join(right = clk, usingColumns = (rt.columns ++ impr.columns).distinct)

    logRDD.map(log => DataSecondParser.fieldBuilder(log))

  }
}

//  Necessity in the object since serialization occurs in the `map`
private object DataSecondParser {

  private def fieldBuilder(log: LogEntry): Row = {
    val immutableFields = Row(
      log.dateTime,
      log.eventType,
      log.requesrId,
      log.userCookie,
      log.site,
      log.ipAddress,
      log.useragent
    )

    val mergedList: DataFrame =
      rt.join(right = impr, usingColumns = rt.columns)
        .join(right = clk, usingColumns = (rt.columns ++ impr.columns).distinct)

    val checkList: Array[String] = mergedList.columns.drop(immutableFields.length)

    val segmentList = log.segments.toList

    val mutableFieldsTest: Array[Row] = checkList.zipWithIndex.map { case (list, i) =>

      if (segmentList.lengthCompare(i) != 0) {
        if (segmentList(i)._1 == list) {
          Row(segmentList(i)._2)
        } else Row(null)
      } else Row(null)
    }

    val mutableFields: immutable.Iterable[Row] = log.segments.map { seg =>
      Row(seg._2)
    }

    mutableFieldsTest.foldLeft(immutableFields)((head: Row, tail: Row) => Row.merge(head, tail))

  }
}

