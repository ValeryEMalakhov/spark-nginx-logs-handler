package com.prb.dnhs.parsers

import scala.language.implicitConversions

import com.prb.dnhs.DriverContext
import com.prb.dnhs.entities.LogEntry
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


class DataSecondParser extends DataParser[RDD[LogEntry], RDD[Row]] {

  def parse(logRDD: RDD[LogEntry]): RDD[Row] = {

    val rtPart: RDD[Row] = logRDD
      .filter(_.eventType == "rt")
      .map(log => DataSecondParser.fieldBuilder(log))

    val imprPart: RDD[Row] = logRDD
      .filter(_.eventType == "impr")
      .map(log => DataSecondParser.fieldBuilder(log))

    val clkPart: RDD[Row] = logRDD
      .filter(_.eventType == "clk")
      .map(log => DataSecondParser.fieldBuilder(log))

    val xx = rtPart.union(imprPart).union(clkPart).sortBy(_.apply(0).toString)
    xx.foreach(println)

    val rtDF: DataFrame = DriverContext.sqlContext.createDataFrame(rtPart, DriverContext.rt.schema)
    val imprDF: DataFrame = DriverContext.sqlContext.createDataFrame(imprPart, DriverContext.impr.schema)
    val clkDF: DataFrame = DriverContext.sqlContext.createDataFrame(clkPart, DriverContext.clk.schema)

    val cols1 = rtDF.columns.toList
    val cols2 = imprDF.columns.toList
    val cols3 = clkDF.columns.toList
    val total = (cols1 ++ cols2 ++ cols3).distinct

    val logDF: Dataset[Row] =
      rtDF.select(DataSecondParser.expr(cols1, total): _*)
        .union(imprDF.select(DataSecondParser.expr(cols2, total): _*))
        .union(clkDF.select(DataSecondParser.expr(cols3, total): _*))
        .sort("dateTime").dropDuplicates

    //==================================
    logDF.show(100, truncate = false)

    // Only if we need RDD[Row], else return DataFrame
    logDF.rdd
  }
}

//  Necessity in the object since serialization occurs in the `map`
private object DataSecondParser {

  private def fieldBuilder(log: LogEntry) = {
    val immutableFields = Row(
      log.dateTime,
      log.eventType,
      log.requesrId,
      log.userCookie,
      log.site,
      log.ipAddress,
      log.useragent
    )

    if (!(log.segments sameElements Array[String]("-"))) {

      val segments: Array[Map[String, String]] = log.segments.map { seg =>
        seg.split('=').toList.grouped(2).map { case List(a, b) => a -> b }.toMap
      }

      val mutableFields: Array[Row] = segments.map { seg =>
        Row(seg.head._2)
      }

      mutableFields.foldLeft(immutableFields)((out: Row, part: Row) => Row.merge(out, part))

    } else immutableFields

  }

  private def expr(colsPart: List[String], allCols: List[String]) = {
    allCols.map {
      case x if colsPart.contains(x) => col(x)
      case x => lit(null).as(x)
    }
  }
}

