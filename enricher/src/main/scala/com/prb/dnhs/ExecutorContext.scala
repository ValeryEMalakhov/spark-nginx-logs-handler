package com.prb.dnhs

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import com.prb.dnhs.executors._
import com.prb.dnhs.entities.DefaultParsedPixel

/**
  * The ExecutorContext object contains a number of parameters
  * that enable to work with application classes.
  */
object ExecutorContext {

  val converterToDataFrame: RddConverter[DataFrame] = new RddToDataFrameConverter()

  val packagerAsTextFile: FilePackager[RDD[DefaultParsedPixel]] = new FilePackagerUsingTextFile()

  val packagerAsCSV: FilePackager[DataFrame] = new FilePackagerUsingCSV()

  val parser: RddParser = new RddFirstParser()
}

