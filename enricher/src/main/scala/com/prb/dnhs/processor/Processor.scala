package com.prb.dnhs.processor

import com.prb.dnhs.{DriverContext, ExecutorContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

class Processor {

  def process(args: ProcessorConfig): Unit = {

    val logRDD: RDD[String] = DriverContext.processor.sparkSession
      .sparkContext
      .textFile {
        if (args.inputDir == "") DriverContext.processor.defInputPath
        else args.inputDir
      }

    if (args.debug)
      logRDD.collect.foreach(println)

    val logRow: RDD[Row] = logRDD.flatMap(DriverContext.processor.parser.value.parse)

    // obtain a combined dataframe from the created rdd and the merged scheme
    val logDF = DriverContext.processor.sparkSession
      .createDataFrame(logRow, DriverContext.processor.schemas.getSchema("generic-event").get)

    if (args.debug)
      logDF.sort("dateTime").show(100, truncate = false)

    //DriverContext.recorder.save(logDF)
  }

}