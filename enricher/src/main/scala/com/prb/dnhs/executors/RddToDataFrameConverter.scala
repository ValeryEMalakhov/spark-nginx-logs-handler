package com.prb.dnhs.executors

import com.prb.dnhs.entities._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

class RddToDataFrameConverter extends RddConverter[DataFrame] {

  override def convert(logRDD: RDD[ParsedPixel]): DataFrame = {

    import com.prb.dnhs.DriverContext.sqlContext.implicits._

    logRDD.toDF("Date_time", "Event_type", "Request_id", "User_cookie",
                "Site", "User_ip", "User_agent", "Segments")
  }
}
