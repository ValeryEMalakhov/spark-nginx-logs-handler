package com.prb.dnhs.executors

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.rdd.RDD
import com.prb.dnhs.DriverContext
import com.prb.dnhs.entities.ParsedPixel

class FilePackagerUsingTextFile extends FilePackager[RDD[ParsedPixel]] {

  override def save(logData: RDD[ParsedPixel]): Unit = {

    //  filter logs by events
    val rtLogRDD = logData.filter(rdd => rdd.eventType == "rt")
    val imprLogRDD = logData.filter(rdd => rdd.eventType == "impr")
    val clkLogRDD = logData.filter(rdd => rdd.eventType == "clk")

    //  get string with time in format like "1970_12_30__23_59_59"
    val destinationFolderName = new SimpleDateFormat("yyyy_MM_dd__HH_mm_ss").format(Calendar.getInstance().getTime)

    rtLogRDD.saveAsTextFile(DriverContext.pathToFile + s"DONE/textFile/" + destinationFolderName + "/rt/")
    imprLogRDD.saveAsTextFile(DriverContext.pathToFile + s"DONE/textFile/" + destinationFolderName + "/impr/")
    clkLogRDD.saveAsTextFile(DriverContext.pathToFile + s"DONE/textFile/" + destinationFolderName + "/clk/")
  }
}
