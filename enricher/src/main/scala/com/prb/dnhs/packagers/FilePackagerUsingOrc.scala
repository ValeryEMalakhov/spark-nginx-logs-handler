package com.prb.dnhs.packagers

import java.text.SimpleDateFormat
import java.util.Calendar

import com.prb.dnhs.DriverContext
import org.apache.spark.sql.DataFrame

class FilePackagerUsingOrc extends FilePackager[DataFrame] {

  override def save(logData: DataFrame): Unit = {

    //  filter logs by events
    val rtLogRDD = logData.filter("Event_type like 'rt'")
    val imprLogRDD = logData.filter("Event_type like 'impr'")
    val clkLogRDD = logData.filter("Event_type like 'clk'")

    //  get string with time in format like "1970_12_30__23_59_59"
    val destinationFolderName = new SimpleDateFormat("yyyy_MM_dd__HH_mm_ss").format(Calendar.getInstance().getTime)

    rtLogRDD.write
      .format("orc")
      //.option("header", "true")
      .save(DriverContext.pathToFile + s"DONE/orc/" + destinationFolderName + "/rt/")

    imprLogRDD.write
      .format("orc")
      //.option("header", "true")
      .save(DriverContext.pathToFile + s"DONE/orc/" + destinationFolderName + "/impr/")

    clkLogRDD.write
      .format("orc")
      //.option("header", "true")
      .save(DriverContext.pathToFile + s"DONE/orc/" + destinationFolderName + "/clk/")
  }
}