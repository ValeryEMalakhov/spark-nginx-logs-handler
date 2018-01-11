package com.prb.dnhs.recorders

import java.text.SimpleDateFormat
import java.util.Calendar

import com.prb.dnhs.DriverContext
import org.apache.spark.sql.DataFrame

object ParquetRecorder {

  implicit class ParquetRecorder(inputDF: DataFrame) {

    def save(): Unit = {

      val destinationFolderName = new SimpleDateFormat("yyyy_MM_dd__HH_mm_ss").format(Calendar.getInstance().getTime)

      /*
            inputDF.write
              .format("parquet")
              .save(DriverContext.pathToFile + s"DONE/parquet/" + destinationFolderName)
      */
//      inputDF.write.saveAsTable("")
    }
  }
}
