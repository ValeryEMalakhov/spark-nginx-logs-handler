package com.prb.dnhs.recorders

import java.time.Instant

import com.prb.dnhs.DriverContext
import org.apache.spark.sql.{DataFrame, Row}

class ParquetRecorder extends DataRecorder[DataFrame] {

  def save(data: DataFrame): Unit = {

    val batchId = Instant.now.toEpochMilli
    val destPath = buildPath(DriverContext.recorder.warehouseLocation, DriverContext.recorder.tableName, batchId)

    if (!DriverContext.recorder
      .sparkSession
      .sqlContext
      .tableNames
      .contains(DriverContext.recorder.tableName)) {

      val fields = data.schema.fields
        .map { f =>
          if (f.dataType.typeName == "array")
            s"${f.name} ${f.dataType.typeName}<string>"
          else
            s"${f.name} ${f.dataType.typeName}"
        }.mkString(", ")

      DriverContext.recorder
        .sparkSession
        .sql(s"CREATE TABLE IF NOT EXISTS ${DriverContext.recorder.tableName}($fields) PARTITIONED BY (batch_Id string)")
    }

    // write
    data.write.parquet(destPath)

    // update metastore info
    DriverContext.recorder
      .sparkSession
      .sql(s"ALTER TABLE ${DriverContext.recorder.tableName} ADD IF NOT EXISTS PARTITION(batch_Id=$batchId)")
  }

  private def buildPath(warehouseLocation: String, tableName: String, batchId: Long): String = {
    s"$warehouseLocation/$tableName/batch_id=$batchId"
  }
}
