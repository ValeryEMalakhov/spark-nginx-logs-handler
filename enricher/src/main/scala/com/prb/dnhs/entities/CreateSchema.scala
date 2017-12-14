package com.prb.dnhs.entities

import com.prb.dnhs.DriverContext._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

//  Debug Object for parquet-file creation
object CreateSchema {

  def create(): Unit = {
    val schema =
      StructType(
        StructField(name = "dateTime", dataType = StringType, nullable = false) ::
          StructField(name = "eventType", dataType = StringType, nullable = false) ::
          StructField(name = "requestId", dataType = StringType, nullable = false) ::
          StructField(name = "userCookie", dataType = StringType, nullable = false) ::
          StructField(name = "site", dataType = StringType, nullable = false) ::
          StructField(name = "ipAddress", dataType = StringType, nullable = false) ::
          StructField(name = "useragent", dataType = StringType, nullable = false) ::

          //  StructField(name = "AdId", dataType = IntegerType, nullable = true) ::
          //  StructField(name = "SomeId", dataType = IntegerType, nullable = true) ::
          Nil
      )

    val emptyVal = Seq(Row("", "", "", "", "", "", "" /*, null, null*/))
    val emptyDF = sqlContext.createDataFrame(sc.parallelize(emptyVal), schema)

    emptyDF.printSchema()
    emptyDF.write.option("header", "true").parquet("src/main/resources/schemas/test")
  }
}
