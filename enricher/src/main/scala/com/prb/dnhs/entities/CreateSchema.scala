package com.prb.dnhs.entities

import com.prb.dnhs.DriverContext
import com.prb.dnhs.DriverContext._
import com.prb.dnhs.DriverContext.sqlContext._
import com.prb.dnhs.DriverContext.sqlContext.implicits._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object CreateSchema {

  def create(): Unit = {
    val schema =
      StructType(
        StructField(name = "dateTime", dataType = StringType, nullable = false) ::
          StructField(name = "eventType", dataType = StringType, nullable = false) ::
          StructField(name = "requesrId", dataType = StringType, nullable = false) ::
          StructField(name = "userCookie", dataType = StringType, nullable = false) ::
          StructField(name = "site", dataType = StringType, nullable = false) ::
          StructField(name = "ipAddress", dataType = StringType, nullable = false) ::
          StructField(name = "useragent", dataType = StringType, nullable = false) ::

          /*StructField(name = "AdId", dataType = IntegerType, nullable = true) ::*/
          /*StructField(name = "SomeId", dataType = IntegerType, nullable = true) ::*/ Nil
      )

    val emptyVal = Seq(Row("", "", "", "", "", "", ""/*, null, null*/))

    val emptyDF = sqlContext.createDataFrame(sc.parallelize(emptyVal), schema)
    //    val emptyDF = sqlContext.createDataFrame(sc.parallelize(Seq(Row())), schema)

    emptyDF.show()
    emptyDF.printSchema()

    emptyDF.write.option("header", "true").parquet("src/main/resources/schemas/test")

    //    clk.printSchema()
  }
}
