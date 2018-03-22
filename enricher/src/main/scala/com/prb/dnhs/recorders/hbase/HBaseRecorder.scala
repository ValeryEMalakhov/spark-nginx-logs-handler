package com.prb.dnhs.recorders.hbase

import com.prb.dnhs.recorders.DataRecorder
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Table}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}
import org.slf4j.Logger
import HBaseUtils._

abstract class HBaseRecorder extends DataRecorder[RDD[Row]] {

  val log: Logger
  val sparkSession: SparkSession
  val dataTableName: String
  val genericSchema: StructType

  override def save(logRow: RDD[Row], path: String): Unit = {

    val logDF = sparkSession
      .createDataFrame(logRow, genericSchema)
      .select("userCookie", "eventType", "segments")
      .rdd
      .flatMap { row =>
        if (row(1) == "rt")
          Some(row(0).asInstanceOf[String], row(2).asInstanceOf[Seq[String]].map((_, "0")).toMap)
        else None
      }
      .collect

    log.info("Creating connection to HBase")
    val conf = HBaseConfiguration.create()
    val connection = ConnectionFactory.createConnection(conf)
    val admin = connection.getAdmin
    log.info("Connection created")

    val tables = admin.listTableNames()

    log.info("Creating table in HBase")
    val newTable = TableName.valueOf(dataTableName)
    val tDescriptor: HTableDescriptor = new HTableDescriptor(newTable)

    tDescriptor.addFamily(new HColumnDescriptor("SegmentsFamily"))

    createTable(admin, tDescriptor)
    log.info("Table created")

    val table: Table = connection.getTable(newTable)

    logDF.foreach { user =>
      insertLines(table, user._1, "US", user._2)
    }

    logRow.foreach(row => row.get(0))
  }
}
