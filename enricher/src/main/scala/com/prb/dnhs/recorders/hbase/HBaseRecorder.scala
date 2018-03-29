package com.prb.dnhs.recorders.hbase

import com.prb.dnhs.recorders.DataRecorder
import org.apache.hadoop.hbase.client.{Admin, Connection, Put}
import org.apache.hadoop.hbase.spark._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{collect_list, udf}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, sql}
import org.slf4j.Logger

abstract class HBaseRecorder extends DataRecorder[RDD[sql.Row]] {

  val log: Logger
  val sparkContext: SparkContext
  val sqlContext: SQLContext
  val hbaseConn: Connection
  val hbaseContext: HBaseContext
  val tableName: String
  val columnFamily: String
  val columnQualifier: String
  val genericSchema: StructType

  private val USER = "userCookie"
  private val EVENT = "eventType"
  private val SEGMENTS = "segments"

  override def save(logRow: RDD[sql.Row]): Unit = {

    val logDF = sqlContext
      .createDataFrame(logRow, genericSchema)
      .select(USER, EVENT, SEGMENTS)

    val flattenMap = udf((xs: Seq[Map[String, String]]) => xs.reduceLeft(_ ++ _))
    val flattenList = udf((xs: Seq[Seq[String]]) => xs.map(_.map((_, "0")).toMap))

    val fDF = logDF
      .filter(logDF(EVENT) === "rt")
      .groupBy(USER)
      .agg(flattenMap(flattenList(collect_list(SEGMENTS))).as(SEGMENTS))
      .toDF

    fDF.show(false)

    val hbaseRDD = HBaseRecorder.dfTransform(fDF, columnFamily)

    hbaseRDD.collect.foreach(println)

    log.info("Creating connection to HBase")
    val admin = hbaseConn.getAdmin

    log.info("Creating table in HBase")
    val tableNameVal = TableName.valueOf(tableName)
    val tDescriptor: HTableDescriptor = new HTableDescriptor(tableNameVal)

    tDescriptor.addFamily(new HColumnDescriptor(columnFamily))

    HBaseRecorder.createTable(admin, tDescriptor)
    log.info("Table created if not exist")

    hbaseContext.bulkPut[(Array[Byte], Seq[(Array[Byte], Array[Byte], Array[Byte])])](
      hbaseRDD,
      tableNameVal,
      (row) => {
        val put = new Put(row._1)
        row._2.foreach { (value) =>
          // family name / column qualifier / column value
          put.addColumn(value._1, value._2, value._3)
        }
        put
      }
    )
  }
}

object HBaseRecorder {

  private def dfTransform(in: DataFrame, cf: String) = {
    in.rdd.flatMap { row =>
      row.get(1).asInstanceOf[Map[String, String]].map { r =>
        (Bytes.toBytes(row(0).asInstanceOf[String]), Seq((Bytes.toBytes(cf), Bytes.toBytes(r._1), Bytes.toBytes(r._2))))
      }
    }
  }

  private def createTable(admin: Admin, table: HTableDescriptor): Unit = {
    if (!admin.tableExists(table.getTableName)) admin.createTable(table)
  }

}
