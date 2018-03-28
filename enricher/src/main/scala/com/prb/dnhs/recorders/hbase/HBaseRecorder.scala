package com.prb.dnhs.recorders.hbase

import com.prb.dnhs.recorders.DataRecorder
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, ConnectionFactory, Put}
import org.apache.hadoop.hbase.spark._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, sql}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions.{collect_list, udf}
import org.slf4j.Logger

abstract class HBaseRecorder extends DataRecorder[RDD[sql.Row]] {

  val log: Logger
  val sparkContext: SparkContext
  val sqlContext: SQLContext
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
    val conf = HBaseConfiguration.create()
    // conf.addResource(new Path("/etc/hbase/conf/core-site.xml"))
    // conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"))

    val connection = ConnectionFactory.createConnection(conf)
    val admin = connection.getAdmin
    log.info("Connection created")

    log.info("Creating table in HBase")
    val tableNameVal = TableName.valueOf(tableName)
    val tDescriptor: HTableDescriptor = new HTableDescriptor(tableNameVal)

    tDescriptor.addFamily(new HColumnDescriptor(columnFamily))

    HBaseRecorder.createTable(admin, tDescriptor)
    log.info("Table created")

    val hbaseContext = new HBaseContext(sparkContext, conf)

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

    //(RowKey, columnFamily, columnQualifier, value)
    //val table: Table = connection.getTable(tableNameVal)
  }
}

object HBaseRecorder {
  def dfTransform(in: DataFrame, cf: String): RDD[(Array[Byte], Seq[(Array[Byte], Array[Byte], Array[Byte])])] = {
    in.rdd.flatMap { row =>
      row.get(1).asInstanceOf[Map[String, String]].map { r =>
        (Bytes.toBytes(row(0).asInstanceOf[String]), Seq((Bytes.toBytes(cf), Bytes.toBytes(r._1), Bytes.toBytes(r._2))))
      }
    }
  }

  def createTable(admin: Admin, table: HTableDescriptor): Unit = {
    if (!admin.tableExists(table.getTableName)) admin.createTable(table)
    /*
        if (admin.tableExists(table.getTableName)) {
          admin.disableTable(table.getTableName)
          admin.deleteTable(table.getTableName)
        }
        admin.createTable(table)
    */
  }

  /*
    /**
      * Insert a line to HBase
      *
      * @param table target table
      * @param rk    line's row key
      * @param cf    column family's name
      * @param q     column family's qualifier
      * @param value value to insert
      */
    def insertLine(table: Table, rk: String, cf: String, q: String, value: String): Unit = {
      val put = new Put(Bytes.toBytes(rk))
      put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(q), Bytes.toBytes(value))
      // TODO: use bulk-writer as HBase Connector
      table.put(put)
    }

    /**
      * Insert multiple lines into a row
      *
      * @param table   target table
      * @param rk      lines row key
      * @param cf      column family's name
      * @param content map of column family's qualifiers and corresponding values
      */
    def insertLines(table: Table, rk: String, cf: String, content: Map[String, String]): Unit = {
      val put = new Put(Bytes.toBytes(rk))
      content.foreach(x => put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(x._1), Bytes.toBytes(x._2)))
      table.put(put)
    }
  */
}
