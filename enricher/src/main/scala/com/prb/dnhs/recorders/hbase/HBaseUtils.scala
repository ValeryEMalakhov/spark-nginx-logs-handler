package com.prb.dnhs.recorders.hbase

import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client.{Admin, Put, Table}
import org.apache.hadoop.hbase.util.Bytes

object HBaseUtils {

  def createTable(admin: Admin, table: HTableDescriptor): Unit = {
    /*
        if (admin.tableExists(table.getTableName)) {
          admin.disableTable(table.getTableName)
          admin.deleteTable(table.getTableName)
        }
        admin.createTable(table)
    */
    if (!admin.tableExists(table.getTableName)) admin.createTable(table)
  }

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
}
