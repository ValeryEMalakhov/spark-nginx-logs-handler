package com.prb.dnhs.entities

import scala.io._

import org.apache.spark.sql.types._
import org.apache.spark.sql.execution.datasources.parquet._
import org.apache.parquet.schema._
import com.typesafe.scalalogging._
import com.prb.dnhs.DriverContext._

//  Block of global values
object SchemaRepos {
  // trait loger val sl4j
  lazy val log = Logger("root")

  val schemaConverter = new PublicParquetSchemaConverter()

  val coreFile: BufferedSource = Source.fromFile("src/main/resources/schemas/core.parquet")
  val coreMessage: MessageType = MessageTypeParser.parseMessageType(coreFile.getLines.mkString)
  val core: StructType = schemaConverter.convert(coreMessage)

  val rtFile: BufferedSource = Source.fromFile("src/main/resources/schemas/rt.parquet")
  val rtMessage: MessageType = MessageTypeParser.parseMessageType(rtFile.getLines.mkString)
  val rt: StructType = schemaConverter.convert(rtMessage)

  val imprFile: BufferedSource = Source.fromFile("src/main/resources/schemas/impr.parquet")
  val imprMessage: MessageType = MessageTypeParser.parseMessageType(imprFile.getLines.mkString)
  val impr: StructType = schemaConverter.convert(imprMessage)

  val clkFile: BufferedSource = Source.fromFile("src/main/resources/schemas/clk.parquet")
  val clkMessage: MessageType = MessageTypeParser.parseMessageType(clkFile.getLines.mkString)
  val clk: StructType = schemaConverter.convert(clkMessage)

  // TODO: Map schemas with ID
  // getSchema get id -> struct type

  //  get merged schema without duplicated columns
  val mergedSchema: scala.Seq[StructField] = (rt ++ impr ++ clk).distinct

  val mutablePartOfSchema: scala.Seq[StructField] = mergedSchema.drop(core.length)

}
