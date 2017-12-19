package com.prb.dnhs.entities

import scala.io._

import org.apache.spark.sql.types._
import org.apache.spark.sql.execution.datasources.parquet._
import org.apache.parquet.schema._
import com.typesafe.scalalogging._

// Block of global values
object SchemaRepos {

  // create a [[Logger]] wrapping the given underlying `org.slf4j.Logger`.
  val LOG = Logger("root")

  // the spark.sql private class instance that containing
  //  all the necessary methods for converting parquet schemes
  private val schemaConverter = new PublicParquetSchemaConverter()

  private val coreFile: BufferedSource = Source.fromFile("src/main/resources/schemas/core.parquet")
  private val coreMessage: MessageType = MessageTypeParser.parseMessageType(coreFile.getLines.mkString)
  private val core: StructType = schemaConverter.convert(coreMessage)

  private val rtFile: BufferedSource = Source.fromFile("src/main/resources/schemas/rt.parquet")
  private val rtMessage: MessageType = MessageTypeParser.parseMessageType(rtFile.getLines.mkString)
  private val rt: StructType = schemaConverter.convert(rtMessage)

  private val imprFile: BufferedSource = Source.fromFile("src/main/resources/schemas/impr.parquet")
  private val imprMessage: MessageType = MessageTypeParser.parseMessageType(imprFile.getLines.mkString)
  private val impr: StructType = schemaConverter.convert(imprMessage)

  private val clkFile: BufferedSource = Source.fromFile("src/main/resources/schemas/clk.parquet")
  private val clkMessage: MessageType = MessageTypeParser.parseMessageType(clkFile.getLines.mkString)
  private val clk: StructType = schemaConverter.convert(clkMessage)

  // get merged schema without duplicated columns
  private val merged: StructType = StructType((rt ++ impr ++ clk).distinct)
  private val mutablePart: StructType = StructType(merged.drop(core.length))

  private val schemaMap: Map[String, StructType] = Map(
    "core" -> core,
    "rt" -> rt,
    "impr" -> impr,
    "clk" -> clk,
    "merged" -> merged,
    "mutable" -> mutablePart)

  // the function receives the schema from Map by String-key
  def getSchema(eventType: String): StructType = {
    schemaMap(eventType)
  }

  // the function receives the schema directly from the file (alternative - not used)
  def getSchemaFromFile(name: String): StructType = {

    val file = Source.fromFile(s"src/main/resources/schemas/${name}.parquet")
    val message = MessageTypeParser.parseMessageType(file.getLines.mkString)
    schemaConverter.convert(message)
  }
}

