package com.prb.dnhs.entities

import scala.io._

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.types._
import org.apache.spark.sql.execution.datasources.parquet._
import org.apache.parquet.schema._

private class SchemaRepos {

  // the spark.sql private class instance that containing
  //  all the necessary methods for converting parquet schemes
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

  // get merged schema without duplicated columns
  val merged: StructType = StructType((rt ++ impr ++ clk).distinct)
  val mutablePart: StructType = StructType(merged.drop(core.length))

  val schemaMap: Map[String, StructType] = Map(
    "core" -> core,
    "rt" -> rt,
    "impr" -> impr,
    "clk" -> clk,
    "merged" -> merged,
    "mutable" -> mutablePart)
}

// Block of global values
object SchemaRepos {

  private val sr = new SchemaRepos()

  // the function receives the schema from Map by String-key
  def getSchema(eventType: String): StructType = {
    sr.schemaMap(eventType)
  }

  // the function receives the schema directly from the file (alternative - not used)
  def getSchemaFromFile(name: String): StructType = {

    val file = Source.fromFile(s"src/main/resources/schemas/${name}.parquet")
    val message = MessageTypeParser.parseMessageType(file.getLines.mkString)
    sr.schemaConverter.convert(message)
  }
}