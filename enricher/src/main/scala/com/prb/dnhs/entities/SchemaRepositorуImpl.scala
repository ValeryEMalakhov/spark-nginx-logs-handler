package com.prb.dnhs.entities

import scala.io.Source

import com.prb.dnhs.helpers.ConfigHelper
import org.apache.parquet.schema.{MessageType, MessageTypeParser}
import org.apache.spark.sql.execution.datasources.parquet.PublicParquetSchemaConverter
import org.apache.spark.sql.types.StructType

class SchemaRepositorуImpl extends SchemaRepositorу with ConfigHelper {

  import SchemaRepositorуImpl._

  // the spark.sql private class instance that containing
  //  all the necessary methods for converting parquet schemes
  private val schemaConverter = new PublicParquetSchemaConverter()

  def getSchema(schemaName: String): Option[StructType] = {
    schemas.get(schemaName)
  }

  private def readParquetSchema(schemaName: String): StructType = {
    val path = getClass.getResource(s"/schemas/$schemaName")
    val fileSource = Source.fromURL(path)
    val file = fileSource.getLines

    val message: MessageType = MessageTypeParser.parseMessageType(file.mkString)

    schemaConverter.convert(message)
  }

  private def readPixelSchemas(): Map[String, StructType] = {
    DEFAULT_SCHEMAS.flatMap { file =>
      val structType = readParquetSchema(file)
      Map(file.dropRight(8) -> structType)
    }.toMap

  }

  private def getGenericEvent(schemaMap: Map[String, StructType]): StructType = {
    StructType(schemaMap.values.toList.flatten.distinct)
  }

  private val schemas = {
    val pixelSchemas = readPixelSchemas()
    val generic = getGenericEvent(pixelSchemas)

    pixelSchemas ++ Map(GENERIC_EVENT -> generic)
  }
}

object SchemaRepositorуImpl {
  val GENERIC_EVENT = "generic-event"

  val DEFAULT_SCHEMAS = List("clk.parquet", "impr.parquet", "rt.parquet")
}

