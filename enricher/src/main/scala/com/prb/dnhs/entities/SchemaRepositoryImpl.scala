package com.prb.dnhs.entities

import scala.io.Source

import org.apache.parquet.schema.{MessageType, MessageTypeParser}
import org.apache.spark.sql.execution.datasources.parquet.PublicParquetSchemaConverter
import org.apache.spark.sql.types.StructType
import SchemaRepositorу._

class SchemaRepositoryImpl extends SchemaRepositorу {

  // the spark.sql private class instance that containing
  //  all the necessary methods for converting parquet schemes
  private val schemaConverter = new PublicParquetSchemaConverter()

  def getSchema(schemaName: String): Option[StructType] = {
    schemas.get(schemaName)
  }

  private[entities] def readParquetSchema(schemaName: String) = {
    val path = getClass.getResource(s"/schemas/$schemaName")
    val fileSource = Source.fromURL(path)
    val file = fileSource.getLines

    val message: MessageType = MessageTypeParser.parseMessageType(file.mkString)

    schemaConverter.convert(message)
  }

  private def readPixelSchemas() = {
    DEFAULT_SCHEMAS.flatMap { file =>
      val structType = readParquetSchema(file._2)
      Map(file._1 -> structType)
    }
  }

  private def getGenericEvent(schemaMap: Map[String, StructType]) = {
    StructType(schemaMap.values.toList.flatten.distinct)
  }

  private val schemas = {

    val pixelSchemas = readPixelSchemas()
    val generic = getGenericEvent(pixelSchemas)

    pixelSchemas ++ Map(GENERIC_EVENT -> generic)
  }
}

