package com.prb.dnhs.entities

import java.io.File

import scala.io.{BufferedSource, Source}

import com.prb.dnhs.LoggerHelper
import org.apache.parquet.schema.{MessageType, MessageTypeParser}
import org.apache.spark.sql.execution.datasources.parquet.PublicParquetSchemaConverter
import org.apache.spark.sql.types.StructType

trait SchemaRepositorу {
  def getSchema(schemaName: String): Option[StructType]
}

object SchemaRepositorу {
  val GENERIC_EVENT = "generic-event"
}
trait SchemaRepositorуImpl extends SchemaRepositorу {
  import SchemaRepositorу._
  // the spark.sql private class instance that containing
  //  all the necessary methods for converting parquet schemes
  private val schemaConverter = new PublicParquetSchemaConverter()

  def getSchema(schemaName: String): Option[StructType] = {
    schemas.get(schemaName)
  }


  private def readParquetSchema(schemaName: String): StructType = {
    val file: BufferedSource = Source.fromFile(s"src/main/resources/schemas/$schemaName")
    val message: MessageType = MessageTypeParser.parseMessageType(file.getLines.mkString)
    schemaConverter.convert(message)
  }

  private def readPixelSchemas(): Map[String, StructType] = {
    val parquetFileList: Array[String] = new File("src/main/resources/schemas").list()


    parquetFileList.flatMap { file =>
      val structType = readParquetSchema(file)
      Map(file.dropRight(8) -> structType)
    }.toMap
  }

  private def getGenericEvent(schemaMap: Map[String, StructType]):StructType ={ StructType(schemaMap.values.toList.flatten.distinct)}

  private val schemas = {
    val pixelSchemas = readPixelSchemas()
    val generic = getGenericEvent(pixelSchemas)
    pixelSchemas ++ Map(GENERIC_EVENT -> generic)
  }
}

