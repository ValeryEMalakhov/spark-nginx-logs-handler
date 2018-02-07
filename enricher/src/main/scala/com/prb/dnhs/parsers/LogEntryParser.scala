package com.prb.dnhs.parsers

import scala.language.implicitConversions
import scala.util.control.NonFatal

import com.prb.dnhs.entities.{LogEntry, SchemaRepositorу}
import com.prb.dnhs.exceptions.ErrorType._
import com.prb.dnhs.exceptions._
import com.prb.dnhs.helpers.LoggerHelper
import com.prb.dnhs.validators._
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._

abstract class LogEntryParser
  extends DataParser[LogEntry, Either[ErrorDetails, Row]]
    with LoggerHelper {

  val schemas: SchemaRepositorу
  val queryStringValidator: QueryStringValidator

  val TAB = "\t"

  /**
    * Parse and validate the incoming LogEntry, in particular the queryString field
    *
    * @param logEntry the current parsing string
    */
  override def parse(logEntry: LogEntry): Either[ErrorDetails, Row] = {

    val logEntryRow = buildLogEntryRow(logEntry)

    for {
      // get the current event scheme
      eventSchema <- getEventSchemaStructure(
        logEntry,
        logEntry.eventType).right

      // get the generic scheme
      genericSchema <- getEventSchemaStructure(
        logEntry,
        schemas.GENERIC_EVENT).right

      // get validated query string as a Seq of Row
      validatedRow <- queryStringValidator.validate(
        logEntry,
        logEntryRow.length,
        eventSchema).right

      // get the parsed and convert the query string as a Seq of Row
      queryStringRowSeq <- buildQueryStringRowSeq(
        logEntry,
        logEntryRow.length,
        genericSchema).right

      // concat general and mutable fields
      concatRow <- buildConcatenatedRow(
        logEntry,
        logEntryRow,
        queryStringRowSeq,
        genericSchema).right
    } yield concatRow
  }

  /**
    * Save as Row general fields which are common for all schemes
    * and verification of which was carried out at the previous stage
    *
    * @param logEntry the current parse string
    */
  private def buildLogEntryRow(logEntry: LogEntry) = {
    Row(
      logEntry.dateTime,
      logEntry.eventType,
      logEntry.requestId,
      logEntry.userCookie,
      logEntry.site,
      logEntry.ipAddress,
      logEntry.useragent
    )
  }

  /**
    *
    * @param logEntry     the current parsing string
    * @param logEntrySize number of general schema fields
    * @param eventSchema  the current event schema
    */
  private def buildQueryStringRowSeq(
      logEntry: LogEntry,
      logEntrySize: Int,
      eventSchema: StructType) = {

    val queryStringSchema = eventSchema.fields.drop(logEntrySize)

    Right(queryStringSchema.map { field =>
      if (logEntry.queryString.exists(_._1 == field.name)) {
        Row(convertFieldValue(logEntry, field))
      } else Row(null)
    })
  }

  /**
    * Converts the value of the input field to the type specified by the schema
    *
    * @param logEntry the current parsing string
    * @param field    a field which value must be converted to the specified type
    */
  private def convertFieldValue(logEntry: LogEntry, field: StructField) = {
    field.dataType match {
      case StringType => logEntry.queryString(field.name).toString
      case IntegerType => logEntry.queryString(field.name).toInt
      case FloatType => logEntry.queryString(field.name).toFloat
      case ArrayType(StringType, _) => logEntry.queryString(field.name).split(",").toList
    }
  }

  /**
    * Builds from `generalFields` and `mutableFields` one generic row with generic `schema`
    *
    * @param logEntryRow       general columns, gets from the LogEntry fields except the queryString
    * @param queryStringRowSeq mutable columns, gets from parsed queryString
    * @param schema            generic schema
    */
  private def buildConcatenatedRow(
      logEntry: LogEntry,
      logEntryRow: Row,
      queryStringRowSeq: Seq[Row],
      schema: StructType) = {

    try Right(
      new GenericRowWithSchema(
        queryStringRowSeq
          .foldLeft(logEntryRow)((head: Row, tail: Row) => Row.merge(head, tail))
          .toSeq
          .toArray,
        schema
      ))
    catch {
      case NonFatal(ex) => Left(
        ErrorDetails(
          errorType = ParserError,
          errorMessage = ex.getMessage,
          line = buildErrString(logEntry)
        )
      )
    }
  }

  //TODO maybe change `getSchema` from `Option` to `Either` and remove this method
  /**
    * Gets an array of variable fields with field name, it's type, and nullification
    * from the `schemaName` scheme
    *
    * @param schemaName name of current `eventType`
    */
  private def getEventSchemaStructure(
      logEntry: LogEntry,
      schemaName: String)
  : Either[ErrorDetails, StructType] = {

    try Right {
      schemas
        .getSchema(schemaName)
        .get
    }
    catch {
      case NonFatal(ex) => Left(
        ErrorDetails(
          errorType = InvalidSchemaError,
          errorMessage = s"$schemaName not found",
          line = buildErrString(logEntry)
        )
      )
    }
  }

  /**
    * Returns the current parse string to the original format
    *
    * @param logEntry the current parsing string
    */
  private def buildErrString(logEntry: LogEntry) = {

    val generalFields =
      Seq(logEntry.dateTime,
        logEntry.eventType,
        logEntry.requestId,
        logEntry.userCookie,
        logEntry.site,
        logEntry.ipAddress,
        logEntry.useragent).mkString(TAB)

    val queryString =
      logEntry
        .queryString
        .mkString("&")
        .replaceAll(" -> ", "=")

    s"$generalFields$TAB$queryString"
  }
}

