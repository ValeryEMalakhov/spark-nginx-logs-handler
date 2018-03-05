package com.prb.dnhs.utils

import java.io.{File, FileOutputStream, PrintWriter}
import java.time.Instant
import java.util.zip.GZIPOutputStream

import com.prb.dnhs.TestDriverContext
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{LongType, StructField, StructType}

import scala.language.implicitConversions

object TestUtils {

  private val sparkSession = TestDriverContext.dcSparkSession

  private val schemas = TestDriverContext.dcSchemaRepos
  private val GENERIC_EVENT = "generic-event"

  def implFolders(): Unit = {
    val testDir = Seq(
      new File("ITest")
      , new File("ITest/READY")
      , new File("ITest/READY/processed_data")
      , new File("ITest/READY/processed_batches")
    )

    testDir.foreach(f => f.mkdir())
  }

  def createBaseDB(input: Seq[Row]): Unit = {
    // lazy val batchId: Long = Instant.now.toEpochMilli

    createDataTable(sparkSession.sparkContext.parallelize(input), TestDriverContext.globalBatchId)

    createBatchesTable(sparkSession.sparkContext.parallelize(Seq(Row(TestDriverContext.globalBatchId))))
  }

  def cleanFolders(path: String = "ITest"): Unit = {
    val derby = new File("derby.log")
    val metastore = new File("metastore_db")
    val testDir = new File(path)

    FileUtils.forceDelete(derby)
    FileUtils.deleteDirectory(metastore)
    FileUtils.deleteDirectory(testDir)
  }

  private def createDataTable(input: RDD[Row], batchId: Long): Unit = {
    val genericSchema = schemas.getSchema("GENERIC_EVENT").get

    sparkSession
      .createDataFrame(
        input,
        genericSchema
      )
      .withColumn("batchId", lit(batchId))
      .write
      .format("parquet")
      .partitionBy("batchId")
      .saveAsTable("processed_data")
  }

  private def createBatchesTable(input: RDD[Row]): Unit = {
    sparkSession
      .createDataFrame(
        input,
        StructType(StructField("batchId", LongType, nullable = false) :: Nil)
      )
      .write
      .format("parquet")
      .saveAsTable("processed_batches")
  }

  implicit class TestOps[T](val input: String) extends AnyVal {

    def writeFile(name: String, path: String = "ITest/READY"): Unit = {

      val fos = new FileOutputStream(s"$path/$name")
      val gzos = new GZIPOutputStream(fos)
      val w = new PrintWriter(gzos)

      w.write(input)

      w.close()
      gzos.close()
      fos.close()
    }
  }

}
