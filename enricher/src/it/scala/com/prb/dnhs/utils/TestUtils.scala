package com.prb.dnhs.utils

import java.io.{File, FileOutputStream, PrintWriter}
import java.time.Instant
import java.util.zip.GZIPOutputStream

import com.prb.dnhs.DriverContextIT
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{LongType, StructField, StructType}

import scala.language.implicitConversions

object TestUtils {

  private lazy val sparkSession = DriverContextIT.dcSparkSession

  private lazy val schemas = DriverContextIT.dcSchemaRepos
  private lazy val GENERIC_EVENT = "generic-event"

  def implFolders(): Unit = {

    println("Cleaning folders before testing")
    preCleaningFolders()

    val testDir = Seq(
        new File("ITest")
      , new File("ITest/READY")
      , new File("ITest/READY/processing")
      , new File("ITest/READY/processed")
    )

    testDir.foreach(f => f.mkdir())
  }

  def createBaseDB(input: Seq[Row]): Unit = {
    lazy val batchId: Long = Instant.now.toEpochMilli

    createDataTable(sparkSession.sparkContext.parallelize(input), batchId)

    createBatchesTable(sparkSession.sparkContext.parallelize(Seq(Row(batchId))))
  }

  def preCleaningFolders(path: String = "ITest"): Unit = {
    val testDir = new File(path)
    val metastore = new File("metastore_db")
    val derby = new File("derby.log")

    if (testDir.exists)   FileUtils.forceDelete(testDir)
    if (metastore.exists) FileUtils.forceDelete(metastore)
    if (derby.exists)     FileUtils.forceDelete(derby)
  }

  def cleaningFolders(path: String = "ITest"): Unit = {
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        val testDir = new File(path)
        val metastore = new File("metastore_db")
        val derby = new File("derby.log")

        //        FileUtils.forceDeleteOnExit(testDir)
        FileUtils.forceDelete(testDir)
        FileUtils.forceDelete(metastore)
        FileUtils.forceDelete(derby)
      }
    })
  }

  private def createDataTable(input: RDD[Row], batchId: Long): Unit = {
    val genericSchema = schemas.getSchema(GENERIC_EVENT).get

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
