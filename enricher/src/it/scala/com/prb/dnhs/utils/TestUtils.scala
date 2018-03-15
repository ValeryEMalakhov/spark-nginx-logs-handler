package com.prb.dnhs.utils

import java.io.{FileOutputStream, PrintWriter}
import java.time.Instant
import java.util.zip.GZIPOutputStream

import com.prb.dnhs.DriverContextIT
//import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.lit

import scala.language.implicitConversions

object TestUtils {

  private lazy val sparkSession = DriverContextIT.dcSparkSession
  private lazy val fs = DriverContextIT.dcFS

  private lazy val schemas = DriverContextIT.dcSchemaRepos
  private lazy val log = DriverContextIT.logger
  private lazy val GENERIC_EVENT = "generic-event"
  private lazy val PROC_TABLE = "processed_data"

  private val defaultPreviousData = Seq[Row](
    Row("20/Feb/2018:17:01:48 +0000", "rt", "67d5a56d15ca093a16b0a9706f40ba63",
      "100", "192.168.80.132", "192.168.80.1",
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:58.0) Gecko/20100101 Firefox/58.0",
      List("{121", "true", "some info", "1}"), null),
    Row("20/Feb/2018:17:01:57 +0000", "impr", "ef9237b744f404a53aa54acfef0e4f7d",
      "100", "192.168.80.132", "192.168.80.1",
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:58.0) Gecko/20100101 Firefox/58.0",
      null, 100),
    Row("20/Feb/2018:17:02:09 +0000", "impr", "17c8beb0d1dab1cb6d7c4fe8dc22fe56",
      "100", "192.168.80.132", "192.168.80.1",
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:58.0) Gecko/20100101 Firefox/58.0",
      null, 101),
    Row("20/Feb/2018:17:02:13 +0000", "impr", "8c1d43221121cbcf2eecc4afc696980c",
      "100", "192.168.80.132", "192.168.80.1",
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:58.0) Gecko/20100101 Firefox/58.0",
      null, 102),
    Row("20/Feb/2018:17:02:24 +0000", "rt", "14cee1544a7048880e4dffee0e4b3e5a",
      "101", "192.168.80.132", "192.168.80.1",
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:58.0) Gecko/20100101 Firefox/58.0",
      List("{121", "true", "some info", "1}"), null)
  )

  def prepareEnv(inputPreData: Seq[Row] = defaultPreviousData): Unit = {
    log.debug("Cleaning up folders before creating new ones")
    cleaningFolders()

    log.debug("File system preparation")
    prepareFolders()

    log.debug("File system preparation")
    prepareDatabase(inputPreData)
  }

  def cleaningFolders(path: String = "ITest"): Unit = {

    fs.delete(new Path(path), true)
    fs.delete(new Path("metastore_db"), true)
    fs.delete(new Path("derby.log"), true)

    log.debug("Folders deleted")
    //    val testDir = new File(path)
    //    val metastore = new File("metastore_db")
    //    val derby = new File("derby.log")

    //    if (testDir.exists) FileUtils.forceDelete(testDir)
    //    if (metastore.exists) FileUtils.forceDelete(metastore)
    //    if (derby.exists) FileUtils.forceDelete(derby)
  }

  def prepareFolders(): Unit = {
    fs.mkdirs(new Path("ITest/READY/processing"))
    fs.mkdirs(new Path("ITest/READY/processed"))
    log.debug("Folders created")
  }

  def prepareDatabase(input: Seq[Row]): Unit = {
    lazy val batchId: Long = Instant.now.toEpochMilli

    val genericSchema = schemas.getSchema(GENERIC_EVENT).get

    sparkSession
      .createDataFrame(
        sparkSession.sparkContext.parallelize(input),
        genericSchema
      )
      .withColumn("batchId", lit(batchId))
      .write
      .format("parquet")
      .partitionBy("batchId")
      .saveAsTable(PROC_TABLE)

    log.debug("Database created")
  }

  def prepareFile(data: String, name: String, path: String = "ITest/READY"): Unit = {

    val fos = new FileOutputStream(s"$path/$name")
    val gzos = new GZIPOutputStream(fos)
    val w = new PrintWriter(gzos)

    w.write(data)

    w.close()
    gzos.close()
    fos.close()

    log.debug("File saved")
  }
}
