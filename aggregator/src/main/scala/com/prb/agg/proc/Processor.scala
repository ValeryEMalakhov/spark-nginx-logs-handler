package com.prb.agg.proc

import com.prb.agg.chive.{BatchHiveWriter, CHiveReader}
import com.prb.agg.file.SparkFileReader
import com.prb.agg.prep.Advertiser
import com.prb.agg.sim.chive.{AdsHiveWriter, LogHiveWriter}
import com.prb.agg.sim.prep.Enricher
import org.apache.spark.sql.functions._
import org.slf4j.Logger

abstract class Processor {

  val log: Logger

  val hiveReader: CHiveReader
  val adsHiveWriter: AdsHiveWriter
  val logHiveWriter: LogHiveWriter
  val batchHiveWriterWriter: BatchHiveWriter

  val fileReader: SparkFileReader

  val adsAdvertiser: Advertiser
  val logsEnricher: Enricher

  val queriesExecutor: QueriesExec

  def process(args: ProcessorConfig): Unit = {
    log.info("Application started")

    val dbLogs = hiveReader.read("processed_data")
    val dbAds = hiveReader.read("ads")
    //dbLogs.show(false)
    //dbAds.show(false)

    val joinedData = dbLogs.join(dbAds, "AdId")

    // queriesExecutor.executeDFQueries(joinedData)
    // queriesExecutor.executeSQLQueries(joinedData)

    batchHiveWriterWriter.write(dbLogs)

    log.info("Application is finished")
  }

  private def prepEnv() = {
    val logs = logsEnricher.prepare(fileReader.read("data/logs.csv"))
    val ads = adsAdvertiser.prepare(fileReader.read("data/ads.csv"))

    logHiveWriter.write(logs)
    adsHiveWriter.write(ads)
  }
}
