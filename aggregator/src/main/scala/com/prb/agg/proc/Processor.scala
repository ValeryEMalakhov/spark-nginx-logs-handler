package com.prb.agg.proc

import org.slf4j.Logger
import com.prb.agg._
import com.prb.agg.chive.{AdsHiveWriter, CHiveReader, LogHiveWriter}
import com.prb.agg.file.SparkFileReader
import com.prb.agg.prep.{Advertiser, Enricher}
import org.apache.spark.sql.functions._

abstract class Processor {

  val log: Logger

  val hiveReader: CHiveReader
  val adsHiveWriter: AdsHiveWriter
  val logHiveWriter: LogHiveWriter

  val fileReader: SparkFileReader

  val adsAdvertiser: Advertiser
  val logsEnricher: Enricher

  def process(args: ProcessorConfig): Unit = {
    log.info("Application started")

    //val ads = adsAdvertiser.prepare(fileReader.read("data/ads.csv"))
    //val logs = logsEnricher.prepare(fileReader.read("data/logs.csv"))
    //adsHiveWriter.write(ads)
    //logHiveWriter.write(logs)

    val dbAds = hiveReader.read("ads")
    val dbLogs = hiveReader.read("processed_data")
    //dbAds.show(false)
    //dbLogs.show(false)

    val joinData = dbLogs.join(dbAds, "AdId")
    //joinData.filter(joinData("eventType") === "clk").show(false)

    joinData.groupBy("ageCategory", "section", "userCookie").agg(count("userCookie")).orderBy("userCookie").show(false)

    log.info("Application is finished")
  }
}
