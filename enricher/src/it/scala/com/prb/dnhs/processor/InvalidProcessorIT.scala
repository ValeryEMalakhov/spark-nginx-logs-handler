package com.prb.dnhs.processor

import com.prb.dnhs.DriverContextIT
import com.prb.dnhs.utils.ResultVerifier._
import com.prb.dnhs.utils.SpecUtils
import com.prb.dnhs.utils.TestUtils._
import org.specs2._

import scala.language.implicitConversions

class InvalidProcessorIT extends mutable.Specification
  with SpecUtils
  with Serializable {

  private val log = DriverContextIT.logger

  log.debug("Preparing the test variables")
  private val testData =
    "20/Feb/2018:17:03:26 +0000\terr\t4276eef079760f85665bceeaa015567d\t" +
      "101\t192.168.80.132\t192.168.80.1\t" +
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:58.0) Gecko/20100101 Firefox/58.0\t" +
      "AdId=101\n" +
      "20/Feb/2018:17:03:38 +0000\terr\t6a460ba62095b69625a71b07b141ad99\t" +
      "100\t192.168.80.132\t192.168.80.1\t" +
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:58.0) Gecko/20100101 Firefox/58.0\t" +
      "AdId=103\n" +
      "20/Feb/2018:17:03:55 +0000\terr\td83dc4abc50ed3a0df86ea95a90e6efe\t" +
      "102\t192.168.80.132\t192.168.80.1\t" +
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:58.0) Gecko/20100101 Firefox/58.0\t" +
      "segments={121,true,some%20info,1}"

  log.debug("Test execution started")
  "If the operation of the log handler is not successful" >> {

    "the current batch shouldn't have been added to the table of processed data" >> {
      log.debug("Preparing the test dataset")
      prepareFile(data = testData, name = "invalid.log.gz")

      DriverContextIT.processor.process(ProcessorConfig(debug = true))

      checkBatchAvailability(DriverContextIT.globalBatchId) must beFalse
    }
  }
}

