package com.prb.dnhs

import org.specs2._

import utils.TestUtils._

class ProcessorIT extends mutable.Specification {

  //Preparing the environment
  /**
    * Create FS folders /ITest /ITest/READY
    * /ITest/READY/processing /ITest/READY/processed
    * Write in folder /ITest/READY itest text file
    * Crete local metastore + db itest
    * Write in folder /ITest/READY/processed itest fin
    */

  val validSrc = "20/Feb/2018:17:03:26 +0000\timpr\t4276eef079760f85665bceeaa015567d\t101\t192.168.80.132\t192.168.80.1\tMozilla/5.0 (Windows NT 10.0; Win64; x64; rv:58.0) Gecko/20100101 Firefox/58.0\tAdId=101\n20/Feb/2018:17:03:38 +0000\tclk\t6a460ba62095b69625a71b07b141ad99\t100\t192.168.80.132\t192.168.80.1\tMozilla/5.0 (Windows NT 10.0; Win64; x64; rv:58.0) Gecko/20100101 Firefox/58.0\tAdId=103\n20/Feb/2018:17:03:55 +0000\trt\td83dc4abc50ed3a0df86ea95a90e6efe\t102\t192.168.80.132\t192.168.80.1\tMozilla/5.0 (Windows NT 10.0; Win64; x64; rv:58.0) Gecko/20100101 Firefox/58.0\tsegments={121,true,some%20info,1}"
  val invalidSrc = "20/Feb/2018:17:07:18 +0000\timpr\t50064af250c05b50039118f1de157923\t106\t192.168.80.132\t192.168.80.1\tMozilla/5.0 (Windows NT 10.0; Win64; x64; rv:58.0) Gecko/20100101 Firefox/58.0\tAdId=112\n20/Feb/2018:17:07:28 +0000\tclk\t131b415f76bfc79e4f1aa603c15e9125\t106\t192.168.80.132\t192.168.80.1\tMozilla/5.0 (Windows NT 10.0; Win64; x64; rv:58.0) Gecko/20100101 Firefox/58.0\tAdId=err"
  val processedSrc = "20/Feb/2018:17:01:48 +0000\trt\t67d5a56d15ca093a16b0a9706f40ba63\t100\t192.168.80.132\t192.168.80.1\tMozilla/5.0 (Windows NT 10.0; Win64; x64; rv:58.0) Gecko/20100101 Firefox/58.0\tsegments={121,true,some%20info,1}\n20/Feb/2018:17:01:57 +0000\timpr\tef9237b744f404a53aa54acfef0e4f7d\t100\t192.168.80.132\t192.168.80.1\tMozilla/5.0 (Windows NT 10.0; Win64; x64; rv:58.0) Gecko/20100101 Firefox/58.0\tAdId=100\n20/Feb/2018:17:02:09 +0000\timpr\t17c8beb0d1dab1cb6d7c4fe8dc22fe56\t100\t192.168.80.132\t192.168.80.1\tMozilla/5.0 (Windows NT 10.0; Win64; x64; rv:58.0) Gecko/20100101 Firefox/58.0\tAdId=101\n20/Feb/2018:17:02:13 +0000\timpr\t8c1d43221121cbcf2eecc4afc696980c\t100\t192.168.80.132\t192.168.80.1\tMozilla/5.0 (Windows NT 10.0; Win64; x64; rv:58.0) Gecko/20100101 Firefox/58.0\tAdId=102\n20/Feb/2018:17:02:24 +0000\trt\t14cee1544a7048880e4dffee0e4b3e5a\t101\t192.168.80.132\t192.168.80.1\tMozilla/5.0 (Windows NT 10.0; Win64; x64; rv:58.0) Gecko/20100101 Firefox/58.0\tsegments={121,true,some%20info,1}"

  println("Create")
  implFolders()
  println("Write")
  validSrc.writeFile("valid.log.gz")
  println("Delete")
  cleanFolders()
  println("Done")

  //Preparing variables


  //Testing

  "Some test" >> {
    1 must_== 1
  }


}
