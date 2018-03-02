package com.prb.dnhs.utils

import java.io.{File, FileOutputStream, PrintWriter}
import java.util.zip.GZIPOutputStream

import org.apache.commons.io.FileUtils

object TestUtils {

  def implFolders(): Unit = {
    val testDir = Seq(
      new File("ITest")
      , new File("ITest/READY")
      , new File("ITest/READY/processed_data")
      , new File("ITest/READY/processed_batches")
    )

    testDir.foreach(f => f.mkdir())
  }

  def cleanFolders(path: String = "ITest"): Unit = {
    val testDir = new File(path)
    FileUtils.deleteDirectory(testDir)
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
