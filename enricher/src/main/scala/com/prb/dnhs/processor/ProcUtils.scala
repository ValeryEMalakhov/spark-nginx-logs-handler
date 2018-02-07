package com.prb.dnhs.processor

import com.prb.dnhs.DriverContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

///////////////////////////////////////////////////////////////////////
//                                                                   //
// THIS IS SIMULATION! DO NOT USE WITHOUT ADAPTATION IN THE PROJECT! //
//                                                                   //
///////////////////////////////////////////////////////////////////////

object ProcUtils {

  // part as DataReader, MainParser, HiveRecorderImpl etc from Procrssor
  val procClass = new ProcClass()

  def load(path: String) = {
    procClass._load(path)
  }

  implicit class ProcOps[T](val data: RDD[T]) {

    def parse = {
      procClass._parse(data)
    }

    def handle(path: String) = {
      procClass._handle(path, data)
    }

    def print = {
      data.take(5).foreach(println)
      data
    }
  }

}

// assembled hodgepodge of all `main` classes methods
class ProcClass() {

  // from ArchiveReaderImpl
  def _load[T](path: String): RDD[String] = {
    println("We start load!")
    val seq = Seq("a\talpha\t0", "b\tbeta\t1", "b_2\tbeta_2\t1", "o\tomega\t2")

    DriverContext.sparkSession.sparkContext.parallelize(seq)
  }

  // from MainParser
  def _parse[T](data: RDD[T]): RDD[Test] = {

    println("We start parse!")
    data.map { row =>
      val parsed = row.toString.split("\t")
      Test(parsed(0), parsed(1), parsed(2).toInt)
    }
  }

  // from MainHandler
  def _handle[T](path: String, data: RDD[T]): RDD[Row] = {
    println("We start handling!")
    data.map(Row(_))
  }
}

// test class for simulation
// equivalent to `LogEntry`
case class Test(
    id: String,
    name: String,
    level: Int
)

