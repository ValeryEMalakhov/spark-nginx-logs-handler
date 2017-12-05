package com.prb.dnhs

import java.io._
import scala.io._

object Main extends App {

  Processor.run(args)

  //  val stream : InputStream = getClass.getClassLoader.getResourceAsStream("someResFile.txt")
  //  val file: Iterator[String] = Source.fromInputStream(stream).getLines
}

