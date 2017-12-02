package com.prb.dnhs

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import com.prb.dnhs.entities.ParsedPixel
import com.prb.dnhs.entities.ParsedPixel._
import com.typesafe.config._

object Main extends App {

  Processor.run(args)

}

