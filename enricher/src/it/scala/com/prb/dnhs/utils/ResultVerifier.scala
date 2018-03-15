package com.prb.dnhs.utils

import com.prb.dnhs.DriverContextIT

object ResultVerifier {

  private lazy val sparkSession = DriverContextIT.dcSparkSession
  private lazy val fs = DriverContextIT.dcFS

  private lazy val schemas = DriverContextIT.dcSchemaRepos
  private lazy val log = DriverContextIT.logger
  private lazy val GENERIC_EVENT = "generic-event"
  private lazy val PROC_TABLE = "processed_data"

  def checkBatchAvailability(batchId: Long): Boolean =
    sparkSession.sql(s"SHOW PARTITIONS default.$PROC_TABLE")
    .collect
    .exists(_.toString.contains(batchId))
}
