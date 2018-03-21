package com.prb.dnhs.utils

import com.prb.dnhs.DriverContextIT
import com.prb.dnhs.utils.TestUtils._
import org.specs2.specification.{AfterAll, BeforeAfter, BeforeAll}

trait SpecUtils extends BeforeAll with AfterAll {

  private val log = DriverContextIT.logger

  override def beforeAll: Unit = {
    log.debug("Preparing the environment")
    prepareEnv()
  }

  override def afterAll: Unit = {
    log.debug("Creates a task for deleting test folders after execution")
    cleaningDatabase()
    cleaningFolders()
  }
}
