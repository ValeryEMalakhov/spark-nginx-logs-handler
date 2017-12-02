package com.prb.dnhs.executors

/**
  * The `FilePackager` trait can be used to
  * save the data structures in different file formats.
  */
trait FilePackager[T] {

  def save(logData: T): Unit
}
