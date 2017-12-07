package com.prb.dnhs.packagers

/**
  * The `FilePackager` trait can be used to
  * save the data structures in different file formats.
  */
trait FilePackager[T] {

  def save(logData: T): Unit
}
