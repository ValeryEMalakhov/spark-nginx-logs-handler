package com.prb.dnhs.recorders

trait DataRecorder[T] {

  def save(
      data: T,
      batchId: String,
      path: String = ""): Unit
}
