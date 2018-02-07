package com.prb.dnhs.recorders

trait DataRecorder[T] {

  def save(data: T, path: String = ""): Unit
}
