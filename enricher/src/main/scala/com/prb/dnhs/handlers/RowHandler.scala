package com.prb.dnhs.handlers

trait RowHandler[T, O] {

  def handle(
      data: T,
      batchId: String = "",
      outputDir: String = ""): O
}
