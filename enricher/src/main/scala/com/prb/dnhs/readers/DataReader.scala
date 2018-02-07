package com.prb.dnhs.readers

trait DataReader[O] {

  def read(inputDir: String = ""): O
}
