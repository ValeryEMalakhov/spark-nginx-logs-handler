package com.prb.dnhs.parsers

trait DataParser[T, O] {

  def parse(logData: T): O
}
