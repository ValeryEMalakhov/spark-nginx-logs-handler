package com.prb.dnhs.parsers

trait DataParser[T, O] extends Serializable {

  def parse(logData: T): O
}
