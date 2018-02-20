package com.prb.dnhs.parsers

trait DataParser[T, O] {

  def parse(inputLog: T): O
}
