package com.prb.dnhs.validators

trait Validator[T, O] {

  def validate(data: T): O
}

