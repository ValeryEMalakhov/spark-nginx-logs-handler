package com.prb.dnhs.validators

trait Validator[T, O] {

  def validate(inputData: T): O
}

