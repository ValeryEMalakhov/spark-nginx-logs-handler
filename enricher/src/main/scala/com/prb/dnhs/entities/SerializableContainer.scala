package com.prb.dnhs.entities

abstract class SerializableContainer[T] {
  def func : T

  @transient
  lazy val function : T = func
}

