package com.prb.dnhs.entities

abstract class SerializableContainer[T] {

  def obj : T

  @transient
  lazy val value : T = obj
}

