package com.prb.dnhs.entities

abstract class SerializableContainer[T] extends Serializable {

  def obj : T

  @transient
  lazy val value : T = obj
}

