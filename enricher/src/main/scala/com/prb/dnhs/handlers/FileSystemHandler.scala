package com.prb.dnhs.handlers

trait FileSystemHandler[T] {

  def handle(): T
}
