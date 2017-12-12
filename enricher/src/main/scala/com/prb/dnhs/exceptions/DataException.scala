package com.prb.dnhs.exceptions

case class DataException(message: String = "Incorrect data type") extends RuntimeException(message)

