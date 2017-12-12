package com.prb.dnhs.exceptions

case class ParseException(message: String = "LogEntry parse failed.") extends RuntimeException(message)

