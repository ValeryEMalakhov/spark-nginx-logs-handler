package com.prb.dnhs.exceptions

import java.time.Instant

case class ErrorDetails(
    timestamp: Long = Instant.now.toEpochMilli,
    errorType: ErrorType,
    errorMessage: String,
    line: String
)
