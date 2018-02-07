package com.prb.dnhs.processor

case class ProcessorConfig(
    inputDir: String = "",
    outputDir: String = "",
    startupMode: String = "prod",
    debug: Boolean = false
)