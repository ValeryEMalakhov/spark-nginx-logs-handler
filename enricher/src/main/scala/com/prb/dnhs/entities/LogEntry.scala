package com.prb.dnhs.entities

case class LogEntry(
    dateTime: String,
    eventType: String,
    requestId: String,
    userCookie: String,
    site: String,
    ipAddress: String,
    useragent: String,
    segments: Map[String, String],
    mutableFields: Map[String, String]
)
