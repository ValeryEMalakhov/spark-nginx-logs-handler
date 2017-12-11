package com.prb.dnhs.entities

case class LogEntry(
    dateTime: String,
    eventType: String,
    requesrId: String,
    userCookie: String,
    site: String,
    ipAddress: String,
    useragent: String,

    segments: Map[String, String]
)
