package com.prb.dnhs.entities

case class ParsedPixel(
    dateTime: String,
    eventType: String,
    requesrId: String,
    userCookie: String,
    site: String,
    ipAddress: String,
    useragent: String,

    segments: String
)
