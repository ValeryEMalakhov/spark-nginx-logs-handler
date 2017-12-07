package com.prb.dnhs.entities

case class LogEvent(
    dateTime: String,
    eventType: String,
    requesrId: String,
    userCookie: String,
    site: String,
    ipAddress: String,
    useragent: String,

    //  For `clk`
    AdId: String,
    SomeId: String
)
