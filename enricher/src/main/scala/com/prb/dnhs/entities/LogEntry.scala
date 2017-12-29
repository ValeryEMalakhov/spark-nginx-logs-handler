package com.prb.dnhs.entities

import cats.data.Validated

case class LogEntry(
    dateTime: String,
    eventType: String,
    requestId: String,
    userCookie: String,
    site: String,
    ipAddress: String,
    useragent: String,
    queryString: Map[String, String] = Map.empty
) {

//  def validate(): Validated[this] = {
//
//  }
}
