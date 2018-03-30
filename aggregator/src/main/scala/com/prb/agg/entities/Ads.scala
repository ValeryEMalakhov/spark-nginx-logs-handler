package com.prb.agg.entities

/**
  * @example Ads(0, "some products", "some manufacturer", "some software", 14, "1.0.0")
  *
  * @param AdId advertising id
  * @param name name of advertising
  * @param owner producer or owner of advertising
  * @param section advertising section (products, spare parts, toys, etc.)
  * @param ageCategory lower age limit for the display of advertising
  * @param version version of advertising
  */
case class Ads(
      AdId: Int
    , name: String
    , owner: String
    , section: String
    , ageCategory: Int
    , version: String
)
