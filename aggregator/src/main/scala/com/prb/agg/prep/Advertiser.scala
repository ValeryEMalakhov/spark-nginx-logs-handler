package com.prb.agg.prep

import com.prb.agg.entities.Ads
import org.apache.spark.rdd.RDD

class Advertiser {

  def prepare(data: RDD[String]): RDD[Ads] = {

    data.map{row =>
      val pRow = row.split(',')
      Ads(pRow(0).toInt, pRow(1), pRow(2), pRow(3), pRow(4).toInt, pRow(5))
    }
  }
}
