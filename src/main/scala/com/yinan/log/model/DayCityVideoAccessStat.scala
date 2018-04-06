package com.yinan.log.model

/**
  * 每个城市最受欢迎的课程排行
  *
  * @param day
  * @param cmsId
  * @param city
  * @param times
  * @param timesRank
  */
case class DayCityVideoAccessStat(day: String, cmsId: Long, city: String, times: Long, timesRank: Int)
