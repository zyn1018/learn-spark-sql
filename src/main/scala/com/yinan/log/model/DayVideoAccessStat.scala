package com.yinan.log.model

/**
  * 统计每天课程访问次数实体类
  *
  * @param day
  * @param cmsId
  * @param times
  */
case class DayVideoAccessStat(day: String, cmsId: Long, times: Long)
