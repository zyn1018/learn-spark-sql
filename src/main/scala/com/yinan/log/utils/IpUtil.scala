package com.yinan.log.utils

import com.ggstar.util.ip.IpHelper

object IpUtil {
  def getCity(ip: String): String = {
    IpHelper.findRegionByIp(ip)
  }

  def main(args: Array[String]): Unit = {
    getCity("")
  }
}
