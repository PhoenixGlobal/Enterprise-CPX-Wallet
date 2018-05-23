package com.chinapex.legacy.services

/**
  * Created by sli on 17-7-4.
  */
object UAAResolutionService {

  def resolve(ua: String): UaResolutionResult = {
    val browser = getBrowser(ua)
    val browserVersion = getBrowserVersion(browser, ua)
    val os = getOS(ua)
//    val osVersion = getOSVersion(os, ua)
    val device = getDevice(ua)

    val browserFix = {
      if (browser.equals("Safari")){
        if (ua.indexOf("Chrome") > -1){
          "Chrome"
        }
        else{
          "Safari"
        }
      }
      else{
        browser
      }
    }

    if (device.equals("Unknown")){
      UaResolutionResult("PC", os, browserFix, browserVersion)
    }
    else{
      UaResolutionResult(device, os, browserFix, browserVersion)
    }
//    UaResolutionResult(device, os, browser, version)
  }

  def getBrowserVersion(b: String, ua: String): String = {
    try {


      b match {
        case "Safari" => {
          val regex = """.*Version/([\d.]+).*""".r
          val regex(version) = ua
          version
        }
        case "Chrome" => {
          try {
            val regex = """.*Chrome/([\d.]+).*""".r
            val regex(version) = ua
            version
          } catch {
            case _ => {
              val regex = """.*CriOS/([\d.]+).*""".r
              val regex(version) = ua
              version
            }
          }
        }
        case "IE" => {
          try {
            val regex = """.*MSIE ([\d.]+).*""".r
            val regex(version) = ua
            version
          } catch {
            case _ => {
              val regex = """.*rv:([\d.]+).*""".r
              val regex(version) = ua
              version
            }
          }
        }
        case "Edge" => {
          val regex = """.*Edge/([\d.]+).*""".r
          val regex(version) = ua
          version
        }
        case "Firefox" => {
          try {
            val regex = """.*Firefox/([\d.]+).*""".r
            val regex(version) = ua
            version
          } catch {
            case _ => {
              val regex = """.*FxiOS/([\d.]+).*""".r
              val regex(version) = ua
              version
            }
          }
        }
        case "Firefox Focus" => {
          val regex = """.*Focus/([\d.]+).*""".r
          val regex(version) = ua
          version
        }
        case "Chromium" => {
          val regex = """.*Chromium/([\d.]+).*""".r
          val regex(version) = ua
          version
        }
        case "Opera" => {
          try {
            val regex = """.*Opera/([\d.]+).*""".r
            val regex(version) = ua
            version
          } catch {
            case _ => {
              val regex = """.*OPR/([\d.]+).*""".r
              val regex(version) = ua
              version
            }
          }
        }
        case "Vivaldi" => {
          val regex = """.*Vivaldi/([\d.]+).*""".r
          val regex(version) = ua
          version
        }
        case "Yandex" => {
          val regex = """.*YaBrowser/([\d.]+).*""".r
          val regex(version) = ua
          version
        }
        case "Kindle" => {
          val regex = """.*Version/([\d.]+).*""".r
          val regex(version) = ua
          version
        }
        case "Maxthon" => {
          val regex = """.*Maxthon/([\d.]+).*""".r
          val regex(version) = ua
          version
        }
        case "QQBrowser" => {
          val regex = """.*QQBrowser/([\d.]+).*""".r
          val regex(version) = ua
          version
        }
        case "QQ" => {
          val regex = """.*QQ/([\d.]+).*""".r
          val regex(version) = ua
          version
        }
        case "Baidu" => {
          val regex = """.*BIDUBrowser[\s/]([\d.]+).*""".r
          val regex(version) = ua
          version
        }
        case "UC" => {
          val regex = """.*UC?Browser/([\d.]+).*""".r
          val regex(version) = ua
          version
        }
        case "Sogou" => {
          try {
            val regex = """.*SE ([\d.X]+).*""".r
            val regex(version) = ua
            version
          } catch {
            case _ => {
              val regex = """.*SogouMobileBrowser/([\d.]+).*""".r
              val regex(version) = ua
              version
            }
          }
        }
        case "2345Explorer" => {
          val regex = """.*2345Explorer/([\d.]+).*""".r
          val regex(version) = ua
          version
        }
        case "TheWorld" => {
          val regex = """.*TheWorld ([\d.]+).*""".r
          val regex(version) = ua
          version
        }
        case "XiaoMi" => {
          val regex = """.*MiuiBrowser/([\d.]+).*""".r
          val regex(version) = ua
          version
        }
        case "Quark" => {
          val regex = """.*Quark/([\d.]+).*""".r
          val regex(version) = ua
          version
        }
        case "Qiyu" => {
          val regex = """.*Qiyu/([\d.]+).*""".r
          val regex(version) = ua
          version
        }
        case "WeChat" => {
          val regex = """.*MicroMessenger/([\d.]+).*""".r
          val regex(version) = ua
          version
        }
        case "Taobao" => {
          val regex = """.*AliApp\(TB/([\d.]+).*""".r
          val regex(version) = ua
          version
        }
        case "Alipay" => {
          val regex = """.*AliApp\(AP/([\d.]+).*""".r
          val regex(version) = ua
          version
        }
        case "Weibo" => {
          val regex = """.*Weibo__([\d.]+).*""".r
          val regex(version) = ua
          version
        }
        case "Douban" => {
          val regex = """.*com.douban.frodo/([\d.]+).*""".r
          val regex(version) = ua
          version
        }
        case "Suning" => {
          val regex = """.*SNEBUY-APP([\d.]+).*""".r
          val regex(version) = ua
          version
        }
        case "iQiYi" => {
          val regex = """.*IqiyiVersion/([\d.]+).*""".r
          val regex(version) = ua
          version
        }
        case _ => {
          ""
        }
      }
    } catch {
      case _ => ""
    }
  }

  def getBrowser(ua: String): String = {
    val browserMap = Map(
      "Safari" -> "Safari",
      "Chrome" -> "Chrome",
      "CriOS" -> "Chrome",
      "MSIE" -> "IE",
      "Trident" -> "IE",
      "Edge" -> "Edge",
      "Firefox" -> "Firefox",
      "FxiOS" -> "Firefox",
      "Focus" -> "Firefox Focus",
      "Chromium" -> "Chromium",
      "Opera" -> "Opera",
      "OPR" -> "Opera",
      "Vivaldi" -> "Vivaldi",
      "YaBrowser" -> "Yandex",
      "Kindle" -> "Kindle",
      "Silk/" -> "Kindle",
      "360EE" -> "360",
      "360SE" -> "360",
      "UC" -> "UC",
      " UBrowser" -> "UC",
      "QQBrowser" -> "QQBrowser",
      "QQ/" -> "QQ",
      "Baidu" -> "Baidu",
      "BIBUBrowser" -> "Baidu",
      "Maxthon" -> "Maxthon",
      "MetaSr" -> "Sogou",
      "Sogou" -> "Sogou",
      "LBBROWSER" -> "LBBROWSER",
      "2345Explorer" -> "2345Explorer",
      "TheWorld" -> "TheWorld",
      "MiuiBrowser" -> "XiaoMi",
      "Quark" -> "Quark",
      "Qiyu" -> "Qiyu",
      "MicroMessenger" -> "WeChat",
      "AliApp(TB" -> "Taobao",
      "AliApp(AP" -> "Alipay",
      "Weibo" -> "Weibo",
      "com.douban.frodo" -> "Douban",
      "SNEBUY-APP" -> "Suning",
      "IqiyiApp" -> "iQiYi"
    )

    browserMap.foreach(bb => {
      val index = ua.indexOf(bb._1)
      if(index > -1){
        return bb._2
      }
    })
    "Unknown"
  }

  def getOSVersion(os: String, ua: String): String = {
    os match {
      case "Windows" => {
        val regex = """.*Windows NT ([\d.]+);.*""".r
        val regex(version) = ua
        version match {
          case "6.4" => "10"
          case "6.3" => "8.1"
          case "6.2" => "8"
          case "6.1" => "7"
          case "6.0" => "Vista"
          case "5.2" => "XP"
          case "5.1" => "XP"
          case "5.0" => "2000"
          case _ => ""
        }
      }
      case "Android" => {
        val regex = """.*Android ([\d.]+);.*""".r
        val regex(version) = ua
        version
      }
      case "iOS" => {
        val regex = """.*OS ([\d_]+) like.*""".r
        val regex(version) = ua
        version.replace("_", ".")
      }
      case "Debian" => {
        val regex = """.*Debian/([\d.]+).*""".r
        val regex(version) = ua
        version
      }
      case "Windows Phone" => {
        val regex = """.*Windows Phone( OS)? ([\d.]+);.*""".r
        val regex(version) = ua
        version
      }
      case "Mac OS X" => {
        val regex = """.*Mac OS X ([\d_]+).*""".r
        val regex(version) = ua
        version.replace("_", ".")
      }
      case "WebOS" => {
        val regex = """.*hpwOS/([\d.]+).*""".r
        val regex(version) = ua
        version
      }
      case _ => ""
    }
  }

  def getOS(ua: String): String = {
    val osMap = Map(
      "Windows" -> "Windows",
      "Linux" -> "Linux",
      "X11" -> "Linux",
      "Macintosh" -> "Mac OS X",
      "Android" -> "Android",
      "Adr" -> "Android",
      "Ubuntu" -> "Ubuntu",
      "FreeBSD" -> "FreeBSD",
      "Debian" -> "Debian",
      "IEMobile" -> "Windows Phone",
      "BlackBerry" -> "BlackBerry",
      "RIM" -> "BlackBerry",
      "MeeGo" -> "MeeGo",
      "Symbian" -> "Symbian",
      "like Mac OS X" -> "iOS",
      "CrOS" -> "Chrome OS",
      "hpwOS" -> "WebOS"
    )
    osMap.foreach(os => {
      val index = ua.indexOf(os._1)
      if(index > -1){
        return os._2
      }
    })
    "Unknown"
  }

  def getDevice(ua: String): String = {
    val deviceMap = Map(
      "Mobi" -> "Mobile",
      "iPh" -> "Mobile",
      "480" -> "Mobile",
      "Tablet" -> "Tablet",
      "Pad" -> "Tablet",
      "Nexus 7" -> "Tablet"
    )
    deviceMap.foreach(device => {
      val index = ua.indexOf(device._1)
      if(index > -1){
        return device._2
      }
    })
    "Unknown"
  }

  def getBrowserCore(ua: String): String = {
    val coreMap = Map(
      "Trident" -> "Trident",
      "NET CLR" -> "Trident",
      "Presto" -> "Presto",
      "AppleWebKit" -> "WebKit",
      "Gecko/" -> "Gecko"
    )
    coreMap.foreach(core => {
      val index = ua.indexOf(core._1)
      if(index > -1){
        return core._2
      }
    })
    "Unknown"
  }

}
