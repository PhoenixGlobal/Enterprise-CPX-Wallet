package com.chinapex.utils

import java.io.InputStream
import sun.misc.IOUtils

/**
  *
  */
object FileHelper {

  /**
    *
    * @param path eg. /readme.txt
    */
  def readBytesFromResource(path: String) = {
    val stream : InputStream = getClass.getResourceAsStream(path)
    IOUtils.readFully(stream, -1, false)
  }
}
