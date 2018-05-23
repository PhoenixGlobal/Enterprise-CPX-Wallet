package com.chinapex.helpers

/**
  * Created by pengmingguo on 7/18/17.
  */
class MessageClassLoader extends ClassLoader(classOf[MessageClassLoader].getClassLoader) {

  def defineClass(clazz: Array[Byte]): Class[_] = {
    defineClass(null, clazz, 0, clazz.length)
  }
}
